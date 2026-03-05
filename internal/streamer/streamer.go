package streamer

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"steamer/internal/metadata"
	"steamer/internal/queue"
	"steamer/internal/scanner"
	"steamer/internal/subscribers"

	"github.com/ferluci/fast-realip"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

//go:embed index.html
var indexHTML []byte

//go:embed sync.js
var syncJS []byte

//go:embed scary.mp3
var scaryMP3 []byte

type Streamer struct {
	Queue      *queue.TrackQueue
	Subs       *subscribers.Manager
	Scanner    scanner.LibraryScanner
	Metadata   *metadata.Provider
	logger     zerolog.Logger
	rateLimits sync.Map
	flaggedIPs sync.Map
}

func New(scanner scanner.LibraryScanner, meta *metadata.Provider, logger zerolog.Logger) *Streamer {
	return &Streamer{
		Queue:    queue.NewTrackQueue(scanner, logger),
		Subs:     subscribers.NewManager(logger),
		Scanner:  scanner,
		Metadata: meta,
		logger:   logger.With().Str("component", "streamer").Logger(),
	}
}

func (s *Streamer) LoggerMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		start := time.Now()

		next(ctx)

		size := 0
		if !ctx.Response.IsBodyStream() {
			size = len(ctx.Response.Body())
		}

		s.logger.Trace().
			Str("method", string(ctx.Method())).
			Str("path", string(ctx.Path())).
			Str("ip", realip.FromRequest(ctx)).
			Int("status", ctx.Response.StatusCode()).
			Int("size", size).
			Dur("duration", time.Since(start)).
			Msg("request")
	}
}

func (s *Streamer) RateLimitMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) != fasthttp.MethodPost {
			next(ctx)
			return
		}

		rawIP := realip.FromRequest(ctx)
		if rawIP == "" {
			return
		}

		ip := net.ParseIP(rawIP)
		if ip.IsLoopback() || ip.IsPrivate() {
			next(ctx)
			return
		}

		if val, ok := s.rateLimits.Load(rawIP); ok {
			lastSeen := val.(time.Time)
			if time.Since(lastSeen) < 5*time.Second {
				s.logger.Warn().Str("ip", rawIP).Msg("rate limit exceeded")
				ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
				return
			}
			s.rateLimits.Delete(rawIP)
		}

		s.rateLimits.Store(rawIP, time.Now())
		next(ctx)
	}
}

type broadcastPayload struct {
	queue.State
	Listeners int `json:"listeners"`
}

func (s *Streamer) marshalState() []byte {
	state := s.Queue.GetState()
	b, _ := json.Marshal(broadcastPayload{state, s.Subs.Count()})
	return b
}

func (s *Streamer) BroadcastState() {
	s.Subs.Broadcast(string(s.marshalState()))
}

func (s *Streamer) IndexHandler(ctx *fasthttp.RequestCtx) {
	if string(ctx.Path()) != "/" {
		ctx.NotFound()
		return
	}
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetBody(indexHTML)
}

func (s *Streamer) JSHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/javascript; charset=utf-8")
	ctx.SetBody(syncJS)
}

func (s *Streamer) TimeHandler(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(ctx, "%d", time.Now().UnixMilli())
}

func (s *Streamer) EventsHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/event-stream")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Connection", "keep-alive")

	ipStr := realip.FromRequest(ctx)
	ch := make(chan string, 1)
	s.Subs.Add(ch, ipStr)
	s.BroadcastState()

	initialState := s.marshalState()

	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		defer func() {
			s.Subs.Remove(ch, ipStr)
			s.BroadcastState()
		}()

		fmt.Fprintf(w, "data: %s\n\n", initialState)
		if err := w.Flush(); err != nil {
			return
		}

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case msg := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				if err := w.Flush(); err != nil {
					return
				}
			case <-ticker.C:
				fmt.Fprintf(w, ": ping\n\n")
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})
}

func (s *Streamer) AudioHandler(ctx *fasthttp.RequestCtx) {
	if _, flagged := s.flaggedIPs.Load(realip.FromRequest(ctx)); flagged {
		ctx.Response.Header.Set("Cache-Control", "no-store, no-cache, must-revalidate")
		ctx.SetContentType("audio/mpeg")
		ctx.SetBody(scaryMP3)
		return
	}

	state := s.Queue.GetState()
	track := state.Name

	if track == "" {
		ctx.Error("No track playing", fasthttp.StatusNotFound)
		return
	}

	local, ok := s.Scanner.(*scanner.LocalScanner)
	if !ok {
		ctx.Error("Audio serving not supported for this scanner", fasthttp.StatusNotImplemented)
		return
	}

	ctx.Response.Header.Set("Cache-Control", "no-store, no-cache, must-revalidate")
	s.logger.Debug().Str("track", track).Msg("serving audio")
	fasthttp.ServeFile(ctx, filepath.Join(local.MusicDir, track))
}

func (s *Streamer) ArtHandler(ctx *fasthttp.RequestCtx) {
	state := s.Queue.GetState()
	track := state.Name
	if track == "" {
		ctx.NotFound()
		return
	}

	art, err := s.Metadata.GetArtwork(track)
	if err != nil || art == nil {
		ctx.NotFound()
		return
	}

	ctx.SetContentType(art.MIMEType)
	ctx.Response.Header.Set("Cache-Control", "public, max-age=3600")
	ctx.SetBody(art.Data)
}

func (s *Streamer) SkipTrackHandler(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != fasthttp.MethodPost {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return
	}

	s.logger.Info().Str("ip", realip.FromRequest(ctx)).Msg("skip requested")
	s.Queue.Advance(s.BroadcastState)
}

func (s *Streamer) LibraryHandler(ctx *fasthttp.RequestCtx) {
	files := s.Scanner.Scan()
	ctx.SetContentType("application/json")
	json.NewEncoder(ctx).Encode(files)
}

type queueCmd struct {
	Action action `json:"action"`
	Track  string `json:"track"`
	Index  int    `json:"index"`
}

type action string

const (
	ActionAdd    action = "add"
	ActionRemove action = "remove"
)

func (s *Streamer) QueueHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/json")

	switch string(ctx.Method()) {
	case fasthttp.MethodGet:
		state := s.Queue.GetState()
		json.NewEncoder(ctx).Encode(state.Queue)

	case fasthttp.MethodPost:
		var cmd queueCmd
		if err := json.Unmarshal(ctx.PostBody(), &cmd); err != nil {
			ctx.Error("Bad Request", fasthttp.StatusBadRequest)
			return
		}

		switch cmd.Action {
		case ActionAdd:
			if !s.Scanner.Exists(cmd.Track) {
				ip := realip.FromRequest(ctx)
				s.flaggedIPs.Store(ip, struct{}{})

				s.logger.Warn().
					Str("invalid_track", cmd.Track).
					Str("ip", ip).
					Msg("malicious input detected, ip flagged")

				ctx.SetStatusCode(fasthttp.StatusTeapot)
				ctx.SetBodyString("pidOr")
				return
			}
			s.Queue.AppendItem(cmd.Track, s.BroadcastState)

		case ActionRemove:
			s.Queue.RemoveItem(cmd.Index, s.BroadcastState)

		default:
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusOK)

	default:
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
	}
}

func (s *Streamer) Serve(port uint16) error {
	s.logger.Info().Msg("server running on http://localhost:7777")
	return fasthttp.ListenAndServe(fmt.Sprintf(":%d", port), s.BuildRoute())
}

func (s *Streamer) BuildRoute() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/":
			s.IndexHandler(ctx)
		case "/sync.js":
			s.JSHandler(ctx)
		case "/art":
			s.ArtHandler(ctx)
		case "/time":
			s.LoggerMiddleware(s.TimeHandler)(ctx)
		case "/events":
			s.LoggerMiddleware(s.EventsHandler)(ctx)
		case "/audio":
			s.LoggerMiddleware(s.AudioHandler)(ctx)
		case "/api/next":
			s.LoggerMiddleware(s.RateLimitMiddleware(s.SkipTrackHandler))(ctx)
		case "/api/queue":
			s.LoggerMiddleware(s.RateLimitMiddleware(s.QueueHandler))(ctx)
		case "/api/library":
			s.LoggerMiddleware(s.LibraryHandler)(ctx)
		default:
			ctx.NotFound()
		}
	}
}
