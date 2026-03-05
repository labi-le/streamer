package streamer

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"steamer/internal/metadata"
	"steamer/internal/queue"
	"steamer/internal/scanner"
	"steamer/internal/subscribers"
	"steamer/pkg/storage"

	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

//go:embed index.html
var indexHTML []byte

//go:embed sync.js
var syncJS []byte

//go:embed scary.mp3
var scaryMP3 []byte

const loggerKey = "req_logger"

func reqLogger(ctx *fasthttp.RequestCtx) *zerolog.Logger {
	if log, ok := ctx.UserValue(loggerKey).(*zerolog.Logger); ok {
		return log
	}
	nop := zerolog.Nop()
	return &nop
}

type Streamer struct {
	Queue      *queue.TrackQueue
	Subs       *subscribers.Manager
	Scanner    scanner.LibraryScanner
	Metadata   *metadata.Provider
	logger     zerolog.Logger
	rateLimits *storage.SyncMap[netip.Addr, time.Time]
	flaggedIPs *storage.SyncMap[netip.Addr, time.Time]
}

func New(scanner scanner.LibraryScanner, meta *metadata.Provider, logger zerolog.Logger) *Streamer {
	return &Streamer{
		Queue:      queue.NewTrackQueue(scanner, logger),
		Subs:       subscribers.NewManager(logger),
		Scanner:    scanner,
		Metadata:   meta,
		logger:     logger.With().Str("component", "streamer").Logger(),
		rateLimits: storage.NewSyncMapStorage[netip.Addr, time.Time](),
		flaggedIPs: storage.NewSyncMapStorage[netip.Addr, time.Time](),
	}
}

func (s *Streamer) cleanupRoutine() {
	ticker := time.NewTicker(2 * time.Minute)
	for range ticker.C {
		now := time.Now()
		s.rateLimits.Tap(func(ip netip.Addr, lastSeen time.Time) bool {
			if now.Sub(lastSeen) > 1*time.Minute {
				s.rateLimits.Delete(ip)
			}
			return true
		})
		s.flaggedIPs.Tap(func(ip netip.Addr, flaggedAt time.Time) bool {
			if now.Sub(flaggedAt) > 1*time.Hour {
				s.flaggedIPs.Delete(ip)
			}
			return true
		})
	}
}

func (s *Streamer) LoggerMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		start := time.Now()
		ip := getClientIP(ctx)

		reqLog := s.logger.With().Str("ip", ip.String()).Logger()
		ctx.SetUserValue(loggerKey, &reqLog)

		next(ctx)

		size := 0
		if !ctx.Response.IsBodyStream() {
			size = len(ctx.Response.Body())
		}

		reqLog.Trace().
			Str("method", string(ctx.Method())).
			Str("path", string(ctx.Path())).
			Int("status", ctx.Response.StatusCode()).
			Int("size", size).
			Dur("duration", time.Since(start)).
			Msg("request")
	}
}

func (s *Streamer) RateLimitMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		rawIP := getClientIP(ctx)

		if lastSeen, ok := s.rateLimits.Get(rawIP); ok {
			if time.Since(lastSeen) < 2*time.Second {
				reqLogger(ctx).Warn().Msg("rate limit exceeded")
				ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
				return
			}
		}

		s.rateLimits.Add(rawIP, time.Now())
		next(ctx)
	}
}

type broadcastPayload struct {
	queue.State
	Listeners int `json:"listeners"`
}

func (s *Streamer) marshalState() []byte {
	b, _ := json.Marshal(broadcastPayload{
		s.Queue.GetState(),
		s.Subs.Count(),
	})
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

	logger := reqLogger(ctx)

	ch := make(chan string, 1)
	s.Subs.Add(ch)
	logger.Trace().Msg("new subscriber connected")

	s.BroadcastState()

	initialState := s.marshalState()

	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		defer func() {
			s.Subs.Remove(ch)
			s.BroadcastState()
			logger.Trace().Msg("subscriber disconnected")
		}()

		_, _ = fmt.Fprintf(w, "data: %s\n\n", initialState)
		if err := w.Flush(); err != nil {
			return
		}

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case msg := <-ch:
				_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
				if err := w.Flush(); err != nil {
					return
				}
			case <-ticker.C:
				_, _ = fmt.Fprintf(w, ": ping\n\n")
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})
}

func (s *Streamer) AudioHandler(ctx *fasthttp.RequestCtx) {
	if s.flaggedIPs.Exist(getClientIP(ctx)) {
		ctx.Response.Header.Set("Cache-Control", "no-store, no-cache, must-revalidate")
		ctx.SetContentType("audio/mpeg")
		ctx.SetBody(scaryMP3)
		return
	}

	state := s.Queue.GetState()
	if state.Name == "" {
		ctx.Error("No track playing", fasthttp.StatusNotFound)
		return
	}

	ctx.Response.Header.Set("Cache-Control", "no-store, no-cache, must-revalidate")
	reqLogger(ctx).Trace().Str("track", state.Name).Msg("serving audio")
	fasthttp.ServeFile(ctx, s.Scanner.SelectFile(state.Name))
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

	reqLogger(ctx).Info().Msg("skip requested")
	if s.Queue.Advance() {
		s.BroadcastState()
	}
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
				ip := getClientIP(ctx)
				s.flaggedIPs.Add(ip, time.Now())

				reqLogger(ctx).Warn().
					Str("invalid_track", cmd.Track).
					Msg("malicious input detected, ip flagged")

				ctx.SetStatusCode(fasthttp.StatusTeapot)
				ctx.SetBodyString("pidOr")
				return
			}
			if s.Queue.AppendItem(cmd.Track) {
				s.BroadcastState()
			}

		case ActionRemove:
			if s.Queue.RemoveItem(cmd.Index) {
				s.BroadcastState()
			}

		default:
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusOK)

	default:
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
	}
}

func (s *Streamer) FaviconHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (s *Streamer) Serve(port uint16) error {
	go s.cleanupRoutine()

	s.logger.Info().Msgf("server running on http://localhost:%d", port)

	server := &fasthttp.Server{
		Handler:       s.BuildRoute(),
		MaxConnsPerIP: 100,
		ReadTimeout:   5 * time.Second,
		IdleTimeout:   30 * time.Second,
	}

	return server.ListenAndServe(fmt.Sprintf(":%d", port))
}

func (s *Streamer) BuildRoute() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/":
			s.IndexHandler(ctx)
		case "/favicon.ico":
			s.FaviconHandler(ctx)
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

func getClientIP(ctx *fasthttp.RequestCtx) netip.Addr {
	remoteAddr, _ := netip.AddrFromSlice(ctx.RemoteIP())
	remoteAddr = remoteAddr.Unmap()

	if !remoteAddr.IsPrivate() && !remoteAddr.IsLoopback() {
		return remoteAddr
	}

	if xRealIP := string(ctx.Request.Header.Peek("X-Real-IP")); xRealIP != "" {
		if addr, err := netip.ParseAddr(xRealIP); err == nil {
			return addr
		}
	}

	if xff := string(ctx.Request.Header.Peek("X-Forwarded-For")); xff != "" {
		if addr, err := netip.ParseAddr(strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])); err == nil {
			return addr
		}
	}

	return remoteAddr
}
