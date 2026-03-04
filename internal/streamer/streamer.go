package streamer

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"steamer/internal/metadata"
	"steamer/internal/queue"
	"steamer/internal/scanner"
	"steamer/internal/subscribers"

	"github.com/rs/zerolog"
)

//go:embed index.html
var indexHTML []byte

//go:embed sync.js
var syncJS []byte

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

func getIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *loggingResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.size += n
	return n, err
}

func (w *loggingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (s *Streamer) LoggerMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w}
		ipStr := getIP(r)

		defer func() {
			status := lw.status
			if status == 0 {
				status = http.StatusOK
			}
			s.logger.Trace().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("ip", ipStr).
				Int("status", status).
				Int("size", lw.size).
				Dur("duration", time.Since(start)).
				Msg("HTTP Request")
		}()

		next(lw, r)
	}
}

func (s *Streamer) RateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next(w, r)
			return
		}

		ipStr := getIP(r)
		ip := net.ParseIP(ipStr)
		if ip != nil && (ip.IsLoopback() || ip.IsPrivate()) {
			next(w, r)
			return
		}

		if val, ok := s.rateLimits.Load(ipStr); ok {
			lastSeen := val.(time.Time)
			if time.Since(lastSeen) < 5*time.Second {
				s.logger.Warn().Str("ip", ipStr).Msg("rate limit exceeded")
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
			s.rateLimits.Delete(ipStr)
		}

		s.rateLimits.Store(ipStr, time.Now())
		next(w, r)
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

func (s *Streamer) IndexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (s *Streamer) JSHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(syncJS)
}

func (s *Streamer) TimeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%d", time.Now().UnixMilli())
}

func (s *Streamer) EventsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ipStr := getIP(r)
	ch := make(chan string, 1)
	s.Subs.Add(ch, ipStr)
	s.BroadcastState()

	fmt.Fprintf(w, "data: %s\n\n", string(s.marshalState()))
	w.(http.Flusher).Flush()

	defer func() {
		s.Subs.Remove(ch, ipStr)
		s.BroadcastState()
	}()

	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Streamer) AudioHandler(w http.ResponseWriter, r *http.Request) {
	ipStr := getIP(r)

	if _, flagged := s.flaggedIPs.Load(ipStr); flagged {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		s.logger.Info().Str("ip", ipStr).Msg("serving screamer to flagged IP")
		http.ServeFile(w, r, "/tmp/scary.mp3")
		return
	}

	state := s.Queue.GetState()
	track := state.Name

	if track == "" {
		http.Error(w, "No track playing", http.StatusNotFound)
		return
	}

	local, ok := s.Scanner.(*scanner.LocalScanner)
	if !ok {
		http.Error(w, "Audio serving not supported for this scanner", http.StatusNotImplemented)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	s.logger.Debug().Str("track", track).Msg("serving audio")
	http.ServeFile(w, r, filepath.Join(local.MusicDir, track))
}

func (s *Streamer) ArtHandler(w http.ResponseWriter, r *http.Request) {
	state := s.Queue.GetState()
	track := state.Name
	if track == "" {
		http.NotFound(w, r)
		return
	}

	art, err := s.Metadata.GetArtwork(track)
	if err != nil || art == nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", art.MIMEType)
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Write(art.Data)
}

func (s *Streamer) SkipTrackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	s.logger.Info().Str("ip", getIP(r)).Msg("skip requested")
	s.Queue.Advance(s.BroadcastState)
}

func (s *Streamer) LibraryHandler(w http.ResponseWriter, r *http.Request) {
	files := s.Scanner.Scan()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

type queueCmd struct {
	Action string `json:"action"`
	Track  string `json:"track"`
	Index  int    `json:"index"`
}

func (s *Streamer) QueueHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		state := s.Queue.GetState()
		json.NewEncoder(w).Encode(state.Queue)
		return
	}

	if r.Method == http.MethodPost {
		var cmd queueCmd
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		switch cmd.Action {
		case "add":
			if !s.Scanner.Exists(cmd.Track) {
				ipStr := getIP(r)
				s.flaggedIPs.Store(ipStr, struct{}{})

				s.logger.Warn().
					Str("invalid_track", cmd.Track).
					Str("ip", ipStr).
					Msg("malicious input detected, IP flagged for screamer")

				w.WriteHeader(http.StatusTeapot)
				w.Write([]byte(`pidOr`))
				return
			}
			s.Queue.AppendItem(cmd.Track, s.BroadcastState)

		case "remove":
			s.Queue.RemoveItem(cmd.Index, s.BroadcastState)

		default:
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}
