package streamer

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
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

		defer func() {
			status := lw.status
			if status == 0 {
				status = http.StatusOK
			}
			s.logger.Info().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("remote_addr", r.RemoteAddr).
				Int("status", status).
				Int("size", lw.size).
				Dur("duration", time.Since(start)).
				Msg("request")
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

		ipStr := r.Header.Get("X-Real-IP")
		if ipStr == "" {
			ipStr = r.Header.Get("X-Forwarded-For")
			if ipStr != "" {
				ipStr = strings.TrimSpace(strings.Split(ipStr, ",")[0])
			}
		}
		if ipStr == "" {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ipStr = r.RemoteAddr
			} else {
				ipStr = host
			}
		}

		ip := net.ParseIP(ipStr)
		if ip != nil && (ip.IsLoopback() || ip.IsPrivate()) {
			next(w, r)
			return
		}

		if val, ok := s.rateLimits.Load(ipStr); ok {
			lastSeen := val.(time.Time)
			if time.Since(lastSeen) < 5*time.Second {
				s.logger.Warn().Str("ip", ipStr).Msg("Rate limit exceeded")
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
		}

		s.rateLimits.Store(ipStr, time.Now())
		next(w, r)
	}
}

func (s *Streamer) BroadcastState() {
	state := s.Queue.GetState()
	b, _ := json.Marshal(state)
	s.Subs.Broadcast(string(b))
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

	ch := make(chan string, 1)
	s.Subs.Add(ch, r.RemoteAddr)

	state := s.Queue.GetState()
	b, _ := json.Marshal(state)

	fmt.Fprintf(w, "data: %s\n\n", string(b))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	defer s.Subs.Remove(ch, r.RemoteAddr)

	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Streamer) AudioHandler(w http.ResponseWriter, r *http.Request) {
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

	safePath := filepath.Join(local.MusicDir, filepath.Clean(track))
	if !strings.HasPrefix(safePath, filepath.Clean(local.MusicDir)) {
		s.logger.Warn().Str("track", track).Str("remote_addr", r.RemoteAddr).Msg("Forbidden access")
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	s.logger.Debug().Str("track", track).Msg("Serving audio")
	http.ServeFile(w, r, safePath)
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

	state := s.Queue.GetState()
	if time.Now().UnixMilli()-state.PlayAt < 500 {
		s.logger.Warn().Msg("Skip too frequent")
		return
	}

	s.logger.Info().Msg("Skip requested")
	s.Queue.Advance(s.BroadcastState)
}

func (s *Streamer) LibraryHandler(w http.ResponseWriter, r *http.Request) {
	files := s.Scanner.Scan()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func (s *Streamer) QueueHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		state := s.Queue.GetState()
		json.NewEncoder(w).Encode(state.Queue)
		return
	}

	if r.Method == http.MethodPost {
		var items []string
		if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
			s.logger.Error().Err(err).Msg("Decode failed")
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		var safeItems []string
		for _, item := range items {
			if s.Scanner.Exists(item) {
				safeItems = append(safeItems, item)
				continue
			}

			s.logger.Warn().
				Str("invalid_track", item).
				Str("remote_addr", r.RemoteAddr).
				Msg("Malicious input detected, dropping request")

			w.WriteHeader(http.StatusTeapot)
			w.Write([]byte(`pidOr`))
			return
		}

		s.Queue.SetItems(safeItems, s.BroadcastState)
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}
