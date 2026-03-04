package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

//go:embed index.html
var indexHTML []byte

//go:embed sync.js
var syncJS []byte

type Streamer struct {
	musicDir     string
	queue        []string
	currentTrack string
	playAt       int64
	subscribers  map[chan string]bool
	mu           sync.Mutex
	logger       zerolog.Logger
}

func NewStreamer(musicDir string, logger zerolog.Logger) *Streamer {
	return &Streamer{
		musicDir:    musicDir,
		subscribers: make(map[chan string]bool),
		logger:      logger.With().Str("component", "streamer").Logger(),
	}
}

func (s *Streamer) getLibraryFiles() []string {
	entries, err := os.ReadDir(s.musicDir)
	if err != nil {
		s.logger.Error().Err(err).Str("dir", s.musicDir).Msg("Failed to read music directory")
		return nil
	}
	exts := map[string]bool{".mp3": true, ".m4a": true, ".wav": true, ".flac": true, ".ogg": true, ".aac": true}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && exts[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, e.Name())
		}
	}
	return files
}

func (s *Streamer) initQueueInternal() {
	files := s.getLibraryFiles()
	if len(files) > 0 {
		for i := len(files) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			files[i], files[j] = files[j], files[i]
		}

		limit := 10
		if len(files) < limit {
			limit = len(files)
		}

		s.queue = make([]string, limit)
		copy(s.queue, files[:limit])
		s.logger.Info().Int("size", len(s.queue)).Msg("Queue initialized")
	} else {
		s.logger.Warn().Msg("No music files found to initialize queue")
	}
}

func (s *Streamer) advanceTrackInternal() {
	if len(s.queue) == 0 {
		s.initQueueInternal()
	}
	if len(s.queue) > 0 {
		s.currentTrack = s.queue[0]
		s.queue = s.queue[1:]
		s.logger.Info().Str("track", s.currentTrack).Int("remaining", len(s.queue)).Msg("Advancing to next track")
	}
	s.playAt = time.Now().UnixMilli() + 5000
	s.broadcastStateInternal()
}

func (s *Streamer) getStateJSON() string {
	q := s.queue
	if q == nil {
		q = []string{}
	}
	payload := struct {
		Name   string   `json:"name"`
		PlayAt int64    `json:"play_at"`
		Queue  []string `json:"queue"`
	}{
		Name:   s.currentTrack,
		PlayAt: s.playAt,
		Queue:  q,
	}
	b, _ := json.Marshal(payload)
	return string(b)
}

func (s *Streamer) broadcastStateInternal() {
	msg := s.getStateJSON()
	s.logger.Debug().Msg("Broadcasting state to subscribers")
	for ch := range s.subscribers {
		select {
		case ch <- msg:
		default:
			s.logger.Trace().Msg("Subscriber channel full, skipping message")
		}
	}
}

func (s *Streamer) timeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%d", time.Now().UnixMilli())
}

func (s *Streamer) eventsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan string, 1)

	s.mu.Lock()
	s.subscribers[ch] = true
	initialMsg := s.getStateJSON()
	s.mu.Unlock()

	s.logger.Info().Str("remote_addr", r.RemoteAddr).Msg("New subscriber connected")

	fmt.Fprintf(w, "data: %s\n\n", initialMsg)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	defer func() {
		s.mu.Lock()
		delete(s.subscribers, ch)
		close(ch)
		s.mu.Unlock()
		s.logger.Info().Str("remote_addr", r.RemoteAddr).Msg("Subscriber disconnected")
	}()

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

func (s *Streamer) audioHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	track := s.currentTrack
	s.mu.Unlock()

	safePath := filepath.Join(s.musicDir, filepath.Clean(track))
	if !strings.HasPrefix(safePath, filepath.Clean(s.musicDir)) {
		s.logger.Warn().Str("track", track).Str("remote_addr", r.RemoteAddr).Msg("Forbidden track access attempt")
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	s.logger.Debug().Str("track", track).Msg("Serving audio file")
	http.ServeFile(w, r, safePath)
}

func (s *Streamer) skipTrackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if time.Now().UnixMilli()-s.playAt < 500 {
		s.mu.Unlock()
		s.logger.Warn().Msg("Skip request too frequent")
		return
	}
	s.logger.Info().Msg("Skip requested")
	s.advanceTrackInternal()
	s.mu.Unlock()
}

func (s *Streamer) libraryHandler(w http.ResponseWriter, r *http.Request) {
	files := s.getLibraryFiles()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func (s *Streamer) queueHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		s.mu.Lock()
		json.NewEncoder(w).Encode(s.queue)
		s.mu.Unlock()
		return
	}

	if r.Method == http.MethodPost {
		var newQueue []string
		if err := json.NewDecoder(r.Body).Decode(&newQueue); err != nil {
			s.logger.Error().Err(err).Msg("Failed to decode new queue")
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.queue = newQueue
		s.logger.Info().Int("size", len(s.queue)).Msg("Queue updated via API")
		s.broadcastStateInternal()
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func initLogger(verbose bool) zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}

	if verbose {
		zerolog.CallerMarshalFunc = func(_ uintptr, file string, line int) string {
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			file = short
			return fmt.Sprintf("%s:%d", file, line)
		}
		return zerolog.New(output).
			Level(zerolog.TraceLevel).
			With().
			Timestamp().
			Caller().
			Logger()
	}

	return zerolog.New(output).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()
}

func main() {
	verbose := flag.Bool("v", false, "verbose output")
	flag.Parse()

	logger := initLogger(*verbose)
	log.Logger = logger

	s := NewStreamer("/home/drive/sync/music", logger)

	rand.Seed(time.Now().UnixNano())

	s.mu.Lock()
	s.initQueueInternal()
	s.advanceTrackInternal()
	s.mu.Unlock()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/sync.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Write(syncJS)
	})
	http.HandleFunc("/time", s.timeHandler)
	http.HandleFunc("/events", s.eventsHandler)
	http.HandleFunc("/audio", s.audioHandler)

	http.HandleFunc("/api/next", s.skipTrackHandler)
	http.HandleFunc("/api/library", s.libraryHandler)
	http.HandleFunc("/api/queue", s.queueHandler)

	logger.Info().Msg("Server running on http://localhost:7777")
	if err := http.ListenAndServe(":7777", nil); err != nil {
		logger.Fatal().Err(err).Msg("Server failed")
	}
}
