package main

import (
	"flag"
	"math/rand"
	"net/http"
	"time"

	"steamer/internal/logger"
	"steamer/internal/metadata"
	"steamer/internal/scanner"
	"steamer/internal/streamer"

	"github.com/rs/zerolog/log"
)

func main() {
	verbose := flag.Bool("v", true, "verbose output")
	flag.Parse()

	l := logger.Init(*verbose)
	log.Logger = l

	musicDir := "/home/drive/sync/music"

	localScanner := scanner.NewLocalScanner(musicDir, l)
	metaProvider := metadata.NewProvider(musicDir, l)
	s := streamer.New(localScanner, metaProvider, l)

	rand.Seed(time.Now().UnixNano())

	s.Queue.Init()
	s.Queue.Advance(s.BroadcastState)

	http.HandleFunc("/", s.IndexHandler)
	http.HandleFunc("/sync.js", s.JSHandler)
	http.HandleFunc("/art", s.ArtHandler)

	http.HandleFunc("/time", s.LoggerMiddleware(s.TimeHandler))
	http.HandleFunc("/events", s.LoggerMiddleware(s.EventsHandler))
	http.HandleFunc("/audio", s.LoggerMiddleware(s.AudioHandler))

	http.HandleFunc("/api/next", s.LoggerMiddleware(s.RateLimitMiddleware(s.SkipTrackHandler)))
	http.HandleFunc("/api/queue", s.LoggerMiddleware(s.RateLimitMiddleware(s.QueueHandler)))

	http.HandleFunc("/api/library", s.LoggerMiddleware(s.LibraryHandler))

	l.Info().Msg("server running on http://localhost:7777")
	if err := http.ListenAndServe(":7777", nil); err != nil {
		l.Fatal().Err(err).Msg("server failed")
	}
}
