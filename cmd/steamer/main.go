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
	verbose := flag.Bool("v", false, "verbose output")
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
	http.HandleFunc("/time", s.TimeHandler)
	http.HandleFunc("/events", s.EventsHandler)
	http.HandleFunc("/audio", s.AudioHandler)
	http.HandleFunc("/art", s.ArtHandler)
	http.HandleFunc("/api/next", s.SkipTrackHandler)
	http.HandleFunc("/api/library", s.LibraryHandler)
	http.HandleFunc("/api/queue", s.QueueHandler)

	l.Info().Msg("Server running on http://localhost:7777")
	if err := http.ListenAndServe(":7777", nil); err != nil {
		l.Fatal().Err(err).Msg("Server failed")
	}
}
