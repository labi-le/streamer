package main

import (
	"flag"
	"steamer/internal/logger"
	"steamer/internal/metadata"
	"steamer/internal/scanner"
	"steamer/internal/state"
	"steamer/internal/streamer"
)

func main() {
	verbose := flag.Bool("v", true, "verbose output")
	musicDir := flag.String("path", "/home/drive/sync/music", "streaming path")
	dbPath := flag.String("db", "steamer.db", "path to state database")
	port := flag.Uint("port", 7777, "listen port")

	flag.Parse()

	lg := logger.Init(*verbose)

	store, err := state.NewSQLiteStore(*dbPath)
	if err != nil {
		lg.Fatal().Err(err).Msg("failed to open database")
	}
	defer store.Close()

	localScanner := scanner.NewLocalScanner(*musicDir, lg)
	metaProvider := metadata.NewProvider(*musicDir, lg)
	s := streamer.New(localScanner, metaProvider, lg, store)

	if s.Queue.GetState().Name == "" {
		if s.Queue.Advance() {
			s.BroadcastState()
		}
	}

	if err := s.Serve(uint16(*port)); err != nil {
		lg.Fatal().Err(err).Msg("server failed")
	}
}
