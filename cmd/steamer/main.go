package main

import (
	"flag"
	"steamer/internal/logger"
	"steamer/internal/metadata"
	"steamer/internal/scanner"
	"steamer/internal/streamer"
)

func main() {
	verbose := *flag.Bool("v", true, "verbose output")
	musicDir := *flag.String("path", "/home/drive/sync/music", "streaming path")
	port := uint16(*flag.Uint64("port", 7777, "listen port"))

	flag.Parse()

	lg := logger.Init(verbose)

	localScanner := scanner.NewLocalScanner(musicDir, lg)
	metaProvider := metadata.NewProvider(musicDir, lg)
	s := streamer.New(localScanner, metaProvider, lg)

	if s.Queue.Advance() {
		s.BroadcastState()
	}

	if err := s.Serve(port); err != nil {
		lg.Fatal().Err(err).Msg("server failed")
	}
}
