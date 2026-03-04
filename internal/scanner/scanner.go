package scanner

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type LibraryScanner interface {
	Scan() []string
}

type LocalScanner struct {
	MusicDir   string
	logger     zerolog.Logger
	mu         sync.RWMutex
	cache      []string
	lastUpdate time.Time
}

func NewLocalScanner(musicDir string, logger zerolog.Logger) *LocalScanner {
	return &LocalScanner{
		MusicDir: musicDir,
		logger:   logger.With().Str("component", "scanner").Logger(),
	}
}

func (s *LocalScanner) Scan() []string {
	s.mu.RLock()
	if time.Since(s.lastUpdate) < 1*time.Hour && len(s.cache) > 0 {
		cpy := make([]string, len(s.cache))
		copy(cpy, s.cache)
		s.mu.RUnlock()
		return cpy
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Since(s.lastUpdate) < 1*time.Hour && len(s.cache) > 0 {
		cpy := make([]string, len(s.cache))
		copy(cpy, s.cache)
		return cpy
	}

	entries, err := os.ReadDir(s.MusicDir)
	if err != nil {
		s.logger.Error().Err(err).Str("dir", s.MusicDir).Msg("Failed to read music directory")
		return nil
	}

	exts := map[string]bool{
		".mp3": true, ".m4a": true, ".wav": true,
		".flac": true, ".ogg": true, ".aac": true,
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && exts[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, e.Name())
		}
	}

	s.cache = files
	s.lastUpdate = time.Now()

	s.logger.Debug().Int("count", len(files)).Msg("Library scanned and cached")

	cpy := make([]string, len(s.cache))
	copy(cpy, s.cache)
	return cpy
}
