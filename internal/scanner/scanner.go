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
	Exists(track string) bool
	SelectFile(track string) string
}

type LocalScanner struct {
	MusicDir   string
	logger     zerolog.Logger
	mu         sync.RWMutex
	cache      []string
	cacheMap   map[string]struct{}
	lastUpdate time.Time
}

func NewLocalScanner(musicDir string, logger zerolog.Logger) *LocalScanner {
	return &LocalScanner{
		MusicDir: filepath.Clean(musicDir),
		logger:   logger.With().Str("component", "scanner").Logger(),
	}
}

func (s *LocalScanner) updateCache() {
	entries, err := os.ReadDir(s.MusicDir)
	if err != nil {
		s.logger.Error().Err(err).Str("dir", s.MusicDir).Msg("failed to read music directory")
		return
	}

	exts := map[string]bool{
		".mp3": true, ".m4a": true, ".wav": true,
		".flac": true, ".ogg": true, ".aac": true,
	}

	var files []string
	fileMap := make(map[string]struct{})

	for _, e := range entries {
		if !e.IsDir() && exts[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, e.Name())
			fileMap[e.Name()] = struct{}{}
		}
	}

	s.cache = files
	s.cacheMap = fileMap
	s.lastUpdate = time.Now()

	s.logger.Debug().Int("count", len(files)).Msg("library scanned and cached")
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

	s.updateCache()

	cpy := make([]string, len(s.cache))
	copy(cpy, s.cache)
	return cpy
}

func (s *LocalScanner) SelectFile(track string) string {
	return filepath.Join(s.MusicDir, track)
}

func (s *LocalScanner) Exists(track string) bool {
	s.mu.RLock()
	if time.Since(s.lastUpdate) < 1*time.Hour && len(s.cache) > 0 {
		_, ok := s.cacheMap[track]
		s.mu.RUnlock()
		return ok
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Since(s.lastUpdate) >= 1*time.Hour || len(s.cache) == 0 {
		s.updateCache()
	}

	_, ok := s.cacheMap[track]
	return ok
}
