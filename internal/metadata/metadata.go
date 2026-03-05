package metadata

import (
	"os"
	"path/filepath"

	"github.com/dhowden/tag"
	"github.com/rs/zerolog"

	"steamer/pkg/storage"
)

type filename string

type Artwork struct {
	Data     []byte
	MIMEType string
}

type Provider struct {
	baseDir string
	logger  zerolog.Logger
	cache   *storage.SyncMap[filename, *Artwork]
}

func NewProvider(baseDir string, logger zerolog.Logger) *Provider {
	return &Provider{
		baseDir: filepath.Clean(baseDir),
		logger:  logger.With().Str("component", "metadata").Logger(),
		cache:   storage.NewSyncMapStorage[filename, *Artwork](),
	}
}

func (p *Provider) GetArtwork(path string) (*Artwork, error) {
	key := filename(path)
	if art, ok := p.cache.Get(key); ok {
		return art, nil
	}

	f, err := os.Open(filepath.Join(p.baseDir, path))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m, err := tag.ReadFrom(f)
	if err != nil {
		p.logger.Debug().Err(err).Str("file", path).Msg("failed to read tags")
		p.cache.Add(key, nil)
		return nil, err
	}

	pic := m.Picture()
	if pic == nil {
		p.logger.Debug().Str("file", path).Msg("no picture found in tags")
		p.cache.Add(key, nil)
		return nil, nil
	}

	art := &Artwork{
		Data:     pic.Data,
		MIMEType: pic.MIMEType,
	}
	p.cache.Add(key, art)
	return art, nil
}
