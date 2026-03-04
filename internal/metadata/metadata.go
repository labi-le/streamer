package metadata

import (
	"os"
	"path/filepath"

	"github.com/dhowden/tag"
	"github.com/rs/zerolog"
)

type Artwork struct {
	Data     []byte
	MIMEType string
}

type Provider struct {
	baseDir string
	logger  zerolog.Logger
}

func NewProvider(baseDir string, logger zerolog.Logger) *Provider {
	return &Provider{
		baseDir: filepath.Clean(baseDir),
		logger:  logger.With().Str("component", "metadata").Logger(),
	}
}

func (p *Provider) GetArtwork(filename string) (*Artwork, error) {
	f, err := os.Open(filepath.Join(p.baseDir, filename))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m, err := tag.ReadFrom(f)
	if err != nil {
		p.logger.Debug().Err(err).Str("file", filename).Msg("failed to read tags")
		return nil, err
	}

	pic := m.Picture()
	if pic == nil {
		p.logger.Debug().Str("file", filename).Msg("no picture found in tags")
		return nil, nil
	}

	return &Artwork{
		Data:     pic.Data,
		MIMEType: pic.MIMEType,
	}, nil
}
