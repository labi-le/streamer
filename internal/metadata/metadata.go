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
		baseDir: baseDir,
		logger:  logger.With().Str("component", "metadata").Logger(),
	}
}

func (p *Provider) GetArtwork(filename string) (*Artwork, error) {
	path := filepath.Join(p.baseDir, filepath.Clean(filename))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m, err := tag.ReadFrom(f)
	if err != nil {
		p.logger.Debug().Err(err).Str("file", filename).Msg("Failed to read tags")
		return nil, err
	}

	pic := m.Picture()
	if pic == nil {
		p.logger.Debug().Str("file", filename).Msg("No picture found in tags")
		return nil, nil
	}

	return &Artwork{
		Data:     pic.Data,
		MIMEType: pic.MIMEType,
	}, nil
}
