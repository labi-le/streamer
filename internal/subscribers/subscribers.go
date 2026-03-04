package subscribers

import (
	"sync"

	"github.com/rs/zerolog"
)

type Manager struct {
	subscribers sync.Map
	logger      zerolog.Logger
}

func NewManager(logger zerolog.Logger) *Manager {
	return &Manager{
		logger: logger.With().Str("component", "subscribers").Logger(),
	}
}

func (s *Manager) Add(ch chan string, remoteAddr string) {
	s.subscribers.Store(ch, struct{}{})
	s.logger.Info().Str("remote_addr", remoteAddr).Msg("new subscriber connected")
}

func (s *Manager) Remove(ch chan string, remoteAddr string) {
	s.subscribers.Delete(ch)
	s.logger.Info().Str("remote_addr", remoteAddr).Msg("subscriber disconnected")
}

func (s *Manager) Broadcast(msg string) {
	s.logger.Debug().Msg("broadcasting state to subscribers")
	s.subscribers.Range(func(key, value interface{}) bool {
		ch := key.(chan string)
		select {
		case ch <- msg:
		default:
			s.logger.Trace().Msg("subscriber channel full, skipping message")
		}
		return true
	})
}
