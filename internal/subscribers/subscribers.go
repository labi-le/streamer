package subscribers

import (
	"streamer/pkg/storage"

	"github.com/rs/zerolog"
)

type Manager struct {
	subscribers *storage.SyncMap[chan string, struct{}]
	logger      zerolog.Logger
}

func NewManager(logger zerolog.Logger) *Manager {
	return &Manager{
		subscribers: storage.NewSyncMapStorage[chan string, struct{}](),
		logger:      logger.With().Str("component", "subscribers").Logger(),
	}
}

func (s *Manager) Add(ch chan string) {
	s.subscribers.Add(ch, struct{}{})
}

func (s *Manager) Remove(ch chan string) {
	s.subscribers.Delete(ch)
}

func (s *Manager) Count() int {
	return s.subscribers.Len()
}

func (s *Manager) Broadcast(msg string) {
	s.logger.Trace().Msg("broadcasting state to subscribers")
	s.subscribers.Tap(func(ch chan string, _ struct{}) bool {
		select {
		case ch <- msg:
		default:
			s.logger.Trace().Msg("subscriber channel full, skipping message")
		}
		return true
	})
}
