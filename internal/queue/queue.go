package queue

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"steamer/internal/scanner"

	"github.com/rs/zerolog"
)

type State struct {
	Name   string   `json:"name"`
	PlayAt int64    `json:"play_at"`
	Queue  []string `json:"queue"`
}

type TrackQueue struct {
	state   atomic.Pointer[State]
	mu      sync.Mutex
	scanner scanner.LibraryScanner
	logger  zerolog.Logger
}

func NewTrackQueue(scanner scanner.LibraryScanner, logger zerolog.Logger) *TrackQueue {
	q := &TrackQueue{
		scanner: scanner,
		logger:  logger.With().Str("component", "queue").Logger(),
	}
	q.state.Store(&State{Queue: []string{}})
	return q
}

func (q *TrackQueue) GetState() State {
	return *q.state.Load()
}

func (q *TrackQueue) Advance() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	curr := q.state.Load()

	if curr.PlayAt > time.Now().UnixMilli() {
		q.logger.Debug().Msg("debounce: skip ignored")
		return false
	}

	newState := &State{
		PlayAt: time.Now().UnixMilli() + 3000,
		Queue:  []string{},
	}

	if len(curr.Queue) > 0 {
		newState.Name = curr.Queue[0]
		newState.Queue = curr.Queue[1:]
	} else {
		allFiles := q.scanner.Scan()
		if len(allFiles) > 0 {
			newState.Name = allFiles[rand.Intn(len(allFiles))]
		}
	}

	q.logger.Info().Str("track", newState.Name).Msg("advancing track")
	q.state.Store(newState)
	return true
}

func (q *TrackQueue) AppendItem(item string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	curr := q.state.Load()

	if len(curr.Queue) >= 200 {
		q.logger.Warn().Msg("queue limit reached, rejecting item")
		return false
	}

	newQueue := make([]string, len(curr.Queue), len(curr.Queue)+1)
	copy(newQueue, curr.Queue)
	newQueue = append(newQueue, item)

	q.state.Store(&State{
		Name:   curr.Name,
		PlayAt: curr.PlayAt,
		Queue:  newQueue,
	})
	q.logger.Info().Str("track", item).Msg("track appended to queue")
	return true
}

func (q *TrackQueue) RemoveItem(idx int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	curr := q.state.Load()

	if idx >= 0 && idx < len(curr.Queue) {
		newQueue := make([]string, 0, len(curr.Queue)-1)
		newQueue = append(newQueue, curr.Queue[:idx]...)
		newQueue = append(newQueue, curr.Queue[idx+1:]...)

		q.state.Store(&State{
			Name:   curr.Name,
			PlayAt: curr.PlayAt,
			Queue:  newQueue,
		})
		q.logger.Info().Int("index", idx).Msg("track removed from queue")
		return true
	}
	return false
}
