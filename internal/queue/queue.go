package queue

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"streamer/internal/scanner"

	"github.com/rs/zerolog"
)

type State struct {
	Name   string   `json:"name"`
	PlayAt int64    `json:"play_at"`
	Queue  []string `json:"queue"`
}

type StateStore interface {
	Save(state State) error
	Load() (State, error)
}

type TrackQueue struct {
	state   atomic.Pointer[State]
	mu      sync.Mutex
	scanner scanner.LibraryScanner
	logger  zerolog.Logger
	store   StateStore
}

func NewTrackQueue(scanner scanner.LibraryScanner, logger zerolog.Logger, store StateStore) *TrackQueue {
	q := &TrackQueue{
		scanner: scanner,
		logger:  logger.With().Str("component", "queue").Logger(),
		store:   store,
	}

	initialState := &State{Queue: []string{}}
	if store != nil {
		storedState, err := store.Load()
		if err == nil {
			// Check if we actually got something (Name is not empty)
			if storedState.Name != "" || len(storedState.Queue) > 0 {
				initialState = &storedState
				q.logger.Info().Msg("loaded queue state from storage")
			}
		} else {
			q.logger.Warn().Err(err).Msg("failed to load queue state from storage")
		}
	}

	q.state.Store(initialState)
	return q
}

func (q *TrackQueue) save() {
	if q.store == nil {
		return
	}
	s := q.state.Load()
	if err := q.store.Save(*s); err != nil {
		q.logger.Warn().Err(err).Msg("failed to save queue state")
	}
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
	q.save()
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
	q.save()
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
		q.save()
		return true
	}
	return false
}
