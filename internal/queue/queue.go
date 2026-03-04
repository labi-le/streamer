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

	go q.watchQueue()

	return q
}

func (q *TrackQueue) watchQueue() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		q.fillQueue()
	}
}

func (q *TrackQueue) fillQueue() {
	q.mu.Lock()
	defer q.mu.Unlock()

	curr := q.state.Load()
	if len(curr.Queue) >= 10 {
		return
	}

	allFiles := q.scanner.Scan()
	if len(allFiles) == 0 {
		return
	}

	exists := make(map[string]bool)
	exists[curr.Name] = true
	for _, t := range curr.Queue {
		exists[t] = true
	}

	var available []string
	for _, f := range allFiles {
		if !exists[f] {
			available = append(available, f)
		}
	}

	if len(available) == 0 {
		available = make([]string, len(allFiles))
		copy(available, allFiles)
	}

	q.shuffle(available)

	needed := 10 - len(curr.Queue)
	if needed > len(available) {
		needed = len(available)
	}

	newQueue := make([]string, len(curr.Queue), len(curr.Queue)+needed)
	copy(newQueue, curr.Queue)
	newQueue = append(newQueue, available[:needed]...)

	q.state.Store(&State{
		Name:   curr.Name,
		PlayAt: curr.PlayAt,
		Queue:  newQueue,
	})

	q.logger.Debug().Int("added", needed).Int("total", len(newQueue)).Msg("Queue topped up")
}

func (q *TrackQueue) GetState() State {
	return *q.state.Load()
}

func (q *TrackQueue) Init() {
	q.fillQueue()
}

func (q *TrackQueue) Advance(onDone func()) {
	q.mu.Lock()

	curr := q.state.Load()
	items := curr.Queue

	newState := &State{
		PlayAt: time.Now().UnixMilli() + 5000,
		Queue:  []string{},
	}

	if len(items) > 0 {
		newState.Name = items[0]
		newState.Queue = items[1:]
		q.logger.Info().Str("track", newState.Name).Int("remaining", len(newState.Queue)).Msg("Advancing track")
	} else {
		q.logger.Warn().Msg("Advance called but queue is empty")
	}

	q.state.Store(newState)
	q.mu.Unlock()

	if onDone != nil {
		onDone()
	}
}

func (q *TrackQueue) SetItems(items []string, onDone func()) {
	q.mu.Lock()
	curr := q.state.Load()

	newQueue := make([]string, len(items))
	copy(newQueue, items)

	q.state.Store(&State{
		Name:   curr.Name,
		PlayAt: curr.PlayAt,
		Queue:  newQueue,
	})
	q.logger.Info().Int("size", len(items)).Msg("Queue updated")
	q.mu.Unlock()

	if onDone != nil {
		onDone()
	}
}

func (q *TrackQueue) shuffle(files []string) {
	for i := len(files) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		files[i], files[j] = files[j], files[i]
	}
}
