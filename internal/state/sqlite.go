package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"steamer/internal/queue"

	_ "modernc.org/sqlite"
)

type SQLiteStateStore struct {
	db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStateStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS steamer_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			name TEXT,
			play_at INTEGER,
			queue_json TEXT
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &SQLiteStateStore{db: db}, nil
}

func (s *SQLiteStateStore) Save(state queue.State) error {
	queueJSON, err := json.Marshal(state.Queue)
	if err != nil {
		return fmt.Errorf("marshal queue: %w", err)
	}

	_, err = s.db.Exec(`
		INSERT INTO steamer_state (id, name, play_at, queue_json)
		VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			play_at = excluded.play_at,
			queue_json = excluded.queue_json
	`, state.Name, state.PlayAt, string(queueJSON))

	return err
}

func (s *SQLiteStateStore) Load() (queue.State, error) {
	var (
		st        queue.State
		queueJSON string
	)

	err := s.db.QueryRow(
		`SELECT name, play_at, queue_json FROM steamer_state WHERE id = 1`).
		Scan(
			&st.Name,
			&st.PlayAt,
			&queueJSON,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return st, nil
	}
	if err != nil {
		return st, err
	}

	if err := json.Unmarshal([]byte(queueJSON), &st.Queue); err != nil {
		return st, fmt.Errorf("unmarshal queue: %w", err)
	}

	return st, nil
}

func (s *SQLiteStateStore) Close() error {
	return s.db.Close()
}
