package state

import (
	"path/filepath"
	"reflect"
	"streamer/internal/queue"
	"testing"
)

func TestSQLiteStateStore(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Test 1: Load empty state
	got, err := store.Load()
	if err != nil {
		t.Errorf("Load failed: %v", err)
	}
	if got.Name != "" || got.PlayAt != 0 || len(got.Queue) != 0 {
		t.Errorf("expected empty state, got %+v", got)
	}

	// Test 2: Save and load state
	testState := queue.State{
		Name:   "track1.mp3",
		PlayAt: 123456789,
		Queue:  []string{"track2.mp3", "track3.mp3"},
	}

	err = store.Save(testState)
	if err != nil {
		t.Errorf("Save failed: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Errorf("Load failed after save: %v", err)
	}
	if !reflect.DeepEqual(loaded, testState) {
		t.Errorf("loaded state mismatch:\n got: %+v\nwant: %+v", loaded, testState)
	}

	// Test 3: Update state
	testState.Name = "updated.mp3"
	err = store.Save(testState)
	if err != nil {
		t.Errorf("Update Save failed: %v", err)
	}

	loaded, _ = store.Load()
	if loaded.Name != "updated.mp3" {
		t.Errorf("expected updated name %q, got %q", testState.Name, loaded.Name)
	}
}

func TestSQLiteStateStore_Persistence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "persist.db")

	// Open, save, and close
	store1, _ := NewSQLiteStore(dbPath)
	store1.Save(queue.State{Name: "persistent.mp3", PlayAt: 999, Queue: []string{"q1"}})
	store1.Close()

	// Re-open and check
	store2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to re-open store: %v", err)
	}
	defer store2.Close()

	got, _ := store2.Load()
	if got.Name != "persistent.mp3" {
		t.Errorf("persistence failed, got %q", got.Name)
	}
}

func TestSQLiteStateStore_InvalidPath(t *testing.T) {
	// Trying to open a DB in a non-existent directory without creation rights
	_, err := NewSQLiteStore("/nonexistent/path/to/db.db")
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}
