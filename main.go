package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML []byte

//go:embed sync.js
var syncJS []byte

var (
	musicDir     = "/home/drive/sync/music"
	queue        []string
	currentTrack string
	playAt       int64
	subscribers  = make(map[chan string]bool)
	mu           sync.Mutex
)

func main() {
	rand.Seed(time.Now().UnixNano())

	mu.Lock()
	initQueueInternal()
	advanceTrackInternal()
	mu.Unlock()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/sync.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Write(syncJS)
	})
	http.HandleFunc("/time", timeHandler)
	http.HandleFunc("/events", eventsHandler)
	http.HandleFunc("/audio", audioHandler)

	http.HandleFunc("/api/next", skipTrackHandler)
	http.HandleFunc("/api/library", libraryHandler)
	http.HandleFunc("/api/queue", queueHandler)

	fmt.Println("Server running on http://localhost:7777")
	log.Fatal(http.ListenAndServe(":7777", nil))
}

func getLibraryFiles() []string {
	entries, err := os.ReadDir(musicDir)
	if err != nil {
		return nil
	}
	exts := map[string]bool{".mp3": true, ".m4a": true, ".wav": true, ".flac": true, ".ogg": true, ".aac": true}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && exts[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, e.Name())
		}
	}
	return files
}

func initQueueInternal() {
	files := getLibraryFiles()
	if len(files) > 0 {
		for i := len(files) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			files[i], files[j] = files[j], files[i]
		}

		limit := 10
		if len(files) < limit {
			limit = len(files)
		}

		queue = make([]string, limit)
		copy(queue, files[:limit])
	}
}

func advanceTrackInternal() {
	if len(queue) == 0 {
		initQueueInternal()
	}
	if len(queue) > 0 {
		currentTrack = queue[0]
		queue = queue[1:]
	}
	playAt = time.Now().UnixMilli() + 5000
	broadcastStateInternal()
}

func getStateJSON() string {
	q := queue
	if q == nil {
		q = []string{}
	}
	payload := struct {
		Name   string   `json:"name"`
		PlayAt int64    `json:"play_at"`
		Queue  []string `json:"queue"`
	}{
		Name:   currentTrack,
		PlayAt: playAt,
		Queue:  q,
	}
	b, _ := json.Marshal(payload)
	return string(b)
}

func broadcastStateInternal() {
	msg := getStateJSON()
	for ch := range subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}

func timeHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%d", time.Now().UnixMilli())
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan string, 1)

	mu.Lock()
	subscribers[ch] = true
	initialMsg := getStateJSON()
	mu.Unlock()

	fmt.Fprintf(w, "data: %s\n\n", initialMsg)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	defer func() {
		mu.Lock()
		delete(subscribers, ch)
		close(ch)
		mu.Unlock()
	}()

	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

func audioHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	track := currentTrack
	mu.Unlock()

	safePath := filepath.Join(musicDir, filepath.Clean(track))
	if !strings.HasPrefix(safePath, filepath.Clean(musicDir)) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	http.ServeFile(w, r, safePath)
}

func skipTrackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	mu.Lock()
	if time.Now().UnixMilli()-playAt < 500 {
		mu.Unlock()
		return
	}
	advanceTrackInternal()
	mu.Unlock()
}

func libraryHandler(w http.ResponseWriter, r *http.Request) {
	files := getLibraryFiles()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func queueHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		mu.Lock()
		json.NewEncoder(w).Encode(queue)
		mu.Unlock()
		return
	}

	if r.Method == http.MethodPost {
		var newQueue []string
		if err := json.NewDecoder(r.Body).Decode(&newQueue); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		mu.Lock()
		queue = newQueue
		broadcastStateInternal()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}
