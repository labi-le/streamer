package main

import (
	_ "embed"
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
	loadMusicQueueInternal()
	advanceTrackInternal()
	mu.Unlock()

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/sync.js", jsHandler)
	http.HandleFunc("/time", timeHandler)
	http.HandleFunc("/events", eventsHandler)
	http.HandleFunc("/audio", audioHandler)
	http.HandleFunc("/api/next", skipTrackHandler)

	fmt.Println("Server running on http://localhost:7777")
	log.Fatal(http.ListenAndServe(":7777", nil))
}

func loadMusicQueueInternal() {
	entries, err := os.ReadDir(musicDir)
	if err != nil {
		return
	}

	exts := map[string]bool{".mp3": true, ".m4a": true, ".wav": true, ".flac": true, ".ogg": true, ".aac": true}
	var newQueue []string

	for _, e := range entries {
		if !e.IsDir() && exts[strings.ToLower(filepath.Ext(e.Name()))] {
			newQueue = append(newQueue, filepath.Join(musicDir, e.Name()))
		}
	}

	if len(newQueue) > 0 {
		queue = newQueue
		for i := len(queue) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			queue[i], queue[j] = queue[j], queue[i]
		}
	}
}

func advanceTrackInternal() {
	if len(queue) == 0 {
		loadMusicQueueInternal()
	}

	if len(queue) > 0 {
		currentTrack = queue[0]
		queue = queue[1:]
	}

	playAt = time.Now().UnixMilli() + 3000
	broadcastStateInternal()
}

func broadcastStateInternal() {
	msg := fmt.Sprintf(`{"name": "%s", "play_at": %d}`, filepath.Base(currentTrack), playAt)
	for ch := range subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func jsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(syncJS)
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
	initialMsg := fmt.Sprintf(`{"name": "%s", "play_at": %d}`, filepath.Base(currentTrack), playAt)
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

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	http.ServeFile(w, r, track)
}

func skipTrackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	if time.Now().UnixMilli()-playAt < 500 {
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}
	advanceTrackInternal()
	mu.Unlock()

	w.WriteHeader(http.StatusOK)
}
