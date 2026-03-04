package main

import (
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

var (
	musicDir     = "/home/drive/sync/music"
	queue        []string
	currentTrack string
	playAt       int64
	subscribers  = make(map[chan string]bool)
	mu           sync.Mutex
)

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Perfect Sync</title>
    <style>
        body { font-family: sans-serif; padding: 20px; background: #111; color: #eee; text-align: center; }
        button { padding: 15px 30px; font-size: 18px; margin: 10px; cursor: pointer; background: #333; color: white; border: none; border-radius: 5px;}
        button:active { background: #555; }
        #start-btn { background: #b00; padding: 20px 40px; font-size: 24px; font-weight: bold; }
        #controls { display: none; }
    </style>
</head>
<body>
    <h2>Web Audio API Sync</h2>
    <h4 id="status">Waiting to join...</h4>
    
    <button id="start-btn" onclick="initSystem()">JOIN RADIO</button>
    
    <div id="controls">
        <h3 id="track-name">Loading track...</h3>
        <button onclick="skipTrack()">Next Track</button>
    </div>

    <script>
        const statusEl = document.getElementById('status');
        const trackNameEl = document.getElementById('track-name');
        const startBtn = document.getElementById('start-btn');
        const controls = document.getElementById('controls');

        let audioCtx;
        let currentSource;
        let serverOffset = 0;
        let currentTrackName = "";
        let abortController = null;

        async function initSystem() {
            startBtn.style.display = 'none';
            statusEl.innerText = "Syncing clocks...";
            
            audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            if (audioCtx.state === 'suspended') {
                await audioCtx.resume();
            }

            const offsets = [];
            for (let i = 0; i < 5; i++) {
                const t0 = Date.now();
                const res = await fetch('/time');
                const t1 = Date.now();
                const serverTime = parseInt(await res.text());
                const latency = (t1 - t0) / 2;
                offsets.push(serverTime - latency - t0);
            }
            serverOffset = offsets.reduce((a, b) => a + b) / offsets.length;

            controls.style.display = 'block';
            statusEl.innerText = "Synchronized";
            connectSSE();
        }

        function connectSSE() {
            const events = new EventSource('/events');
            events.onmessage = async (e) => {
                const data = JSON.parse(e.data);
                if (data.name === currentTrackName) return;

                if (currentSource) {
                    currentSource.stop();
                    currentSource.disconnect();
                    currentSource = null;
                }

                if (abortController) {
                    abortController.abort();
                }
                abortController = new AbortController();
                const signal = abortController.signal;
                
                currentTrackName = data.name;
                trackNameEl.innerText = "Downloading: " + data.name;

                try {
                    const response = await fetch('/audio', { signal });
                    const arrayBuffer = await response.arrayBuffer();
                    const audioBuffer = await audioCtx.decodeAudioData(arrayBuffer);

                    if (currentTrackName !== data.name) return;

                    schedulePlayback(audioBuffer, data.play_at, data.name);
                } catch (err) {
                    if (err.name !== 'AbortError') {
                        trackNameEl.innerText = "Error decoding audio";
                    }
                }
            };
        }

        function schedulePlayback(buffer, playAtMs, name) {
            if (currentSource) {
                currentSource.stop();
                currentSource.disconnect();
            }

            currentSource = audioCtx.createBufferSource();
            currentSource.buffer = buffer;
            currentSource.connect(audioCtx.destination);

            const nowMs = Date.now() + serverOffset;
            const diffSeconds = (playAtMs - nowMs) / 1000.0;

            trackNameEl.innerText = "Playing: " + name;

            if (diffSeconds > 0) {
                currentSource.start(audioCtx.currentTime + diffSeconds);
            } else {
                currentSource.start(0, Math.abs(diffSeconds));
            }

            currentSource.onended = () => {
                if (Date.now() + serverOffset >= playAtMs + (buffer.duration * 1000) - 500) {
                    skipTrack();
                }
            };
        }

        function skipTrack() {
            fetch('/api/next', { method: 'POST' });
        }
    </script>
</body>
</html>`

func main() {
	rand.Seed(time.Now().UnixNano())

	mu.Lock()
	loadMusicQueueInternal()
	advanceTrackInternal()
	mu.Unlock()

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/time", timeHandler)
	http.HandleFunc("/events", eventsHandler)
	http.HandleFunc("/audio", audioHandler)
	http.HandleFunc("/api/next", skipTrackHandler)

	fmt.Println("Server running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
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
	w.Write([]byte(indexHTML))
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
