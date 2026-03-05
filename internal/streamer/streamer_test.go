package streamer_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"steamer/internal/metadata"
	"steamer/internal/scanner"
	"steamer/internal/streamer"

	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

// --- Infrastructure ---

// mockScanner implements scanner.LibraryScanner backed by a static list.
type mockScanner struct {
	files []string
}

func (m *mockScanner) Scan() []string { return m.files }
func (m *mockScanner) Exists(name string) bool {
	for _, f := range m.files {
		if f == name {
			return true
		}
	}
	return false
}
func (m *mockScanner) SelectFile(name string) string { return "/nonexistent/" + name }

// newClient creates an HTTP client connected to an in-memory fasthttp server.
// The listener is closed via t.Cleanup when the test ends.
func newClient(t *testing.T, s *streamer.Streamer) *http.Client {
	t.Helper()
	ln := fasthttputil.NewInmemoryListener()
	srv := &fasthttp.Server{Handler: s.BuildRoute()}
	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() { ln.Close() })
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return ln.Dial()
			},
			// Ensure connection reuse so consecutive requests share the same RemoteAddr.
			MaxIdleConnsPerHost: 1,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// newTestEnv is the primary helper: creates a Streamer backed by sc plus a
// connected HTTP client. The metadata provider points at a temp dir.
func newTestEnv(t *testing.T, sc scanner.LibraryScanner) (*streamer.Streamer, *http.Client) {
	t.Helper()
	logger := zerolog.Nop()
	meta := metadata.NewProvider(t.TempDir(), logger)
	s := streamer.New(sc, meta, logger)
	return s, newClient(t, s)
}

// newAudioTestEnv creates a Streamer backed by a real LocalScanner pointing at
// a temp directory that contains a single small MP3 file, and advances the
// queue so that file is the current track.
func newAudioTestEnv(t *testing.T) (*streamer.Streamer, *http.Client, string) {
	t.Helper()
	dir := t.TempDir()
	filename := "test.mp3"
	// Minimal fake MP3 so the file exists and has measurable content.
	if err := os.WriteFile(filepath.Join(dir, filename), bytes.Repeat([]byte{0xFF, 0xFB}, 512), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	logger := zerolog.Nop()
	sc := scanner.NewLocalScanner(dir, logger)
	meta := metadata.NewProvider(dir, logger)
	s := streamer.New(sc, meta, logger)
	s.Queue.Advance(s.BroadcastState)
	return s, newClient(t, s), filename
}

// --- Request helpers ---

func doGet(t *testing.T, client *http.Client, path string, headers ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "http://test"+path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func doPost(t *testing.T, client *http.Client, path string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, "http://test"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func assertStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Errorf("status: got %d, want %d", resp.StatusCode, want)
	}
}

func assertContentType(t *testing.T, resp *http.Response, prefix string) {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, prefix) {
		t.Errorf("Content-Type: got %q, want prefix %q", ct, prefix)
	}
}

// --- Tests: static asset handlers ---

func TestIndexHandler_Root(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/")
	assertStatus(t, resp, 200)
	assertContentType(t, resp, "text/html")
	if body := readBody(t, resp); !strings.Contains(body, "<html") {
		t.Errorf("body does not contain <html>")
	}
}

func TestIndexHandler_NonRootPath_Returns404(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/not-the-root")
	assertStatus(t, resp, 404)
	readBody(t, resp)
}

func TestJSHandler(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/sync.js")
	assertStatus(t, resp, 200)
	assertContentType(t, resp, "application/javascript")
	if body := readBody(t, resp); len(body) == 0 {
		t.Error("empty JS body")
	}
}

func TestUnknownRoute_Returns404(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/api/does-not-exist")
	assertStatus(t, resp, 404)
	readBody(t, resp)
}

// --- Tests: time ---

func TestTimeHandler_ReturnsCurrentUnixMs(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	before := time.Now().UnixMilli()
	resp := doGet(t, client, "/time")
	after := time.Now().UnixMilli()

	assertStatus(t, resp, 200)
	ts, err := strconv.ParseInt(strings.TrimSpace(readBody(t, resp)), 10, 64)
	if err != nil {
		t.Fatalf("body is not a valid integer: %v", err)
	}
	if ts < before || ts > after {
		t.Errorf("timestamp %d out of expected range [%d, %d]", ts, before, after)
	}
}

// --- Tests: artwork ---

func TestArtHandler_NoCurrentTrack_Returns404(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/art")
	assertStatus(t, resp, 404)
	readBody(t, resp)
}

// --- Tests: library ---

func TestLibraryHandler_ReturnsJSONArray(t *testing.T) {
	files := []string{"alpha.mp3", "beta.flac"}
	_, client := newTestEnv(t, &mockScanner{files: files})

	resp := doGet(t, client, "/api/library")
	assertStatus(t, resp, 200)
	assertContentType(t, resp, "application/json")

	var result []string
	if err := json.Unmarshal([]byte(readBody(t, resp)), &result); err != nil {
		t.Fatalf("parse library response: %v", err)
	}
	if len(result) != len(files) {
		t.Errorf("library: got %d items, want %d", len(result), len(files))
	}
}

// --- Tests: queue handler ---

func TestQueueHandler_GetEmpty(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/api/queue")
	assertStatus(t, resp, 200)
	assertContentType(t, resp, "application/json")
	if body := strings.TrimSpace(readBody(t, resp)); body != "[]" {
		t.Errorf("empty queue: got %q, want []", body)
	}
}

func TestQueueHandler_AddValidTrack(t *testing.T) {
	track := "cool_song.mp3"
	s, client := newTestEnv(t, &mockScanner{files: []string{track}})

	resp := doPost(t, client, "/api/queue", map[string]any{"action": "add", "track": track})
	assertStatus(t, resp, 200)
	readBody(t, resp)

	// Verify via state directly: a second HTTP request to the same endpoint would
	// be rate-limited (same "pipe" remote addr within 5 s window).
	state := s.Queue.GetState()
	if len(state.Queue) != 1 || state.Queue[0] != track {
		t.Errorf("queue after add: got %v, want [%q]", state.Queue, track)
	}
}

func TestQueueHandler_AddInvalidTrack_Returns418(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{files: []string{"legit.mp3"}})

	resp := doPost(t, client, "/api/queue", map[string]any{"action": "add", "track": "../../etc/passwd"})
	assertStatus(t, resp, 418)
	if body := readBody(t, resp); body != "pidOr" {
		t.Errorf("body: got %q, want %q", body, "pidOr")
	}
}

func TestQueueHandler_RemoveTrack(t *testing.T) {
	track := "to_remove.mp3"
	s, client := newTestEnv(t, &mockScanner{files: []string{track}})

	// Add directly (bypasses rate limit so the HTTP remove POST isn't blocked).
	s.Queue.AppendItem(track, nil)

	// Remove via HTTP POST (the only POST in this test, so rate limit doesn't trigger).
	resp := doPost(t, client, "/api/queue", map[string]any{"action": "remove", "index": 0})
	assertStatus(t, resp, 200)
	readBody(t, resp)

	// Verify empty.
	resp = doGet(t, client, "/api/queue")
	var q []string
	json.Unmarshal([]byte(readBody(t, resp)), &q)
	if len(q) != 0 {
		t.Errorf("queue after remove: got %v, want []", q)
	}
}

func TestQueueHandler_UnknownAction_Returns400(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doPost(t, client, "/api/queue", map[string]any{"action": "explode"})
	assertStatus(t, resp, 400)
	readBody(t, resp)
}

func TestQueueHandler_InvalidJSON_Returns400(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	req, _ := http.NewRequest(http.MethodPost, "http://test/api/queue", strings.NewReader("not-json{{{"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST /api/queue: %v", err)
	}
	assertStatus(t, resp, 400)
	readBody(t, resp)
}

func TestQueueHandler_WrongMethod_Returns405(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	req, _ := http.NewRequest(http.MethodPut, "http://test/api/queue", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT /api/queue: %v", err)
	}
	assertStatus(t, resp, 405)
	readBody(t, resp)
}

// --- Tests: skip track ---

func TestSkipTrackHandler_PostAdvancesQueue(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{files: []string{"a.mp3", "b.mp3"}})
	resp := doPost(t, client, "/api/next", nil)
	assertStatus(t, resp, 200)
	readBody(t, resp)
}

func TestSkipTrackHandler_GetReturns405(t *testing.T) {
	_, client := newTestEnv(t, &mockScanner{})
	resp := doGet(t, client, "/api/next")
	assertStatus(t, resp, 405)
	readBody(t, resp)
}

// --- Tests: audio handler ---

func TestAudioHandler_NoCurrentTrack_Returns404(t *testing.T) {
	// mockScanner → scanner type assertion fails → 501
	// LocalScanner with empty queue → 404
	dir := t.TempDir()
	logger := zerolog.Nop()
	sc := scanner.NewLocalScanner(dir, logger)
	meta := metadata.NewProvider(dir, logger)
	s := streamer.New(sc, meta, logger)
	// Do NOT advance queue → track is ""
	client := newClient(t, s)

	resp := doGet(t, client, "/audio")
	assertStatus(t, resp, 404)
	readBody(t, resp)
}

func TestAudioHandler_ServesFileWithCorrectContentType(t *testing.T) {
	_, client, _ := newAudioTestEnv(t)
	resp := doGet(t, client, "/audio")
	assertStatus(t, resp, 200)
	assertContentType(t, resp, "audio/")
	body := readBody(t, resp)
	if len(body) == 0 {
		t.Error("audio body is empty")
	}
}

func TestAudioHandler_RangeRequest_Returns206(t *testing.T) {
	_, client, _ := newAudioTestEnv(t)
	resp := doGet(t, client, "/audio", "Range", "bytes=0-127")
	assertStatus(t, resp, 206)
	if cr := resp.Header.Get("Content-Range"); !strings.HasPrefix(cr, "bytes 0-127/") {
		t.Errorf("Content-Range: got %q, want prefix bytes 0-127/", cr)
	}
	body := readBody(t, resp)
	if len(body) != 128 {
		t.Errorf("Range body length: got %d, want 128", len(body))
	}
}

func TestAudioHandler_FlaggedIP_ReceivesScreamer(t *testing.T) {
	// Uses a shared connection (MaxIdleConnsPerHost=1) so both requests arrive
	// from the same pipe address. Invalid-track POST flags the IP; subsequent
	// /audio GET should receive scary.mp3 (audio/mpeg).
	_, client, _ := newAudioTestEnv(t)

	// Flag the connection's IP by sending an invalid track.
	resp := doPost(t, client, "/api/queue", map[string]any{"action": "add", "track": "../evil"})
	assertStatus(t, resp, 418)
	readBody(t, resp)

	// The same IP now receives the screamer instead of the real audio.
	resp = doGet(t, client, "/audio")
	assertStatus(t, resp, 200)
	if ct := resp.Header.Get("Content-Type"); ct != "audio/mpeg" {
		t.Errorf("Content-Type for flagged IP: got %q, want audio/mpeg", ct)
	}
	readBody(t, resp)
}

// --- Tests: SSE events handler ---

func TestEventsHandler_ReceivesInitialState(t *testing.T) {
	sc := &mockScanner{files: []string{"track.mp3"}}
	s, client := newTestEnv(t, sc)
	s.Queue.Advance(s.BroadcastState)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://test/events", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("connect to /events: %v", err)
	}
	defer resp.Body.Close()

	assertStatus(t, resp, 200)
	assertContentType(t, resp, "text/event-stream")

	// Read lines until we get the first data event.
	sc2 := bufio.NewScanner(resp.Body)
	for sc2.Scan() {
		line := sc2.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var payload struct {
			Name      string   `json:"name"`
			PlayAt    int64    `json:"play_at"`
			Queue     []string `json:"queue"`
			Listeners int      `json:"listeners"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload); err != nil {
			t.Fatalf("parse SSE data %q: %v", line, err)
		}
		if payload.Name != "track.mp3" {
			t.Errorf("SSE track: got %q, want track.mp3", payload.Name)
		}
		if payload.PlayAt == 0 {
			t.Error("SSE play_at is zero")
		}
		if payload.Listeners < 1 {
			t.Errorf("SSE listeners: got %d, want >= 1", payload.Listeners)
		}
		return // received valid initial state
	}
	t.Fatal("no SSE data event received before timeout")
}

func TestEventsHandler_SubscriberCountUpdates(t *testing.T) {
	s, client := newTestEnv(t, &mockScanner{})

	if n := s.Subs.Count(); n != 0 {
		t.Fatalf("initial subscriber count: got %d, want 0", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://test/events", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("connect to /events: %v", err)
	}
	defer resp.Body.Close()

	// Read first event to confirm the subscriber was registered before checking.
	sc2 := bufio.NewScanner(resp.Body)
	for sc2.Scan() {
		if strings.HasPrefix(sc2.Text(), "data: ") {
			break
		}
	}

	if n := s.Subs.Count(); n != 1 {
		t.Errorf("subscriber count while connected: got %d, want 1", n)
	}

	// Disconnect the client.
	cancel()
	resp.Body.Close()

	// Cleanup requires two broadcasts:
	//   1st: wakes the SSE goroutine → TCP write fails → internal PipeConn closes.
	//   2nd: wakes the goroutine again → bw.Flush() fails on closed PipeConn → defer removes subscriber.
	// Use a retry loop so the test isn't fragile about goroutine scheduling.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.BroadcastState()
		time.Sleep(50 * time.Millisecond)
		if s.Subs.Count() == 0 {
			return
		}
	}
	t.Errorf("subscriber count after disconnect: got %d, want 0", s.Subs.Count())
}

func TestEventsHandler_BroadcastsQueueState(t *testing.T) {
	track := "queued.mp3"
	sc := &mockScanner{files: []string{track}}
	_, client := newTestEnv(t, sc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://test/events", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("connect to /events: %v", err)
	}
	defer resp.Body.Close()

	lines := bufio.NewScanner(resp.Body)

	// Consume the initial event.
	for lines.Scan() {
		if strings.HasPrefix(lines.Text(), "data: ") {
			break
		}
	}

	// Add a track to the queue via POST; this triggers BroadcastState which
	// sends a new SSE event reflecting the updated queue.
	doPost(t, client, "/api/queue", map[string]any{"action": "add", "track": track})

	// The next data event should include the queued track.
	for lines.Scan() {
		line := lines.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var payload struct {
			Queue []string `json:"queue"`
		}
		json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload)
		if len(payload.Queue) == 1 && payload.Queue[0] == track {
			return // success
		}
	}
	t.Fatal("did not receive updated queue state over SSE")
}

// --- Tests: rate limit middleware ---

func TestRateLimitMiddleware_BlocksRapidPosts(t *testing.T) {
	// In-memory pipe connections have address "pipe" which is not a valid IP,
	// so net.ParseIP returns nil and the loopback/private bypass is skipped —
	// rate limiting is enforced.
	_, client := newTestEnv(t, &mockScanner{files: []string{"x.mp3"}})

	// First POST: accepted.
	resp := doPost(t, client, "/api/next", nil)
	assertStatus(t, resp, 200)
	readBody(t, resp)

	// Second POST within 5s: should be rate-limited.
	resp = doPost(t, client, "/api/next", nil)
	assertStatus(t, resp, 429)
	readBody(t, resp)
}

// --- Tests: queue state propagated through Advance ---

func TestSkipTrackHandler_ChangesCurrentTrack(t *testing.T) {
	tracks := []string{"first.mp3", "second.mp3"}
	s, client := newTestEnv(t, &mockScanner{files: tracks})
	s.Queue.Advance(s.BroadcastState) // pick initial track

	resp := doPost(t, client, "/api/next", nil)
	assertStatus(t, resp, 200)
	readBody(t, resp)

	// play_at is always set to now+5s; verify it is at least ~4s in the future.
	after := s.Queue.GetState()
	minExpected := time.Now().UnixMilli() + 4000
	if after.PlayAt < minExpected {
		t.Errorf("play_at %d is not far enough in the future (want >= %d)", after.PlayAt, minExpected)
	}
}

// --- Tests: queue state struct ---

func TestQueueHandler_StateReflectsAdvance(t *testing.T) {
	track := "the_track.mp3"
	s, client := newTestEnv(t, &mockScanner{files: []string{track}})
	s.Queue.Advance(s.BroadcastState)

	resp := doGet(t, client, "/api/queue")
	assertStatus(t, resp, 200)

	// Queue list should be empty (Advance consumed nothing from user queue).
	var q []string
	json.Unmarshal([]byte(readBody(t, resp)), &q)
	if len(q) != 0 {
		t.Errorf("user queue after advance: got %v, want []", q)
	}

	// The current track should be set.
	state := s.Queue.GetState()
	if state.Name != track {
		t.Errorf("current track: got %q, want %q", state.Name, track)
	}
}

// --- Tests: queue respects user-queued tracks before random picks ---

func TestQueueAdvance_UsesUserQueueFirst(t *testing.T) {
	userTrack := "user_pick.mp3"
	s, client := newTestEnv(t, &mockScanner{files: []string{userTrack, "random.mp3"}})
	s.Queue.Advance(s.BroadcastState) // initial random pick

	// Add directly so the HTTP POST for /api/next is not blocked by rate limit.
	s.Queue.AppendItem(userTrack, nil)

	// Advance via HTTP: must consume userTrack from the user queue.
	resp := doPost(t, client, "/api/next", nil)
	assertStatus(t, resp, 200)
	readBody(t, resp)

	state := s.Queue.GetState()
	if state.Name != userTrack {
		t.Errorf("after advance with user queue: got %q, want %q", state.Name, userTrack)
	}
	if len(state.Queue) != 0 {
		t.Errorf("user queue should be empty after advancing into it, got %v", state.Queue)
	}
}
