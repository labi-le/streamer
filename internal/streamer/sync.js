const statusEl = document.getElementById('status');
const trackNameEl = document.getElementById('track-name');
const startBtn = document.getElementById('start-btn');
const controls = document.getElementById('controls');
const libraryListEl = document.getElementById('library-list');
const queueListEl = document.getElementById('queue-list');
const searchInput = document.getElementById('search-input');

let audioCtx;
let currentSource;
let serverOffset = 0;
let currentTrackName = "";
let currentPlayAt = 0;
let abortController = null;
let currentQueue = [];
let allLibraryFiles = [];
let audioEnabled = false;

loadLibrary();
connectSSE();

async function initSystem() {
    startBtn.style.display = 'none';
    statusEl.innerText = "Syncing clocks...";

    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    if (audioCtx.state === 'suspended') {
        await audioCtx.resume();
    }

    const samples = [];
    for (let i = 0; i < 8; i++) {
        const t0 = Date.now();
        const res = await fetch('/time');
        const t1 = Date.now();
        const serverTime = parseInt(await res.text());
        const rtt = t1 - t0;
        const offset = serverTime - (rtt / 2) - t0;
        samples.push({ rtt, offset });
    }

    samples.sort((a, b) => a.rtt - b.rtt);
    serverOffset = samples[0].offset;

    controls.style.display = 'block';
    statusEl.innerText = "Synchronized";
    audioEnabled = true;

    if (currentTrackName) {
        fetchAndPlayAudio(currentPlayAt, currentTrackName);
    }
}

function connectSSE() {
    const events = new EventSource('/events');
    events.onmessage = async (e) => {
        const data = JSON.parse(e.data);

        currentQueue = data.queue || [];
        renderQueue();

        if (data.play_at === currentPlayAt) return;

        currentPlayAt = data.play_at;
        currentTrackName = data.name;

        trackNameEl.innerText = audioEnabled ? "Downloading: " + data.name : "Currently playing: " + data.name;

        const artUrl = `/art?track=${encodeURIComponent(data.name)}`;
        document.getElementById('bg-art').style.backgroundImage = `url('${artUrl}')`;
        document.getElementById('main-art').style.backgroundImage = `url('${artUrl}')`;

        if (audioEnabled) {
            fetchAndPlayAudio(data.play_at, data.name);
        }
    };
}

async function fetchAndPlayAudio(playAtMs, name) {
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

    trackNameEl.innerText = "Downloading: " + name;

    try {
        const response = await fetch('/audio', { signal });
        const arrayBuffer = await response.arrayBuffer();
        const audioBuffer = await audioCtx.decodeAudioData(arrayBuffer);

        if (currentPlayAt !== playAtMs) return;

        schedulePlayback(audioBuffer, playAtMs, name);
    } catch (err) {
        if (err.name !== 'AbortError') {
            trackNameEl.innerText = "Error decoding audio";
        }
    }
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

async function loadLibrary() {
    const res = await fetch('/api/library');
    allLibraryFiles = await res.json() || [];
    renderLibrary(allLibraryFiles);
}

searchInput.addEventListener('input', (e) => {
    const term = e.target.value.toLowerCase();
    if (!term) {
        renderLibrary(allLibraryFiles);
        return;
    }

    const filtered = allLibraryFiles.filter(f => {
        const str = f.toLowerCase();
        let i = 0, j = 0;
        while (i < str.length && j < term.length) {
            if (str[i] === term[j]) j++;
            i++;
        }
        return j === term.length;
    });

    renderLibrary(filtered);
});

function renderLibrary(files) {
    libraryListEl.innerHTML = '';
    files.forEach(f => {
        const div = document.createElement('div');
        div.className = 'track-item';
        div.innerText = f;
        div.onclick = () => addToQueue(f);
        libraryListEl.appendChild(div);
    });
}

function renderQueue() {
    queueListEl.innerHTML = '';
    currentQueue.forEach((f, idx) => {
        const div = document.createElement('div');
        div.className = 'track-item';
        div.innerText = `${idx + 1}. ${f}`;
        div.onclick = () => removeFromQueue(idx);
        queueListEl.appendChild(div);
    });
}

async function addToQueue(filename) {
    const newQueue = [...currentQueue, filename];
    await fetch('/api/queue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newQueue)
    });
}

async function removeFromQueue(index) {
    const newQueue = [...currentQueue];
    newQueue.splice(index, 1);
    await fetch('/api/queue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newQueue)
    });
}

function toggleUI() {
    const content = document.getElementById('main-content');
    content.classList.toggle('hidden');
}