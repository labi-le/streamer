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