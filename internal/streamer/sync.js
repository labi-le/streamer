const trackNameEl = document.getElementById('track-name');
const startBtn = document.getElementById('start-btn');
const nextBtn = document.getElementById('next-btn');
const libraryListEl = document.getElementById('library-list');
const queueListEl = document.getElementById('queue-list');
const searchInput = document.getElementById('search-input');
const listenersEl = document.getElementById('listeners');

let audioCtx;
let currentSource;
let serverOffset = 0;
let currentTrackName = "";
let currentPlayAt = 0;
let abortController = null;
let currentQueue = [];
let allLibraryFiles = [];
let audioEnabled = false;
let progressRaf = null;
let currentArtUrl = null;

const progressFill = document.getElementById('progress-fill');
const timeElapsed = document.getElementById('time-elapsed');
const timeRemaining = document.getElementById('time-remaining');
const bgArt = document.getElementById('bg-art');
const mainArt = document.getElementById('main-art');

function fmtTime(sec) {
    const s = Math.max(0, Math.floor(sec));
    return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

loadLibrary();
connectSSE();

async function initSystem() {
    startBtn.classList.add('hidden');

    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    if (audioCtx.state === 'suspended') {
        await audioCtx.resume();
    }

    const samples = [];
    for (let i = 0; i < 8; i++) {
        const t0Perf = performance.now();
        const res = await fetch('/time');
        const text = await res.text();
        const rtt = performance.now() - t0Perf;
        const serverTime = parseInt(text, 10);

        const offset = serverTime - (rtt / 2) - t0Perf;
        samples.push({ rtt, offset });
    }

    samples.sort((a, b) => a.rtt - b.rtt);
    serverOffset = samples[0].offset;

    nextBtn.classList.remove('hidden');
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

        if (data.listeners !== undefined) {
            listenersEl.textContent = data.listeners;
        }

        if (data.play_at === currentPlayAt) return;

        currentPlayAt = data.play_at;
        currentTrackName = data.name;

        trackNameEl.innerText = data.name;

        bgArt.style.backgroundImage = '';
        mainArt.style.backgroundImage = '';
        if (currentArtUrl) { URL.revokeObjectURL(currentArtUrl); currentArtUrl = null; }

        fetch(`/art?track=${encodeURIComponent(data.name)}`)
            .then(res => res.ok ? res.blob() : null)
            .then(blob => {
                if (!blob) return;
                currentArtUrl = URL.createObjectURL(blob);
                const artUrl = `url('${currentArtUrl}')`;
                bgArt.style.backgroundImage = artUrl;
                mainArt.style.backgroundImage = artUrl;
            });

        if (audioEnabled) {
            fetchAndPlayAudio(data.play_at, data.name);
        }
    };
}

async function fetchAndPlayAudio(playAtMs, name) {
    if (currentSource) {
        currentSource.onended = null;
        currentSource.stop();
        currentSource.disconnect();
        currentSource = null;
    }

    if (abortController) {
        abortController.abort();
    }
    abortController = new AbortController();
    const signal = abortController.signal;

    if (progressRaf) {
        cancelAnimationFrame(progressRaf);
        progressRaf = null;
    }

    trackNameEl.innerText = name;
    progressFill.style.width = '0%';
    timeElapsed.textContent = 'Loading...';
    timeRemaining.textContent = '';

    try {
        const response = await fetch('/audio', { signal });
        if (!response.ok) throw new Error('Fetch failed');

        const contentLength = response.headers.get('content-length');
        const total = contentLength ? parseInt(contentLength, 10) : 0;
        let loaded = 0;

        const reader = response.body.getReader();
        const chunks = [];

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
            loaded += value.length;

            if (total > 0) {
                const pct = (loaded / total) * 100;
                progressFill.style.width = pct + '%';
                timeRemaining.textContent = Math.round(pct) + '%';
            } else {
                timeRemaining.textContent = (loaded / 1024 / 1024).toFixed(1) + ' MB';
            }
        }

        timeElapsed.textContent = 'Decoding...';
        timeRemaining.textContent = '';

        const arrayBuffer = new Uint8Array(loaded);
        let offset = 0;
        for (const chunk of chunks) {
            arrayBuffer.set(chunk, offset);
            offset += chunk.length;
        }

        const audioBuffer = await audioCtx.decodeAudioData(arrayBuffer.buffer);

        if (currentPlayAt !== playAtMs) return;

        schedulePlayback(audioBuffer, playAtMs, name);
    } catch (err) {
        if (err.name !== 'AbortError') {
            trackNameEl.innerText = "Error loading audio";
            timeElapsed.textContent = 'Error';
            timeRemaining.textContent = '';
        }
    }
}

function schedulePlayback(buffer, playAtMs, name) {
    if (currentSource) {
        currentSource.onended = null;
        currentSource.stop();
        currentSource.disconnect();
    }

    currentSource = audioCtx.createBufferSource();
    currentSource.buffer = buffer;
    currentSource.connect(audioCtx.destination);

    const nowMs = performance.now() + serverOffset;
    const diffSeconds = (playAtMs - nowMs) / 1000.0;

    trackNameEl.innerText = name;

    if (diffSeconds > 0) {
        currentSource.start(audioCtx.currentTime + diffSeconds);
    } else {
        const skipTime = Math.abs(diffSeconds);
        if (skipTime >= buffer.duration) {
            skipTrack();
            return;
        }
        currentSource.start(0, skipTime);
    }

    currentSource.onended = () => {
        const actualNowMs = performance.now() + serverOffset;
        if (actualNowMs >= playAtMs + (buffer.duration * 1000) - 500) {
            skipTrack();
        }
    };

    if (progressRaf) cancelAnimationFrame(progressRaf);
    const duration = buffer.duration;

    function updateProgress() {
        const elapsed = (performance.now() + serverOffset - playAtMs) / 1000;
        const pct = Math.max(0, Math.min(elapsed / duration * 100, 100));
        progressFill.style.width = pct + '%';
        timeElapsed.textContent = fmtTime(elapsed);
        timeRemaining.textContent = '-' + fmtTime(duration - elapsed);

        if (pct < 100) {
            progressRaf = requestAnimationFrame(updateProgress);
        }
    }

    progressFill.style.width = '0%';
    timeElapsed.textContent = '0:00';
    timeRemaining.textContent = '-' + fmtTime(duration);
    requestAnimationFrame(updateProgress);
}

function skipTrack() {
    fetch('/api/next', { method: 'POST' });
}

async function loadLibrary() {
    const res = await fetch('/api/library');
    allLibraryFiles = await res.json() || [];
    renderLibrary(allLibraryFiles);
}

let filteredFiles = [];
let physicalTerm = '';

const codeToChar = {};
'abcdefghijklmnopqrstuvwxyz'.split('').forEach(c => { codeToChar['Key' + c.toUpperCase()] = c; });
for (let i = 0; i <= 9; i++) { codeToChar['Digit' + i] = String(i); codeToChar['Numpad' + i] = String(i); }
Object.assign(codeToChar, {
    Space: ' ', Minus: '-', Equal: '=', BracketLeft: '[', BracketRight: ']',
    Backslash: '\\', Semicolon: ';', Quote: "'", Comma: ',', Period: '.', Slash: '/'
});

function matchScore(str, term) {
    if (str.includes(term)) return 0;
    let first = -1, last = -1, i = 0, j = 0;
    while (i < str.length && j < term.length) {
        if (str[i] === term[j]) {
            if (first === -1) first = i;
            last = i;
            j++;
        }
        i++;
    }
    return j === term.length ? last - first + 1 : Infinity;
}

searchInput.addEventListener('input', (e) => {
    const term = e.target.value.toLowerCase();
    if (!term) {
        filteredFiles = [];
        physicalTerm = '';
        renderLibrary(allLibraryFiles);
        return;
    }

    if (physicalTerm.length > term.length) physicalTerm = physicalTerm.slice(physicalTerm.length - term.length);

    const results = [];
    for (const f of allLibraryFiles) {
        const str = f.toLowerCase();
        const score = Math.min(matchScore(str, term), physicalTerm ? matchScore(str, physicalTerm) : Infinity);
        if (score < Infinity) results.push({ f, score });
    }
    results.sort((a, b) => a.score - b.score);
    filteredFiles = results.map(r => r.f);

    renderLibrary(filteredFiles);
});

searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.repeat && filteredFiles.length > 0) {
        addToQueue(filteredFiles[0]);
        return;
    }
    if (e.ctrlKey || e.altKey || e.metaKey) return;
    if (e.key === 'Backspace') {
        physicalTerm = physicalTerm.slice(0, -1);
    } else if (e.key.length === 1) {
        const c = codeToChar[e.code];
        if (c) physicalTerm += c;
    }
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
    await fetch('/api/queue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: "add", track: filename })
    });
}

async function removeFromQueue(index) {
    await fetch('/api/queue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: "remove", index: index })
    });
}

function toggleUI() {
    const content = document.getElementById('main-content');
    const isHidden = content.classList.contains('hidden');

    if (isHidden) {
        document.body.classList.remove('centered-player');
        content.classList.add('entering');
        content.classList.remove('hidden');
        requestAnimationFrame(() => requestAnimationFrame(() => content.classList.remove('entering')));
    } else {
        content.classList.add('hidden');
        document.body.classList.add('centered-player');
    }
}

document.addEventListener('keydown', (e) => {
    if (e.ctrlKey || e.altKey || e.metaKey) return;
    if (document.activeElement === searchInput) return;
    if (e.key.length !== 1 && e.key !== 'Backspace') return;

    if (e.key.length === 1) {
        const c = codeToChar[e.code];
        if (c) physicalTerm += c;
    }

    const content = document.getElementById('main-content');
    if (content.classList.contains('hidden')) toggleUI();

    const libBox = document.getElementById('library-box');
    if (libBox.classList.contains('mobile-hidden')) switchTab('library');

    searchInput.focus();
});

function switchTab(tab) {
    const libBox = document.getElementById('library-box');
    const queueBox = document.getElementById('queue-box');
    const libBtn = document.getElementById('tab-library');
    const queueBtn = document.getElementById('tab-queue');

    if (tab === 'library') {
        libBox.classList.remove('mobile-hidden');
        queueBox.classList.add('mobile-hidden');
        libBtn.classList.add('active');
        queueBtn.classList.remove('active');
    } else {
        libBox.classList.add('mobile-hidden');
        queueBox.classList.remove('mobile-hidden');
        libBtn.classList.remove('active');
        queueBtn.classList.add('active');
    }
}