const loginPanel = document.getElementById('login-panel');
const dashboardPanel = document.getElementById('dashboard-panel');
const loginError = document.getElementById('login-error');
const syncStatus = document.getElementById('sync-status');
const syncProgress = document.getElementById('sync-progress');
const errorLog = document.getElementById('error-log');
const toast = document.getElementById('toast');
const bufferText = document.getElementById('buffer-text');
const bufferInfo = document.getElementById('buffer-info');
let eventSource = null;
let currentToken = localStorage.getItem('iotdb_access_token');
let lastJobId = null;
let refreshInterval = null;
const REFRESH_SECONDS = 5;
let secondsRemaining = REFRESH_SECONDS;

function triggerPulse(el) {
    if (!el) return;
    el.classList.remove('pulse-effect');
    void el.offsetWidth; // trigger reflow
    el.classList.add('pulse-effect');
}

function updateTimestamp(elId) {
    const el = document.getElementById(elId);
    if (el) {
        el.textContent = `(Last update: ${new Date().toLocaleTimeString()})`;
        triggerPulse(el);
    }
}

function updateCountdownUI() {
    const el = document.getElementById('refresh-countdown');
    if (el) {
        el.textContent = `Next refresh in: ${secondsRemaining}s`;
    }
}

function startAutoreload() {
    if (refreshInterval) clearInterval(refreshInterval);
    secondsRemaining = REFRESH_SECONDS;
    updateCountdownUI();
    refreshInterval = setInterval(() => {
        if (currentToken && !dashboardPanel.classList.contains('hidden')) {
            secondsRemaining--;
            if (secondsRemaining <= 0) {
                fetchAndPlotData();
                fetchAndPlotIoTDB();
                updateBufferStatus();
                secondsRemaining = REFRESH_SECONDS;
            }
            updateCountdownUI();
        }
    }, 1000);
}

function showToast(message, type = 'info') {
    toast.textContent = message;
    toast.className = `toast ${type}`;
    toast.classList.remove('hidden');
    setTimeout(() => toast.classList.add('hidden'), 5000);
}

function showDashboard() {
    loginPanel.classList.add('hidden');
    dashboardPanel.classList.remove('hidden');
    loginError.textContent = '';
    updateBufferStatus();
    fetchAndPlotData();
    fetchAndPlotIoTDB();
}

async function updateBufferStatus() {
    try {
        const response = await fetch('/buffer/status', {
            headers: { Authorization: `Bearer ${currentToken}` },
        });
        if (response.ok) {
            const data = await response.json();
            if (data.exists) {
                bufferText.textContent = `Local TSfile: ${data.filename} (${data.size_kb} KB)`;
                bufferInfo.classList.add('active');
                triggerPulse(bufferInfo.querySelector('.status-dot'));
                updateTimestamp('buffer-last-updated');
            } else {
                bufferText.textContent = 'No local TSfile found (Buffer is empty)';
                bufferInfo.classList.remove('active');
            }
        }
    } catch (err) {
        bufferText.textContent = 'Buffer status unavailable';
    }
}

function showLogin() {
    loginPanel.classList.remove('hidden');
    dashboardPanel.classList.add('hidden');
}

async function login() {
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    try {
        const response = await fetch('/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`,
        });
        if (!response.ok) {
            const error = await response.json();
            loginError.textContent = error.detail || 'Login failed';
            return;
        }
        const data = await response.json();
        currentToken = data.access_token;
        localStorage.setItem('iotdb_access_token', currentToken);
        showDashboard();
        showToast('Logged in successfully', 'success');
    } catch (err) {
        loginError.textContent = 'Unable to contact server';
    }
}

async function fetchAndPlotData() {
    const containerId = 'plot';
    const statusEl = document.getElementById('tsfile-status');
    const statusText = document.getElementById('tsfile-status-text');
    const layout = {
        title: 'Local IoT Sensor Data',
        xaxis: { title: 'Timestamp' },
        yaxis: { title: 'Value' },
        legend: { orientation: 'h' },
    };
    try {
        const response = await fetch('/data', {
            headers: { Authorization: `Bearer ${currentToken}` },
        });
        if (response.status === 401) {
            showLogin();
            return;
        }
        const result = await response.json();

        if (!result.exists) {
            statusText.textContent = "TSFile: Not found";
            Plotly.purge(containerId);
            return;
        }
        if (result.is_empty) {
            statusText.textContent = "TSFile: Empty";
            Plotly.purge(containerId);
            return;
        }

        statusText.textContent = `TSFile: Present (${result.data.length} records)`;
        updateTimestamp('tsfile-last-updated');

        const plotData = [
            {
                x: result.data.map(d => new Date(d.timestamp)),
                y: result.data.map(d => d.temperature),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Temperature (°C)',
            },
            {
                x: result.data.map(d => new Date(d.timestamp)),
                y: result.data.map(d => d.humidity),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Humidity (%)',
            },
            {
                x: result.data.map(d => new Date(d.timestamp)),
                y: result.data.map(d => d.pressure),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Pressure (hPa)',
            },
        ];

        Plotly.newPlot(containerId, plotData, layout, { responsive: true });
        triggerPulse(statusEl);
    } catch (err) {
        showToast('Unable to load local chart data', 'error');
        if (statusText) statusText.textContent = "Failed to load TSFile data";
        Plotly.newPlot(containerId, [], { ...layout, title: 'Local Data (Error Loading)' }, { responsive: true });
    }
}

async function fetchAndPlotIoTDB() {
    const containerId = 'plot-iotdb';
    const statusEl = document.getElementById('iotdb-status');
    const statusText = document.getElementById('iotdb-status-text');
    const layout = {
        title: 'IoTDB Timeseries Data',
        xaxis: { title: 'Timestamp' },
        yaxis: { title: 'Value' },
        legend: { orientation: 'h' },
    };
    try {
        const response = await fetch('/iotdb/data', {
            headers: { Authorization: `Bearer ${currentToken}` },
        });
        if (response.status === 401) {
            showLogin();
            return;
        }
        if (!response.ok) {
            const err = await response.json().catch(() => ({ detail: response.statusText }));
            showToast(`IoTDB: ${err.detail || response.statusText}`, 'error');
            Plotly.newPlot(containerId, [], { ...layout, title: `IoTDB Data (Unavailable: ${response.status})` }, { responsive: true });
            return;
        }
        const data = await response.json();
        const plotData = [
            {
                x: data.data.map(d => new Date(d.timestamp)),
                y: data.data.map(d => d.temperature),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Temperature (°C)',
            },
            {
                x: data.data.map(d => new Date(d.timestamp)),
                y: data.data.map(d => d.humidity),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Humidity (%)',
            },
            {
                x: data.data.map(d => new Date(d.timestamp)),
                y: data.data.map(d => d.pressure),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Pressure (hPa)',
            },
        ];

        Plotly.newPlot(containerId, plotData, layout, { responsive: true });
        if (statusText) statusText.textContent = "IoTDB Status: Connected";
        if (statusEl) triggerPulse(statusEl);
        triggerPulse(document.getElementById(containerId));
        updateTimestamp('iotdb-last-updated');
    } catch (err) {
        showToast('Unable to load IoTDB chart data', 'error');
        Plotly.newPlot(containerId, [], { ...layout, title: 'IoTDB Data (Connection Error)' }, { responsive: true });
    }
}

async function triggerSync() {
    if (!currentToken) {
        showLogin();
        return;
    }

    try {
        const response = await fetch('/sync', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${currentToken}`,
            },
        });
        if (!response.ok) {
            const result = await response.json();
            showToast(result.detail || 'Sync request failed', 'error');
            return;
        }
        const result = await response.json();
        lastJobId = result.job_id;
        syncStatus.textContent = `Sync requested: ${result.status}`;
        syncProgress.textContent = 'Waiting for updates...';
        errorLog.classList.add('hidden');
        subscribeToSyncStatus(lastJobId);
    } catch (err) {
        showToast('Unable to start sync', 'error');
    }
}

function subscribeToSyncStatus(jobId) {
    if (eventSource) {
        eventSource.close();
    }
    eventSource = new EventSource(`/sync/status/${jobId}?token=${encodeURIComponent(currentToken)}`);
    eventSource.onmessage = event => {
        const payload = JSON.parse(event.data || '{}');
        syncStatus.textContent = `Status: ${payload.status}`;
        syncProgress.textContent = payload.progress !== undefined ? `Progress: ${payload.progress}% (${payload.processed_records}/${payload.total_records})` : '';
        if (payload.status === 'completed') {
            showToast('Sync completed successfully', 'success');
            updateBufferStatus();
            fetchAndPlotData();
            fetchAndPlotIoTDB();
            secondsRemaining = REFRESH_SECONDS;
            updateCountdownUI();
            eventSource.close();
        }
        if (payload.status === 'failed') {
            showToast('Sync failed', 'error');
            errorLog.classList.remove('hidden');
            errorLog.textContent = payload.errors ? payload.errors.join('\n') : 'Unknown error';
            eventSource.close();
        }
    };
    eventSource.onerror = () => {
        syncProgress.textContent = 'Connection lost, retrying...';
    };
}

document.getElementById('login-button').addEventListener('click', login);
document.getElementById('sync-button').addEventListener('click', triggerSync);
document.getElementById('retry-button').addEventListener('click', triggerSync);

if (currentToken) {
    showDashboard();
} else {
    showLogin();
}