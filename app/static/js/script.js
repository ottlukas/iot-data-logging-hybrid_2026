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
    startAutoreload();
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
                const dot = bufferInfo.querySelector('.status-dot');
                if (dot) {
                    dot.classList.remove('error-dot');
                    triggerPulse(dot);
                }
                updateTimestamp('buffer-last-updated');
                document.getElementById('metric-buffer-status').textContent = `Active (${data.size_kb} KB)`;
            } else {
                bufferText.textContent = 'No local TSfile found (Buffer is empty)';
                bufferInfo.classList.remove('active');
                const dot = bufferInfo.querySelector('.status-dot');
                if (dot) dot.classList.remove('error-dot');
                document.getElementById('metric-buffer-status').textContent = 'Empty';
            }
        }
    } catch (err) {
        bufferText.textContent = 'Buffer status unavailable';
        document.getElementById('metric-buffer-status').textContent = 'Offline';
        const dot = bufferInfo.querySelector('.status-dot');
        if (dot) dot.classList.add('error-dot');
    }
}

function showLogin() {
    loginPanel.classList.remove('hidden');
    dashboardPanel.classList.add('hidden');
    loginError.textContent = '';
}

function validateLoginInput(username, password) {
    if (!username || username.trim() === '') {
        loginError.textContent = 'Username is required';
        return false;
    }
    if (!password || password.trim() === '') {
        loginError.textContent = 'Password is required';
        return false;
    }
    return true;
}

async function login() {
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    
    // Validate input
    if (!validateLoginInput(username, password)) {
        return;
    }
    
    try {
        const response = await fetch('/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`,
        });
        if (!response.ok) {
            const error = await response.json();
            loginError.textContent = error.detail || 'Login failed - Invalid credentials';
            return;
        }
        const data = await response.json();
        currentToken = data.access_token;
        localStorage.setItem('iotdb_access_token', currentToken);
        showDashboard();
        showToast('Logged in successfully', 'success');
    } catch (err) {
        loginError.textContent = 'Unable to contact server - Please check your connection';
    }
}

function getDarkChartLayout(titleText) {
    return {
        title: {
            text: titleText,
            font: { color: '#f3f4f6', family: 'Inter, sans-serif', size: 15, weight: 'bold' }
        },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        font: { color: '#9ca3af', family: 'Inter, sans-serif' },
        xaxis: { 
            title: 'Timestamp',
            gridcolor: 'rgba(255, 255, 255, 0.05)',
            linecolor: 'rgba(255, 255, 255, 0.1)',
            tickcolor: 'rgba(255, 255, 255, 0.2)',
            zerolinecolor: 'rgba(255, 255, 255, 0.05)'
        },
        yaxis: { 
            title: 'Value',
            gridcolor: 'rgba(255, 255, 255, 0.05)',
            linecolor: 'rgba(255, 255, 255, 0.1)',
            tickcolor: 'rgba(255, 255, 255, 0.2)',
            zerolinecolor: 'rgba(255, 255, 255, 0.05)'
        },
        legend: { 
            orientation: 'h', 
            x: 0.5, 
            xanchor: 'center', 
            y: -0.2, 
            font: { color: '#9ca3af', size: 11 } 
        },
        margin: { t: 50, b: 60, l: 60, r: 20 },
        hovermode: 'closest'
    };
}

function getChartTraces(dataPoints) {
    return [
        {
            x: dataPoints.map(d => new Date(d.timestamp)),
            y: dataPoints.map(d => d.temperature),
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Temperature (\u00b0C)',
            line: { color: '#38bdf8', width: 2 },
            marker: { color: '#38bdf8', size: 4 }
        },
        {
            x: dataPoints.map(d => new Date(d.timestamp)),
            y: dataPoints.map(d => d.humidity),
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Humidity (%)',
            line: { color: '#34d399', width: 2 },
            marker: { color: '#34d399', size: 4 }
        },
        {
            x: dataPoints.map(d => new Date(d.timestamp)),
            y: dataPoints.map(d => d.pressure),
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Pressure (hPa)',
            line: { color: '#fbbf24', width: 2 },
            marker: { color: '#fbbf24', size: 4 }
        }
    ];
}

async function fetchAndPlotData() {
    const containerId = 'plot';
    const statusEl = document.getElementById('tsfile-status');
    const statusText = document.getElementById('tsfile-status-text');
    const layout = getDarkChartLayout('Local IoT Sensor Data');
    
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
            document.getElementById('metric-active-devices').textContent = 'None';
            return;
        }
        if (result.is_empty) {
            statusText.textContent = "TSFile: Empty";
            Plotly.purge(containerId);
            document.getElementById('metric-active-devices').textContent = 'None';
            return;
        }

        statusText.textContent = `TSFile: Present (${result.data.length} records)`;
        updateTimestamp('tsfile-last-updated');

        const devices = [...new Set(result.data.map(d => d.device_id))];
        document.getElementById('metric-active-devices').textContent = devices.join(', ') || 'None';

        const plotData = getChartTraces(result.data);
        Plotly.newPlot(containerId, plotData, layout, { responsive: true });
        triggerPulse(statusEl);
    } catch (err) {
        showToast('Unable to load local chart data', 'error');
        if (statusText) statusText.textContent = "Failed to load TSFile data";
        Plotly.newPlot(containerId, [], { ...layout, title: { text: 'Local Data (Error Loading)', font: { color: '#ef4444' } } }, { responsive: true });
    }
}

async function fetchAndPlotIoTDB() {
    const containerId = 'plot-iotdb';
    const statusEl = document.getElementById('iotdb-status');
    const statusText = document.getElementById('iotdb-status-text');
    const layout = getDarkChartLayout('IoTDB Timeseries Data');
    
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
            Plotly.newPlot(containerId, [], { ...layout, title: { text: `IoTDB Data (Unavailable: ${response.status})`, font: { color: '#ef4444' } } }, { responsive: true });
            document.getElementById('metric-iotdb-status').textContent = 'Disconnected';
            if (statusText) statusText.textContent = "IoTDB Status: Disconnected";
            return;
        }
        const data = await response.json();
        const plotData = getChartTraces(data.data);

        Plotly.newPlot(containerId, plotData, layout, { responsive: true });
        if (statusText) statusText.textContent = "IoTDB Status: Connected";
        document.getElementById('metric-iotdb-status').textContent = 'Connected';
        
        if (statusEl) triggerPulse(statusEl);
        triggerPulse(document.getElementById(containerId));
        updateTimestamp('iotdb-last-updated');
    } catch (err) {
        showToast('Unable to load IoTDB chart data', 'error');
        Plotly.newPlot(containerId, [], { ...layout, title: { text: 'IoTDB Data (Connection Error)', font: { color: '#ef4444' } } }, { responsive: true });
        document.getElementById('metric-iotdb-status').textContent = 'Disconnected';
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
            document.getElementById('metric-last-sync').textContent = new Date().toLocaleTimeString();
            updateBufferStatus();
            fetchAndPlotData();
            fetchAndPlotIoTDB();
            secondsRemaining = REFRESH_SECONDS;
            updateCountdownUI();
            eventSource.close();
        }
        if (payload.status === 'failed') {
            showToast('Sync failed', 'error');
            document.getElementById('metric-last-sync').textContent = `Failed (${new Date().toLocaleTimeString()})`;
            errorLog.classList.remove('hidden');
            errorLog.textContent = payload.errors ? payload.errors.join('\n') : 'Unknown error';
            eventSource.close();
        }
    };
    eventSource.onerror = () => {
        syncProgress.textContent = 'Connection lost, retrying...';
    };
}

// Event Listeners
document.getElementById('login-button').addEventListener('click', login);
document.getElementById('sync-button').addEventListener('click', triggerSync);
document.getElementById('retry-button').addEventListener('click', triggerSync);

// Allow login on Enter key
const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
if (usernameInput && passwordInput) {
    usernameInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            passwordInput.focus();
        }
    });
    passwordInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            login();
        }
    });
}

if (currentToken) {
    showDashboard();
} else {
    showLogin();
}
