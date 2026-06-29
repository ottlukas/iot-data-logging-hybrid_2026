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
let syncInProgress = false;

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
                updateSyncButtonState();
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
    updateSyncButtonState();
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
                bufferText.textContent = `Local TSfile: ${data.filename} (${data.size_kb} KB, ${data.record_count || '?'} records)`;
                bufferInfo.classList.add('active');
                const dot = bufferInfo.querySelector('.status-dot');
                if (dot) {
                    dot.classList.remove('error-dot');
                    triggerPulse(dot);
                }
                updateTimestamp('buffer-last-updated');
                document.getElementById('metric-buffer-status').textContent = `Active (${data.size_kb} KB, ${data.record_count || '?'} records)`;
            } else {
                bufferText.textContent = 'No local TSfile found (Buffer is empty)';
                bufferInfo.classList.remove('active');
                const dot = bufferInfo.querySelector('.status-dot');
                if (dot) dot.classList.remove('error-dot');
                document.getElementById('metric-buffer-status').textContent = 'Empty';
            }
            
            // Update sync status display
            updateSyncStatusDisplay(data);
        }
    } catch (err) {
        bufferText.textContent = 'Buffer status unavailable';
        document.getElementById('metric-buffer-status').textContent = 'Offline';
        const dot = bufferInfo.querySelector('.status-dot');
        if (dot) dot.classList.add('error-dot');
    }
}

function updateSyncStatusDisplay(statusData) {
    const syncStatusEl = document.getElementById('sync-status');
    const syncProgressEl = document.getElementById('sync-progress');
    const bufferStatusEl = document.getElementById('buffer-text');
    
    if (!syncStatusEl) return;
    
    // Map sync_status to display text
    const statusMap = {
        'idle_no_file': 'No local TSFile',
        'ready': 'Ready to sync',
        'sync_running': 'Sync running',
        'sync_success_archived': 'Sync successful — local TSFile archived',
        'sync_failed_retained': 'Sync failed — local TSFile retained',
        'sync_success_archive_failed': 'Sync successful but archiving failed'
    };
    
    const displayText = statusMap[statusData.sync_status] || statusData.sync_status;
    
    // Update the sync status display
    if (statusData.sync_running) {
        syncStatusEl.textContent = 'Status: Sync in progress...';
        syncProgressEl.textContent = 'Please wait';
    } else if (statusData.sync_status === 'sync_success_archived') {
        syncStatusEl.textContent = 'Status: ' + displayText;
        syncProgressEl.textContent = '';
    } else if (statusData.sync_status === 'sync_failed_retained') {
        syncStatusEl.textContent = 'Status: ' + displayText;
        syncProgressEl.textContent = 'You can retry the sync';
    } else if (statusData.sync_status === 'sync_success_archive_failed') {
        syncStatusEl.textContent = 'Status: ' + displayText;
        syncProgressEl.textContent = 'Warning: File cleanup failed';
    } else if (statusData.sync_status === 'ready') {
        syncStatusEl.textContent = 'Status: ' + displayText;
        syncProgressEl.textContent = '';
    } else {
        syncStatusEl.textContent = 'Status: ' + displayText;
        syncProgressEl.textContent = '';
    }
    
    // Update metric last sync
    const metricLastSync = document.getElementById('metric-last-sync');
    if (metricLastSync) {
        if (statusData.sync_status === 'sync_success_archived') {
            metricLastSync.textContent = `Successful (${new Date().toLocaleTimeString()})`;
        } else if (statusData.sync_status === 'sync_failed_retained') {
            metricLastSync.textContent = `Failed (${new Date().toLocaleTimeString()})`;
        } else if (statusData.sync_status === 'sync_running') {
            metricLastSync.textContent = 'Running...';
        } else if (statusData.sync_status === 'sync_success_archive_failed') {
            metricLastSync.textContent = `Success (cleanup failed) (${new Date().toLocaleTimeString()})`;
        }
    }
}

function updateSyncButtonState() {
    const syncButton = document.getElementById('sync-button');
    const retryButton = document.getElementById('retry-button');
    
    if (!syncButton) return;
    
    // Check buffer status and sync state
    fetch('/buffer/status', {
        headers: { Authorization: `Bearer ${currentToken}` }
    }).then(response => {
        if (response.ok) {
            return response.json();
        }
        throw new Error('Failed to get buffer status');
    }).then(data => {
        const hasFile = data.exists && data.record_count > 0;
        const isSyncRunning = data.sync_running || syncInProgress;
        
        // Enable/disable sync button
        if (hasFile && !isSyncRunning) {
            syncButton.disabled = false;
            syncButton.textContent = 'Sync to IoTDB';
            syncButton.classList.remove('disabled');
        } else if (isSyncRunning) {
            syncButton.disabled = true;
            syncButton.textContent = 'Sync in Progress...';
            syncButton.classList.add('disabled');
        } else {
            syncButton.disabled = true;
            syncButton.textContent = 'Sync to IoTDB';
            syncButton.classList.add('disabled');
        }
        
        // Update retry button
        if (retryButton) {
            if (data.sync_status === 'sync_failed_retained' && !isSyncRunning) {
                retryButton.classList.remove('hidden');
            } else {
                retryButton.classList.add('hidden');
            }
        }
    }).catch(err => {
        console.error('Error updating sync button state:', err);
        syncButton.disabled = true;
        syncButton.textContent = 'Sync to IoTDB';
    });
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

    // Check if sync is already in progress
    if (syncInProgress) {
        showToast('Sync is already in progress. Please wait.', 'warning');
        return;
    }

    // Check buffer status first
    try {
        const bufferResponse = await fetch('/buffer/status', {
            headers: { Authorization: `Bearer ${currentToken}` }
        });
        
        if (bufferResponse.ok) {
            const bufferData = await bufferResponse.json();
            if (!bufferData.exists || bufferData.record_count === 0) {
                showToast('No data to sync. Please ingest some data first.', 'warning');
                return;
            }
            if (bufferData.sync_running) {
                showToast('Sync is already in progress. Please wait.', 'warning');
                return;
            }
        }
    } catch (err) {
        showToast('Unable to check buffer status', 'error');
        return;
    }

    syncInProgress = true;
    updateSyncButtonState();

    try {
        const response = await fetch('/sync', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${currentToken}`,
            },
        });
        
        if (response.status === 409) {
            // Conflict - sync already in progress
            showToast('Sync is already in progress. Please wait.', 'warning');
            syncInProgress = false;
            updateSyncButtonState();
            return;
        }
        
        if (response.status === 400) {
            const result = await response.json();
            showToast(result.detail || 'No data to sync', 'warning');
            syncInProgress = false;
            updateSyncButtonState();
            return;
        }
        
        if (!response.ok) {
            const result = await response.json();
            showToast(result.detail || 'Sync request failed', 'error');
            syncInProgress = false;
            updateSyncButtonState();
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
        syncInProgress = false;
        updateSyncButtonState();
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
            syncInProgress = false;
            showToast('Sync completed successfully', 'success');
            document.getElementById('metric-last-sync').textContent = new Date().toLocaleTimeString();
            updateBufferStatus();
            fetchAndPlotData();
            fetchAndPlotIoTDB();
            updateSyncButtonState();
            secondsRemaining = REFRESH_SECONDS;
            updateCountdownUI();
            eventSource.close();
        }
        if (payload.status === 'failed') {
            syncInProgress = false;
            showToast('Sync failed', 'error');
            document.getElementById('metric-last-sync').textContent = `Failed (${new Date().toLocaleTimeString()})`;
            errorLog.classList.remove('hidden');
            errorLog.textContent = payload.errors ? payload.errors.join('\n') : 'Unknown error';
            updateBufferStatus();
            updateSyncButtonState();
            eventSource.close();
        }
    };
    eventSource.onerror = () => {
        syncProgress.textContent = 'Connection lost, retrying...';
        syncInProgress = false;
        updateSyncButtonState();
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
