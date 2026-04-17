async function fetchAndPlotData() {
    const response = await fetch('/data');
    const data = await response.json();
    const plotData = [{
        x: data.data.map(d => new Date(d.timestamp)),
        y: data.data.map(d => d.temperature),
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Temperature (°C)'
    }, {
        x: data.data.map(d => new Date(d.timestamp)),
        y: data.data.map(d => d.humidity),
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Humidity (%)'
    }, {
        x: data.data.map(d => new Date(d.timestamp)),
        y: data.data.map(d => d.pressure),
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Pressure (hPa)'
    }];

    const layout = {
        title: 'IoT Sensor Data',
        xaxis: { title: 'Timestamp' },
        yaxis: { title: 'Value' }
    };

    Plotly.newPlot('plot', plotData, layout);
}

document.getElementById('sync-button').addEventListener('click', async () => {
    const response = await fetch('/sync');
    const data = await response.json();
    document.getElementById('sync-status').textContent = `Synced: ${data.status}`;
    fetchAndPlotData();
});

fetchAndPlotData();