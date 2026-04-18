# Hybrid IoT Data Logging & Visualization

A robust, local-first time-series data management system featuring a FastAPI backend, an Apache IoTDB cloud integration, and a real-time Plotly dashboard.

## 🏗️ System Architecture

1.  **Ingestion**: Sensors send JSON data to the `/ingest` endpoint.
2.  **Local Buffering**: Data is immediately appended to a local line-delimited JSON "TSFile" for offline resilience.
3.  **Sync Manager**: A background process (or manual trigger) reads the local buffer in batches and pushes it to **Apache IoTDB**.
4.  **Visualization**: A dual-chart dashboard compares real-time local data with synchronized historical data from IoTDB.

## Features

-   **🔐 Secure Access**: JWT-based authentication for ingestion, synchronization, and dashboard access.
-   **📊 Dual-Chart Dashboard**: Side-by-side Plotly visualizations showing "Local Buffer" vs. "IoTDB Cloud" datasets.
-   **🔄 Real-time Updates**: 
    -   5-second auto-refresh with a visual countdown timer.
    -   Visual "pulse" indicators on status bars when data updates successfully.
-   **🛰️ Resilient Sync**: 
    -   Asynchronous batch processing with offset tracking.
    -   Server-Sent Events (SSE) for live progress reporting (0-100%).
    -   Automatic file archiving after successful synchronization.
-   **🛡️ Reliability**: 
    -   Idempotent sync logic (resumes from last successful offset).
    -   Rate-limiting on sync triggers to prevent system abuse.
    -   Graceful error handling for offline IoTDB instances.

## Setup

1. **Environment Setup**:
   It is recommended to use a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate     # Windows
   ```

2. **Install Dependencies**:
   ```bash
   python -m pip install -r requirements.txt httpx pytest-asyncio
   ```

3. **Start the Application**:
   ```bash
   uvicorn app.main:app --reload
   ```

4. **Access the Dashboard**:
   Visit `http://localhost:8000/dashboard`

4. Login credentials:
   - operator / operator
   - supervisor / supervisor
   - admin / admin

## 🖥️ API Reference

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/token` | POST | Authenticate and receive JWT. |
| `/ingest` | POST | Append sensor reading to local buffer. |
| `/data` | GET | Retrieve recent records from local TSFile. |
| `/iotdb/data`| GET | Query timeseries data from Apache IoTDB. |
| `/sync` | POST | Trigger manual background sync job. |
| `/buffer/status`| GET | Check existence and size of local buffer file. |

## 📦 Buffering and Sync

Sensor readings are buffered locally in `data/tsfiles/buffer_current.tsfile` and tracked in `data/tsfiles/index.json`.

### Simulate Ingestion
Use the provided CLI tool to generate test data:
```bash
python ingest_sensor_data.py --random --count 5 --username operator --password operator
```

### Trigger sync

Click the `Sync to IoTDB` button on the dashboard. The button sends an authenticated request to `/sync` and opens a live sync status stream.

## Configuration

Environment variables supported:
- `LOCAL_TSFILE_PATH` - local buffer file path
- `LOCAL_ARCHIVE_DIR` - archive directory for completed buffers
- `LOCAL_INDEX_FILE` - local index file path
- `IOTDB_HOST`, `IOTDB_PORT`, `IOTDB_USER`, `IOTDB_PASSWORD`
- `JWT_SECRET_KEY`, `JWT_ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`
- `SYNC_INTERVAL`, `SYNC_RATE_LIMIT`, `SYNC_RATE_WINDOW`

Example Docker Compose snippet:
```yaml
services:
  iotdb:
    image: apache/iotdb:latest
    ports:
      - "6667:6667"
    networks:
      - iot_network

  fastapi:
    build:
      context: .
      dockerfile: Dockerfile.fastapi
    ports:
      - "8000:8000"
    environment:
      - IOTDB_HOST=iotdb
      - IOTDB_PORT=6667
      - IOTDB_USER=root
      - IOTDB_PASSWORD=root
      - LOCAL_TSFILE_PATH=/data/tsfiles/buffer_current.tsfile
    volumes:
      - ./data:/data
    depends_on:
      - iotdb
    networks:
      - iot_network
```

## Testing

Run unit and integration tests with:
```bash
pytest
```

## Notes

- The dashboard supports a local `EventSource` status stream for sync jobs.
- The sync process is idempotent by resuming unprocessed buffer offsets after failures.
- The ingestion helper authenticates with `/token` and sends buffered readings to `/ingest`.
