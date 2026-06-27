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

## 💻 Windows and Linux Setup

This system is designed to run seamlessly on both Linux and Windows operating systems. All filesystem path operations are platform-independent.

### 📋 Prerequisites
- **Python**: Version 3.10, 3.11, or 3.12 (Python 3.13 is supported in fallback mode).
- **Docker & Docker Compose**: (Optional, required for containerized deployment).

---

### 🐍 Virtual Environment Setup

#### Linux/macOS
Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### Windows PowerShell
Create and activate a virtual environment (ensure your ExecutionPolicy allows script execution if needed, e.g., `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`):
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

---

### 📦 Dependency Installation

Install the required packages in your active virtual environment:
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt httpx pytest-asyncio requests
```

> [!NOTE]
> The `tsfile` library requires C++ compilation headers. If the `tsfile` library fails to compile or install on your platform, the application automatically triggers a robust **JSON fallback buffering mode** to keep sensor logging functional.

---

### 🚀 Running the FastAPI App Locally

Start the application with Uvicorn:
```bash
python -m uvicorn app.main:app --reload
```
Once running, visit the interactive dashboard at:
👉 **[http://localhost:8000/dashboard](http://localhost:8000/dashboard)**

Default login accounts:
- Operator: `operator` / `operator`
- Supervisor: `supervisor` / `supervisor`
- Admin: `admin` / `admin`

---

### 🧪 Running Tests

Run the complete, cross-platform test suite:
```bash
python -m pytest
```

---

### 🐳 Running with Docker Compose

To spin up the application along with Apache IoTDB using Docker:

#### Linux / macOS
```bash
docker compose up --build
```

#### Windows (using Docker Desktop)
Ensure that you are running Docker Desktop and use:
```powershell
docker compose up --build
```

---

## ⚙️ Configuration & Environment Variables

The application can be configured using environment variables. Paths are normalized automatically for the current operating system.

### Default Local Data Paths
- **Default Buffer Path**: `data/tsfiles/buffer_current.tsfile` (on Windows resolves using `\`, on Linux using `/`)
- **Default Archive Dir**: `data/tsfiles/archive/`
- **Default Index Path**: `data/tsfiles/index.json`

### Supported Environment Variables
| Variable | Description | Default Value |
| :--- | :--- | :--- |
| `LOCAL_TSFILE_PATH` | Path to the local buffer file | `data/tsfiles/buffer_current.tsfile` |
| `LOCAL_ARCHIVE_DIR` | Directory for synchronized buffer archives | `data/tsfiles/archive` |
| `LOCAL_INDEX_FILE` | Path to the local sync index offset file | `data/tsfiles/index.json` |
| `IOTDB_HOST` | Apache IoTDB host address | `localhost` |
| `IOTDB_PORT` | Apache IoTDB RPC port | `6667` |
| `IOTDB_USER` | Apache IoTDB username | `root` |
| `IOTDB_PASSWORD` | Apache IoTDB password | `root` |
| `IOTDB_CONNECT_RETRIES` | Max connection retry attempts for IoTDB | `6` |
| `IOTDB_CONNECT_BACKOFF_SECONDS` | Delay between connection retry attempts | `2.0` |
| `BATCH_SIZE` | Sync batch size | `20` |
| `SYNC_INTERVAL` | Time in seconds between sync scans | `30` |
| `SYNC_RATE_LIMIT` | Maximum sync triggers allowed in window | `3` |
| `SYNC_RATE_WINDOW` | Time window in seconds for rate limiting | `60` |
| `JWT_SECRET_KEY` | Secret key for signing access tokens | `change-me-super-secret` |
| `JWT_ALGORITHM` | Algorithm used for JWT | `HS256` |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | Token validity lifetime in minutes | `60` |

---

## 🔍 Troubleshooting

### 📁 Path Issues & Separators
- **Problem**: Logged paths look strange or raise errors on Windows.
- **Solution**: The application uses Python's `pathlib.Path` globally. Always format any custom paths passed via environment variables cleanly (e.g., `LOCAL_TSFILE_PATH="C:\my\path\buffer.tsfile"` on Windows, or `LOCAL_TSFILE_PATH="/my/path/buffer.tsfile"` on Linux). Slashes are automatically normalized during Settings initialization.

### 📦 Missing `tsfile` Package
- **Problem**: Warning: `tsfile package not installed. Falling back to JSON appending`.
- **Solution**: The `tsfile` library relies on C/C++ compilation. If a binary wheel is unavailable for your Python version or OS, installing it from source requires tools like `gcc` (Linux) or MSVC C++ Build Tools (Windows). Without them, the backend runs in a robust JSON-fallback mode which writes line-delimited JSON. You do not need to compile `tsfile` to use this hybrid system.

### 🔌 IoTDB Not Reachable
- **Problem**: `Unable to connect to IoTDB` warning on startup, or sync jobs fail.
- **Solution**:
  1. If running locally, check that Apache IoTDB is started and listening on port `6667`.
  2. If using Docker, check that both containers are on the `iot_network` bridge network.
  3. Ensure that the `IOTDB_HOST` environment variable is set to `iotdb` (the service name) when running under Docker, and `localhost` (or `127.0.0.1`) when running the FastAPI app directly.

### 🐳 Docker Volume / Path Behavior on Windows
- **Problem**: FastAPI container cannot read or write to `./data` mount on Windows, or files do not update.
- **Solution**:
  - Docker Desktop on Windows sometimes requires File Sharing permissions to be enabled for your project directory (check settings under *Resources > File sharing*).
  - Use relative path syntax `./data` in your docker-compose file. Avoid using absolute host paths starting with `C:\...` unless mapped correctly.

### 🔑 Permission Issues
- **Problem**: `PermissionError` when archiving or writing files (especially on Windows).
- **Solution**:
  - Windows locks files when they are opened by any process or thread. The system is engineered to explicitly release file handles (`reader.close()`/`writer.close()`) and uses an `asyncio.Lock` around archive operations to prevent race conditions during concurrent requests. Ensure no external processes (like an open editor or Excel view) are locking the TSFile buffer.

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
```bash
python3 ingest_sensor_data.py --random --count 5 --username operator --password operator
```

### Trigger sync

Click the `Sync to IoTDB` button on the dashboard. The button sends an authenticated request to `/sync` and opens a live sync status stream.

## Notes

- The dashboard supports a local `EventSource` status stream for sync jobs.
- The sync process is idempotent by resuming unprocessed buffer offsets after failures.
- The ingestion helper authenticates with `/token` and sends buffered readings to `/ingest`.
