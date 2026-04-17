# IoT Data Logging Hybrid (FastAPI + IoTDB)

This repository provides a local-first time-series dashboard with:
- FastAPI user authentication and JWT-protected sync endpoints.
- A Plotly-based dashboard at `/dashboard`.
- Local TSFile-style buffering of sensor data.
- Async batched sync to IoTDB with retry, progress reporting, and archive handling.

## Features
- JWT login and token-based dashboard access.
- Local offline-first buffering in `data/tsfiles/`.
- Async background sync jobs and periodic network-aware retry.
- Sync status delivered via Server-Sent Events.
- Manual retry button for failed sync attempts.
- Local config support for IoTDB host, port, and credentials.

## Setup

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```

2. Start the app locally:
   ```bash
   uvicorn app.main:app --reload
   ```

3. Open the dashboard:
   Visit `http://localhost:8000/dashboard`

4. Login credentials:
   - operator / operator
   - supervisor / supervisor
   - admin / admin

## Buffering and Sync

Sensor readings are buffered locally in `data/tsfiles/buffer_current.tsfile` and tracked in `data/tsfiles/index.json`.

### Simulate offline buffering

Use the ingestion helper:
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
