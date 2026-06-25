import os
import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.storage import buffer_store
from app.config import settings


@pytest.fixture
def client_with_temp_buffer(tmp_path, monkeypatch):
    # Direct writes to a temporary file
    temp_buffer = tmp_path / "test_buffer.tsfile"
    temp_archive = tmp_path / "archive"
    temp_index = tmp_path / "index.json"
    
    monkeypatch.setattr(settings, "LOCAL_TSFILE_PATH", str(temp_buffer))
    monkeypatch.setattr(settings, "LOCAL_ARCHIVE_DIR", str(temp_archive))
    monkeypatch.setattr(settings, "LOCAL_INDEX_FILE", str(temp_index))
    
    monkeypatch.setattr(buffer_store, "buffer_path", temp_buffer)
    
    # Return TestClient
    with TestClient(app) as client:
        yield client


def get_auth_headers(client: TestClient) -> dict:
    response = client.post("/token", data={"username": "operator", "password": "operator"})
    assert response.status_code == 200
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_health_endpoint(client_with_temp_buffer: TestClient):
    response = client_with_temp_buffer.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_ingest_unauthorized(client_with_temp_buffer: TestClient):
    payload = {
        "device_id": "line1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": 25.5,
        "humidity": 55.0,
        "pressure": 1012.5,
        "electronic_signature": "operator1"
    }
    response = client_with_temp_buffer.post("/ingest", json=payload)
    assert response.status_code == 401


def test_ingest_invalid_payload(client_with_temp_buffer: TestClient):
    headers = get_auth_headers(client_with_temp_buffer)
    # Missing required timestamp
    payload = {
        "device_id": "line1",
        "temperature": "invalid-float-value",  # invalid type
        "humidity": 55.0,
        "pressure": 1012.5,
        "electronic_signature": "operator1"
    }
    response = client_with_temp_buffer.post("/ingest", json=payload, headers=headers)
    assert response.status_code == 422  # validation error


def test_ingest_successful(client_with_temp_buffer: TestClient):
    headers = get_auth_headers(client_with_temp_buffer)
    payload = {
        "device_id": "line1",
        "timestamp": "2026-06-25T18:00:00Z",
        "temperature": 24.8,
        "humidity": 52.3,
        "pressure": 1011.2,
        "electronic_signature": "operator1"
    }
    response = client_with_temp_buffer.post("/ingest", json=payload, headers=headers)
    assert response.status_code == 200
    assert response.json() == {"status": "accepted"}
    
    # Check that file exists now
    assert buffer_store.buffer_path.exists()
    assert buffer_store.buffer_path.stat().st_size > 0


def test_buffer_status_unauthorized(client_with_temp_buffer: TestClient):
    response = client_with_temp_buffer.get("/buffer/status")
    assert response.status_code == 401


def test_buffer_status_authorized(client_with_temp_buffer: TestClient):
    headers = get_auth_headers(client_with_temp_buffer)
    
    # Check empty status
    if buffer_store.buffer_path.exists():
        buffer_store.buffer_path.unlink()
        
    response = client_with_temp_buffer.get("/buffer/status", headers=headers)
    assert response.status_code == 200
    assert response.json()["exists"] is False
    assert response.json()["size_kb"] == 0
    
    # Ingest data and check status again
    payload = {
        "device_id": "line1",
        "timestamp": "2026-06-25T18:00:00Z",
        "temperature": 24.8,
        "humidity": 52.3,
        "pressure": 1011.2,
        "electronic_signature": "operator1"
    }
    client_with_temp_buffer.post("/ingest", json=payload, headers=headers)
    
    response = client_with_temp_buffer.get("/buffer/status", headers=headers)
    assert response.status_code == 200
    assert response.json()["exists"] is True
    assert response.json()["size_kb"] > 0
    assert response.json()["filename"] == buffer_store.buffer_path.name


def test_iotdb_data_unauthorized(client_with_temp_buffer: TestClient):
    response = client_with_temp_buffer.get("/iotdb/data")
    assert response.status_code == 401


def test_iotdb_data_success(client_with_temp_buffer: TestClient, monkeypatch):
    headers = get_auth_headers(client_with_temp_buffer)
    
    # Mock IoTDBClient in app state
    mock_iotdb = AsyncMock()
    mock_iotdb.session = MagicMock()
    mock_iotdb.connect = AsyncMock()
    
    mock_data = [
        {"device_id": "line1", "timestamp": 1625000000000, "temperature": 22.0, "humidity": 45.0, "pressure": 1013.0}
    ]
    mock_iotdb.query_timeseries = AsyncMock(return_value=mock_data)
    
    # Replace client in app state
    monkeypatch.setattr(app.state, "iotdb_client", mock_iotdb)
    
    response = client_with_temp_buffer.get("/iotdb/data?device=line1", headers=headers)
    assert response.status_code == 200
    assert response.json() == {"data": mock_data}
    mock_iotdb.query_timeseries.assert_called_once_with("line1", 200, None)


def test_iotdb_data_error_handling(client_with_temp_buffer: TestClient, monkeypatch):
    headers = get_auth_headers(client_with_temp_buffer)
    
    # Mock IoTDBClient to raise exception during query
    mock_iotdb = AsyncMock()
    mock_iotdb.session = MagicMock()
    mock_iotdb.connect = AsyncMock()
    mock_iotdb.query_timeseries = AsyncMock(side_effect=Exception("Connection reset by peer"))
    
    monkeypatch.setattr(app.state, "iotdb_client", mock_iotdb)
    
    response = client_with_temp_buffer.get("/iotdb/data", headers=headers)
    assert response.status_code == 503
    assert "IoTDB error" in response.json()["detail"]
