import asyncio
import os
from pathlib import Path
import aiofiles
from fastapi.testclient import TestClient
from app.config import settings
from app.buffer import BufferStore
from app.models import SensorReading
from app.sync import SyncManager, SyncJob
from app.main import app

class FakeIoTDBClient:
    def __init__(self):
        self.written_batches = []

    async def connect(self):
        return

    async def write_batch(self, batch):
        self.written_batches.append(batch)

    async def close(self):
        return


def test_sync_manager_processes_buffer_and_archives(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, 'LOCAL_TSFILE_PATH', str(tmp_path / 'buffer_current.tsfile'))
    monkeypatch.setattr(settings, 'LOCAL_ARCHIVE_DIR', str(tmp_path / 'archive'))
    monkeypatch.setattr(settings, 'LOCAL_INDEX_FILE', str(tmp_path / 'index.json'))

    buffer_store = BufferStore()
    readings = [
        SensorReading(
            device_id='machine1',
            timestamp='2026-01-01T00:00:00',
            temperature=20.1,
            humidity=40.0,
            pressure=1010.0,
            electronic_signature='operator1',
        ),
        SensorReading(
            device_id='machine1',
            timestamp='2026-01-01T00:01:00',
            temperature=20.3,
            humidity=40.5,
            pressure=1010.5,
            electronic_signature='operator1',
        ),
    ]

    for reading in readings:
        asyncio.run(buffer_store.append_reading(reading))

    fake_client = FakeIoTDBClient()
    manager = SyncManager(buffer_store=buffer_store, iotdb_client=fake_client)
    job_id = 'test-job'
    manager.jobs[job_id] = SyncJob(job_id=job_id)

    asyncio.run(manager._run_sync_job(job_id))

    assert manager.jobs[job_id].status == 'completed'
    assert len(fake_client.written_batches) == 1
    archive_files = list(Path(settings.LOCAL_ARCHIVE_DIR).glob('buffer_current*.tsfile'))
    assert archive_files, 'Buffer file should have moved to archive'


def test_archive_file_falls_back_when_rename_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, 'LOCAL_TSFILE_PATH', str(tmp_path / 'buffer_current.tsfile'))
    monkeypatch.setattr(settings, 'LOCAL_ARCHIVE_DIR', str(tmp_path / 'archive'))
    monkeypatch.setattr(settings, 'LOCAL_INDEX_FILE', str(tmp_path / 'index.json'))

    buffer_store = BufferStore()
    asyncio.run(buffer_store.append_reading(SensorReading(
        device_id='machine1',
        timestamp='2026-01-01T00:00:00',
        temperature=20.1,
        humidity=40.0,
        pressure=1010.0,
        electronic_signature='operator1',
    )))

    original_rename = os.rename

    def failing_rename(src, dst):
        raise OSError("Invalid cross-device link")

    monkeypatch.setattr(os, 'rename', failing_rename)
    path = buffer_store.buffer_path

    asyncio.run(buffer_store.archive_file(path))

    archive_files = list(Path(settings.LOCAL_ARCHIVE_DIR).glob('buffer_current*.tsfile'))
    assert archive_files, 'Buffer file should still be archived when rename fails'
    assert not path.exists(), 'Original buffer file should be moved'


def test_sync_endpoint_requires_jwt_and_returns_job_id():
    test_client = TestClient(app)
    response = test_client.post('/token', data={'username': 'operator', 'password': 'operator'})
    assert response.status_code == 200
    token = response.json()['access_token']

    class DummySyncManager:
        async def trigger_sync(self):
            return 'dummy-job-id'

    test_client.app.state.sync_manager = DummySyncManager()
    response = test_client.post('/sync', headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 200
    assert response.json()['job_id'] == 'dummy-job-id'


def test_tsfile_reader_error_handling(tmp_path):
    """Tests that the reader handles missing or corrupted binary files gracefully."""
    from app.sensor_data import read_tsfile_points
    import pytest

    # Case 1: File does not exist
    missing_file = str(tmp_path / "non_existent.tsfile")
    
    async def run_missing():
        points = []
        async for p in read_tsfile_points([missing_file], ["temperature"], 0, 9999999999999):
            points.append(p)
        return points

    results = asyncio.run(run_missing())
    assert results == []

    # Case 2: Corrupted File (Writing random bytes)
    corrupt_file = tmp_path / "corrupt.tsfile"
    corrupt_file.write_bytes(b"NOT_A_TSFILE_HEADER" * 10)
    
    async def run_corrupt():
        points = []
        async for p in read_tsfile_points([str(corrupt_file)], ["temperature"], 0, 9999999999999):
            points.append(p)
        return points

    # Should not raise exception, but log error and return empty list or fallback to JSON
    results = asyncio.run(run_corrupt())
    assert isinstance(results, list)

    # Case 3: Empty File
    empty_file = tmp_path / "empty.tsfile"
    empty_file.write_bytes(b"")
    
    async def run_empty():
        points = []
        async for p in read_tsfile_points([str(empty_file)], ["temperature"], 0, 9999999999999):
            points.append(p)
        return points
    results = asyncio.run(run_empty())
    assert results == []
