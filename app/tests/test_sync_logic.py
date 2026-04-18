import pytest
from unittest.mock import AsyncMock, MagicMock
from app.sync import SyncManager

@pytest.mark.asyncio
async def test_sync_job_empty_buffer():
    # Mock buffer store with 0 records
    mock_buffer = MagicMock()
    mock_buffer.total_unprocessed = AsyncMock(return_value=0)
    
    manager = SyncManager(buffer_store=mock_buffer, iotdb_client=MagicMock())
    job_id = await manager.trigger_sync()
    
    # Give it a moment to run task
    import asyncio
    await asyncio.sleep(0.1)
    
    status = manager.get_job_status(job_id)
    assert status["status"] == "completed"
    assert status["total_records"] == 0

@pytest.mark.asyncio
async def test_get_job_status_invalid_id():
    manager = SyncManager(buffer_store=MagicMock(), iotdb_client=MagicMock())
    assert manager.get_job_status("invalid-id") is None

if __name__ == "__main__":
    pytest.main([__file__])