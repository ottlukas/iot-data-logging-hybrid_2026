import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.buffer import BufferStore
from app.config import settings
from app.iotdb_client import IoTDBClient
from app.models import SensorReading

logger = logging.getLogger(__name__)

@dataclass
class SyncJob:
    job_id: str
    status: str = "queued"
    total_records: int = 0
    processed_records: int = 0
    progress: float = 0.0
    errors: List[str] = field(default_factory=list)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class SyncManager:
    def __init__(self, buffer_store: BufferStore = None, iotdb_client: IoTDBClient = None):
        self.buffer_store = buffer_store or BufferStore()
        self.iotdb_client = iotdb_client or IoTDBClient()
        self.jobs: Dict[str, SyncJob] = {}
        self._lock = asyncio.Lock()

    async def trigger_sync(self) -> str:
        async with self._lock:
            job_id = str(uuid.uuid4())
            self.jobs[job_id] = SyncJob(job_id=job_id)
            asyncio.create_task(self._run_sync_job(job_id))
            return job_id

    async def _run_sync_job(self, job_id: str):
        job = self.jobs[job_id]
        job.status = "started"
        job.started_at = asyncio.get_event_loop().time().__str__()

        try:
            total = await self.buffer_store.total_unprocessed()
            job.total_records = total
            if total == 0:
                job.status = "completed"
                job.progress = 100.0
                job.finished_at = asyncio.get_event_loop().time().__str__()
                return

            await self.iotdb_client.connect()
            index = await self.buffer_store.get_index()
            path = self.buffer_store.buffer_path
            offset = index.get(path.name, 0)
            if offset >= 0:
                async for batch in self.buffer_store.read_batches(path, offset, settings.BATCH_SIZE):
                    try:
                        await self.iotdb_client.write_batch(batch)
                        offset += len(batch)
                        job.processed_records += len(batch)
                        job.progress = min(100.0, (job.processed_records / total) * 100)
                        await self.buffer_store.update_index(path, offset)
                    except Exception as sync_error:
                        job.errors.append(str(sync_error))
                        raise

            if await self.buffer_store.count_unprocessed_lines(path, offset) == 0:
                await self.buffer_store.archive_file(path)
                job.status = "completed"
                job.progress = 100.0
            else:
                job.status = "failed"
                job.errors.append("Sync ended without archiving buffer file")
        except Exception as exc:
            job.status = "failed"
            job.errors.append(str(exc))
            logger.exception("Sync job %s failed", job_id)
        finally:
            try:
                await self.iotdb_client.close()
            except Exception:
                pass
            job.finished_at = asyncio.get_event_loop().time().__str__()

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        job = self.jobs.get(job_id)
        if not job:
            return None
        return {
            "job_id": job.job_id,
            "status": job.status,
            "total_records": job.total_records,
            "processed_records": job.processed_records,
            "progress": round(job.progress, 1),
            "errors": job.errors,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
        }

    async def close(self):
        await self.iotdb_client.close()

    async def periodic_sync(self):
        try:
            while True:
                await asyncio.sleep(settings.SYNC_INTERVAL)
                await self.trigger_sync()
        except asyncio.CancelledError:
            pass
