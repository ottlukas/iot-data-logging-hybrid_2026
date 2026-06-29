import asyncio
import aiofiles
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.buffer import BufferStore
from app.config import settings
from app.iotdb_client import IoTDBClient
from app.models import SensorReading

logger = logging.getLogger(__name__)


class SyncStatus(Enum):
    """Explicit state machine for sync status."""
    IDLE_NO_FILE = "idle_no_file"
    READY = "ready"
    SYNC_RUNNING = "sync_running"
    SYNC_SUCCESS_ARCHIVED = "sync_success_archived"
    SYNC_FAILED_RETAINED = "sync_failed_retained"
    SYNC_SUCCESS_ARCHIVE_FAILED = "sync_success_archive_failed"


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
    sync_status: SyncStatus = SyncStatus.IDLE_NO_FILE


class SyncManager:
    def __init__(self, buffer_store: BufferStore = None, iotdb_client: IoTDBClient = None):
        self.buffer_store = buffer_store or BufferStore()
        self.iotdb_client = iotdb_client or IoTDBClient()
        self.jobs: Dict[str, SyncJob] = {}
        self._lock = asyncio.Lock()
        self._sync_lock = asyncio.Lock()  # Prevent concurrent syncs
        self._current_job_id: Optional[str] = None

    async def trigger_sync(self) -> str:
        """
        Trigger a manual sync job. Only one sync can run at a time.
        Returns the job_id if successful, raises HTTPException if sync is already running.
        """
        async with self._lock:
            # Check if a sync is already running
            if self._current_job_id and self._current_job_id in self.jobs:
                current_job = self.jobs[self._current_job_id]
                if current_job.status in ["started", "queued"]:
                    logger.warning("Sync already in progress (job: %s). Rejecting duplicate request.", self._current_job_id)
                    raise RuntimeError("Sync already in progress")

            # Check if TSFile exists and has data
            buffer_path = self.buffer_store.buffer_path
            if not buffer_path.exists():
                logger.info("No TSFile exists at %s. Cannot start sync.", buffer_path)
                raise RuntimeError("No TSFile exists to sync")

            # Check if TSFile has data
            total = await self.buffer_store.total_unprocessed()
            if total == 0:
                logger.info("TSFile exists but contains no unprocessed data. Cannot start sync.")
                raise RuntimeError("No data to sync")

            job_id = str(uuid.uuid4())
            job = SyncJob(job_id=job_id, sync_status=SyncStatus.READY)
            self.jobs[job_id] = job
            self._current_job_id = job_id
            
            logger.info("TSFile detected at %s with %d records. Manual sync requested (job: %s).", 
                       buffer_path, total, job_id)
            
            # Start the sync job
            asyncio.create_task(self._run_sync_job(job_id))
            return job_id

    async def _run_sync_job(self, job_id: str):
        """
        Run the sync job: write all data to IoTDB, then archive TSFile on success.
        """
        job = self.jobs[job_id]
        buffer_path = self.buffer_store.buffer_path
        
        try:
            async with self._sync_lock:
                # Re-check that we're still the current job
                if self._current_job_id != job_id:
                    logger.warning("Job %s is no longer current. Aborting.", job_id)
                    job.status = "failed"
                    job.errors.append("Sync job superseded by another job")
                    job.sync_status = SyncStatus.SYNC_FAILED_RETAINED
                    job.finished_at = datetime.now(timezone.utc).isoformat()
                    return

                job.status = "started"
                job.started_at = datetime.now(timezone.utc).isoformat()
                job.sync_status = SyncStatus.SYNC_RUNNING
                
                logger.info("Sync job %s started. Processing TSFile: %s", job_id, buffer_path.name)

                total = await self.buffer_store.total_unprocessed()
                job.total_records = total
                
                if total == 0:
                    logger.info("Sync job %s: No records to process.", job_id)
                    job.status = "completed"
                    job.progress = 100.0
                    job.sync_status = SyncStatus.IDLE_NO_FILE
                    job.finished_at = datetime.now(timezone.utc).isoformat()
                    self._current_job_id = None
                    return

                # Connect to IoTDB
                await self.iotdb_client.connect()
                
                # Get current offset from index
                index = await self.buffer_store.get_index()
                offset = index.get(buffer_path.name, 0)
                
                logger.info("Sync job %s: Starting sync for %s from offset %d with %d records", 
                           job_id, buffer_path.name, offset, total)

                # Process batches
                all_success = True
                batch_count = 0
                
                async for batch in self.buffer_store.read_batches(buffer_path, offset, settings.BATCH_SIZE):
                    batch_count += 1
                    try:
                        logger.debug("Sync job %s: Writing batch %d with %d records", 
                                    job_id, batch_count, len(batch))
                        await self.iotdb_client.write_batch(batch)
                        offset += len(batch)
                        job.processed_records += len(batch)
                        job.progress = min(100.0, (job.processed_records / total) * 100)
                        
                        logger.info("Sync job %s: Batch %d processed successfully. %d/%d records written.", 
                                   job_id, batch_count, job.processed_records, total)
                        
                        # Update index after each successful batch
                        await self.buffer_store.update_index(buffer_path, offset)
                        
                    except Exception as sync_error:
                        error_msg = str(sync_error)
                        job.errors.append(error_msg)
                        logger.error("Sync job %s: Batch %d failed with error: %s", 
                                    job_id, batch_count, error_msg)
                        all_success = False
                        raise

                # Verify all records were processed
                unprocessed = await self.buffer_store.count_unprocessed_lines(buffer_path, offset)
                
                if all_success and unprocessed == 0:
                    # All records successfully written to IoTDB
                    logger.info("Sync job %s: All %d records successfully written to IoTDB. Verifying completion.", 
                               job_id, job.processed_records)
                    
                    # Attempt to archive the TSFile
                    try:
                        await self._archive_tsfile_after_sync(buffer_path)
                        job.status = "completed"
                        job.progress = 100.0
                        job.sync_status = SyncStatus.SYNC_SUCCESS_ARCHIVED
                        logger.info("Sync job %s: TSFile %s archived successfully after sync.", 
                                   job_id, buffer_path.name)
                    except Exception as archive_error:
                        error_msg = str(archive_error)
                        job.errors.append(f"Sync successful but archiving failed: {error_msg}")
                        job.status = "completed"
                        job.progress = 100.0
                        job.sync_status = SyncStatus.SYNC_SUCCESS_ARCHIVE_FAILED
                        logger.error("Sync job %s: IoTDB sync succeeded but TSFile archiving failed: %s. TSFile retained at %s.", 
                                    job_id, error_msg, buffer_path)
                        logger.warning("Sync job %s: TSFile retained after successful sync due to archiving failure.", job_id)
                        
                else:
                    # Sync failed or didn't complete
                    job.status = "failed"
                    job.sync_status = SyncStatus.SYNC_FAILED_RETAINED
                    if unprocessed > 0:
                        job.errors.append(f"Sync ended with {unprocessed} unprocessed records")
                    logger.error("Sync job %s: Failed or incomplete. TSFile retained at %s.", 
                                job_id, buffer_path)
                    logger.info("Sync job %s: TSFile retained after failure.", job_id)

        except Exception as exc:
            job.status = "failed"
            job.errors.append(str(exc))
            job.sync_status = SyncStatus.SYNC_FAILED_RETAINED
            logger.exception("Sync job %s failed: %s", job_id, exc)
            logger.info("Sync job %s: TSFile retained after exception.", job_id)
        finally:
            job.finished_at = datetime.now(timezone.utc).isoformat()
            self._current_job_id = None

    async def _archive_tsfile_after_sync(self, path: Path):
        """
        Archive the TSFile after successful sync to IoTDB.
        This is the primary completion behavior.
        """
        if not path.exists():
            logger.warning("TSFile %s does not exist. Nothing to archive.", path)
            return
        
        # Use the buffer_store's archive_file method which handles the archiving correctly
        await self.buffer_store.archive_file(path)
        
        # Clear the index for this file
        index_path = Path(settings.LOCAL_INDEX_FILE)
        if index_path.exists():
            try:
                index = await self.buffer_store.get_index()
                if path.name in index:
                    del index[path.name]
                index_path.parent.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(index_path, mode="w") as f:
                    await f.write(json.dumps(index))
                logger.debug("Cleared index entry for %s", path.name)
            except Exception as e:
                logger.warning("Failed to clear index for %s: %s", path.name, e)

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a specific sync job.
        """
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
            "sync_status": job.sync_status.value,
        }

    def get_current_sync_status(self) -> Dict[str, Any]:
        """
        Get the current overall sync status for the dashboard.
        """
        buffer_path = self.buffer_store.buffer_path
        exists = buffer_path.exists()
        
        # Determine if sync is currently running
        sync_running = self._current_job_id is not None and \
                       self._current_job_id in self.jobs and \
                       self.jobs[self._current_job_id].status in ["started", "queued"]
        
        # Determine current status
        if sync_running:
            return {
                "status": "sync_running",
                "file_exists": exists,
                "file_size": buffer_path.stat().st_size if exists else 0,
                "file_name": buffer_path.name,
                "last_modified": datetime.fromtimestamp(buffer_path.stat().st_mtime).isoformat() if exists else None,
                "sync_status": SyncStatus.SYNC_RUNNING.value,
                "job_id": self._current_job_id,
                "sync_running": True,
            }
        
        # Check if there's a recent completed job
        completed_jobs = [j for j in self.jobs.values() if j.status == "completed"]
        if completed_jobs:
            latest_job = max(completed_jobs, key=lambda j: j.finished_at or "")
            if latest_job.sync_status == SyncStatus.SYNC_SUCCESS_ARCHIVED:
                return {
                    "status": "sync_success_archived",
                    "file_exists": exists,
                    "file_size": buffer_path.stat().st_size if exists else 0,
                    "file_name": buffer_path.name,
                    "last_modified": datetime.fromtimestamp(buffer_path.stat().st_mtime).isoformat() if exists else None,
                    "sync_status": SyncStatus.SYNC_SUCCESS_ARCHIVED.value,
                    "job_id": latest_job.job_id,
                    "sync_running": False,
                }
            elif latest_job.sync_status == SyncStatus.SYNC_SUCCESS_ARCHIVE_FAILED:
                return {
                    "status": "sync_success_archive_failed",
                    "file_exists": exists,
                    "file_size": buffer_path.stat().st_size if exists else 0,
                    "file_name": buffer_path.name,
                    "last_modified": datetime.fromtimestamp(buffer_path.stat().st_mtime).isoformat() if exists else None,
                    "sync_status": SyncStatus.SYNC_SUCCESS_ARCHIVE_FAILED.value,
                    "job_id": latest_job.job_id,
                    "sync_running": False,
                }
        
        # Check for failed jobs
        failed_jobs = [j for j in self.jobs.values() if j.status == "failed"]
        if failed_jobs:
            latest_failed = max(failed_jobs, key=lambda j: j.finished_at or "")
            return {
                "status": "sync_failed",
                "file_exists": exists,
                "file_size": buffer_path.stat().st_size if exists else 0,
                "file_name": buffer_path.name,
                "last_modified": datetime.fromtimestamp(buffer_path.stat().st_mtime).isoformat() if exists else None,
                "sync_status": SyncStatus.SYNC_FAILED_RETAINED.value,
                "job_id": latest_failed.job_id,
                "sync_running": False,
            }
        
        # Default: check if file exists
        if exists:
            return {
                "status": "ready",
                "file_exists": True,
                "file_size": buffer_path.stat().st_size,
                "file_name": buffer_path.name,
                "last_modified": datetime.fromtimestamp(buffer_path.stat().st_mtime).isoformat(),
                "sync_status": SyncStatus.READY.value,
                "job_id": None,
                "sync_running": False,
            }
        else:
            return {
                "status": "idle_no_file",
                "file_exists": False,
                "file_size": 0,
                "file_name": buffer_path.name,
                "last_modified": None,
                "sync_status": SyncStatus.IDLE_NO_FILE.value,
                "job_id": None,
                "sync_running": False,
            }

    async def close(self):
        """
        Clean up resources.
        """
        await self.iotdb_client.close()

    async def periodic_sync(self):
        """
        DISABLED: This method is no longer used.
        Sync is now manual-only via the /sync endpoint.
        This method is kept for backward compatibility but does nothing.
        """
        logger.info("Periodic sync is disabled. Use manual sync via /sync endpoint.")
        try:
            while True:
                await asyncio.sleep(settings.SYNC_INTERVAL)
                logger.debug("Periodic sync check - no action taken (manual sync only)")
        except asyncio.CancelledError:
            pass
