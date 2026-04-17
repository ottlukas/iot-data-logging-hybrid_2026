import logging
import asyncio
from typing import List
try:
    from iotdb.Session import Session
except ImportError:
    Session = None
from app.models import SensorReading
from app.config import settings

class SyncManager:
    def __init__(self):
        self.batch: List[SensorReading] = []
        self.sync_failures = 0
        self.sync_success = 0
        if Session is None:
            logging.warning("IoTDB not available - sync will be disabled")
            self.session = None
        else:
            try:
                self.session = Session(
                    settings.IOTDB_HOST,
                    settings.IOTDB_PORT,
                    settings.IOTDB_USER,
                    settings.IOTDB_PASSWORD
                )
                self.session.open()
                logging.info("Connected to IoTDB")
            except Exception as e:
                logging.warning(f"Failed to connect to IoTDB: {e} - sync will be disabled")
                self.session = None

    async def add_to_batch(self, reading: SensorReading):
        self.batch.append(reading)
        if len(self.batch) >= settings.BATCH_SIZE:
            await self.trigger_sync()

    async def trigger_sync(self):
        if not self.batch:
            return
        batch = self.batch.copy()
        self.batch.clear()
        try:
            success = await self.sync_batch_to_cloud(batch)
            if success:
                self.sync_success += 1
            else:
                self.sync_failures += 1
                self.batch.extend(batch)
        except Exception as e:
            logging.error(f"Sync failed: {e}")
            self.sync_failures += 1
            self.batch.extend(batch)

    async def sync_batch_to_cloud(self, batch: List[SensorReading]):
        if self.session is None:
            logging.warning("IoTDB not available - skipping sync")
            return False
        
        storage_group = "root.pharma"
        self.session.set_storage_group(storage_group)

        paths = []
        values = []
        timestamps = []

        for reading in batch:
            device_path = f"{storage_group}.{reading.device_id}"
            timestamp = reading.timestamp.isoformat()

            paths.extend([
                f"{device_path}.temperature",
                f"{device_path}.humidity",
                f"{device_path}.pressure",
            ])
            values.extend([
                reading.temperature,
                reading.humidity,
                reading.pressure,
            ])
            timestamps.append(timestamp)

        self.session.insert_records(
            timestamps,
            [paths] * len(batch),
            [values] * len(batch)
        )
        return True

    async def close(self):
        if self.session is not None:
            self.session.close()
    
    async def periodic_sync(self):
        """Periodic sync task that runs in the background."""
        try:
            while True:
                await asyncio.sleep(settings.SYNC_INTERVAL)
                await self.trigger_sync()
        except asyncio.CancelledError:
            pass