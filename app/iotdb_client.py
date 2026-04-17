import asyncio
import logging
from typing import List

try:
    from iotdb.Session import Session
except ImportError:  # pragma: no cover
    Session = None

from app.config import settings
from app.models import SensorReading

logger = logging.getLogger(__name__)

class IoTDBClient:
    def __init__(self, host: str = None, port: int = None, user: str = None, password: str = None):
        self.host = host or settings.IOTDB_HOST
        self.port = port or settings.IOTDB_PORT
        self.user = user or settings.IOTDB_USER
        self.password = password or settings.IOTDB_PASSWORD
        self.session = None

    async def connect(self):
        if Session is None:
            raise RuntimeError("IoTDB python client is not installed")

        def open_session():
            session = Session(self.host, self.port, self.user, self.password)
            session.open()
            return session

        last_exception = None
        for attempt in range(1, settings.IOTDB_CONNECT_RETRIES + 1):
            try:
                self.session = await asyncio.to_thread(open_session)
                logger.info("Connected to IoTDB at %s:%s", self.host, self.port)
                return
            except Exception as exc:
                last_exception = exc
                logger.warning(
                    "IoTDB connection attempt %s/%s failed: %s",
                    attempt,
                    settings.IOTDB_CONNECT_RETRIES,
                    exc,
                )
                if attempt == settings.IOTDB_CONNECT_RETRIES:
                    break
                await asyncio.sleep(settings.IOTDB_CONNECT_BACKOFF_SECONDS)

        raise RuntimeError("Unable to connect to IoTDB") from last_exception

    async def close(self):
        if self.session is not None:
            await asyncio.to_thread(self.session.close)
            self.session = None

    async def write_batch(self, batch: List[SensorReading]):
        if not batch:
            return
        if self.session is None:
            raise RuntimeError("IoTDB session is not connected")

        def insert_records():
            storage_group = "root.factory"
            try:
                self.session.set_storage_group(storage_group)
            except Exception as exc:
                if "already been created" not in str(exc):
                    raise

            timestamps = []
            device_ids = []
            measurements = []
            data_types = []
            values = []

            for reading in batch:
                device_id = f"{storage_group}.{reading.device_id}"
                timestamps.append(int(reading.timestamp.timestamp() * 1000))
                device_ids.append(device_id)
                measurements.append(["temperature", "humidity", "pressure"])
                data_types.append([3, 3, 3])  # FLOAT type codes for IoTDB
                values.append([
                    reading.temperature if reading.temperature is not None else 0.0,
                    reading.humidity if reading.humidity is not None else 0.0,
                    reading.pressure if reading.pressure is not None else 0.0,
                ])

            if hasattr(self.session, "insert_records"):
                self.session.insert_records(device_ids, timestamps, measurements, data_types, values)
            elif hasattr(self.session, "insert_record"):
                for ts, device, measurement, row_types, row_values in zip(
                    timestamps, device_ids, measurements, data_types, values
                ):
                    self.session.insert_record(device, ts, measurement, row_types, row_values)
            else:
                raise RuntimeError("Connected IoTDB session does not support insert_records or insert_record")

        await asyncio.to_thread(insert_records)
        logger.debug("Wrote %s readings to IoTDB", len(batch))
