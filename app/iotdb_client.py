import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional

try:
    from iotdb.Session import Session
except ImportError:  # pragma: no cover
    Session = None

from app.config import settings
from app.models import SensorReading

logger = logging.getLogger(__name__)

class IoTDBClient:
    def __init__(self, host: str = None, port: int = None, user: str = None, password: str = None, zone_id: str = None):
        self.host = host or settings.IOTDB_HOST
        self.port = port or settings.IOTDB_PORT
        self.user = user or settings.IOTDB_USER
        self.password = password or settings.IOTDB_PASSWORD
        self.zone_id = zone_id
        self.session = None

    async def connect(self):
        if Session is None:
            raise ImportError("IoTDB python client is not installed")

        def open_session():
            session = Session(self.host, self.port, self.user, self.password)
            session.open()
            return session

        last_exception = None
        for attempt in range(1, settings.IOTDB_CONNECT_RETRIES + 1):
            try:
                self.session = await asyncio.to_thread(open_session)
                if self.zone_id:
                    await asyncio.to_thread(self.session.set_time_zone, self.zone_id)
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

    async def query_range(self, device_path: str, measurements: List[str], start_ms: int, end_ms: int):
        if self.session is None:
            raise RuntimeError("IoTDB session is not connected")
        if not measurements:
            return

        columns = ", ".join(measurements)
        sql = f"select {columns} from {device_path} where time >= {start_ms} and time <= {end_ms}"
        dataset = await asyncio.to_thread(self.session.execute_query_statement, sql)
        column_types = dataset.get_column_types()
        try:
            while await asyncio.to_thread(dataset.has_next):
                row = await asyncio.to_thread(dataset.next)
                yield row, column_types
        finally:
            await asyncio.to_thread(dataset.close_operation_handle)

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

    async def query_timeseries(
        self,
        device: str = None,
        limit: int = 200,
        start_time: str = None,
    ) -> List[dict]:
        if Session is None:
            raise ImportError("IoTDB python client is not installed")
        if self.session is None:
            raise RuntimeError("IoTDB session is not connected")

        device_path = f"root.factory.{device}" if device else "root.factory.*"
        start_ms = self._parse_start_time(start_time)

        if device:
            sql = f"SELECT temperature, humidity, pressure FROM {device_path} WHERE time >= {start_ms} ORDER BY time DESC LIMIT {limit}"
        else:
            sql = f"SELECT * FROM root.factory.* WHERE time >= {start_ms} ORDER BY time DESC LIMIT {limit}"
        logger.debug("Executing IoTDB query: %s", sql)

        try:
            dataset = await asyncio.to_thread(self.session.execute_query_statement, sql)
            column_names = dataset.get_column_names()
            column_types = dataset.get_column_types()
            data = []
            while await asyncio.to_thread(dataset.has_next):
                row = await asyncio.to_thread(dataset.next)
                timestamp = row.get_timestamp()
                fields = row.get_fields()
                
                # Map column names to field values (column_names[0] is 'Time')
                row_dict = {}
                for i, field in enumerate(fields):
                    col_name = column_names[i + 1]
                    col_type = column_types[i + 1]
                    val = field.get_object_value(col_type)
                    # Convert potential numpy types (like float32) to native Python for JSON serialization
                    if val is not None and hasattr(val, "item"):
                        val = val.item()
                    row_dict[col_name] = val

                if device:
                    data.append({
                        "device_id": device,
                        "timestamp": timestamp,
                        "temperature": row_dict.get("temperature", row_dict.get(f"{device_path}.temperature")),
                        "humidity": row_dict.get("humidity", row_dict.get(f"{device_path}.humidity")),
                        "pressure": row_dict.get("pressure", row_dict.get(f"{device_path}.pressure")),
                    })
                else:
                    devices_in_row = {}
                    for col_name, value in row_dict.items():
                        parts = col_name.split(".")
                        if len(parts) >= 4 and parts[0] == "root" and parts[1] == "factory":
                            dev_id, measurement = parts[2], parts[3]
                            if dev_id not in devices_in_row:
                                devices_in_row[dev_id] = {"device_id": dev_id, "timestamp": timestamp}
                            devices_in_row[dev_id][measurement] = value
                    for dev_data in devices_in_row.values():
                        data.append(dev_data)
            # Sort by timestamp descending and take limit
            data.sort(key=lambda x: x["timestamp"], reverse=True)
            data = data[:limit]
            logger.debug("Retrieved %s rows from IoTDB", len(data))
            return data
        except Exception as e:
            logger.error("Error executing IoTDB query: %s", e)
            raise RuntimeError(f"IoTDB query failed: {e}")
        finally:
            if 'dataset' in locals():
                await asyncio.to_thread(dataset.close_operation_handle)

    @staticmethod
    def _parse_start_time(start_time: Optional[str]) -> int:
        if not start_time:
            return 0
        try:
            return int(start_time)
        except ValueError:
            text = start_time.strip()
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)