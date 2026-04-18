import asyncio
import json
import logging
import aiofiles
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Dict, AsyncIterator

try:
    from tsfile import TSFileWriter, TSFileReader, Tablet, TSDataType
except ImportError:
    TSFileReader = None
    TSFileWriter = None
    TSDataType = None

from app.config import settings
from app.models import SensorReading

logger = logging.getLogger(__name__)

TSFILE_INSTALL_URL = "https://tsfile.apache.org/UserGuide/latest/QuickStart/QuickStart-PYTHON.html"

class BufferStore:
    def __init__(self):
        self.buffer_path = Path(settings.LOCAL_TSFILE_PATH)
        self.device_id = "root.factory.line1"
        self.measurements = ["temperature", "humidity", "pressure"]
        self._lock = asyncio.Lock()

    async def append_reading(self, reading: SensorReading):
        """
        Appends a reading using the Apache TSFile specification.
        """
        if not self.buffer_path.exists():
            logger.info(f"Creating new TSFile buffer at {self.buffer_path.absolute()}")

        if TSFileWriter is None:
            logger.warning(f"tsfile package not installed. Falling back to JSON appending at {self.buffer_path.name}. See {TSFILE_INSTALL_URL}")
            async with self._lock:
                async with aiofiles.open(self.buffer_path, mode="a", encoding="utf-8") as f:
                    await f.write(reading.model_dump_json() + "\n")
            return

        logger.info(f"Appending reading for {reading.device_id} to {self.buffer_path.name}")

        async with self._lock:
            # Note: TSFileWriter does not support incremental appending to a sealed file.
            # In a production scenario, one would buffer points in memory or use a WAL.
            # Here we follow the specification to write data into the TSFile format.
            await asyncio.to_thread(self._write_to_tsfile, reading)

    def _write_to_tsfile(self, reading: SensorReading):
        # Ensure parent directory exists
        self.buffer_path.parent.mkdir(parents=True, exist_ok=True)

        # Create a Tablet for the reading
        timestamp = int(reading.timestamp.timestamp() * 1000)
        data_types = [TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT]
        
        # Values are expected as a list of lists (one list per measurement)
        values = [
            [float(reading.temperature)] if reading.temperature is not None else [0.0],
            [float(reading.humidity)] if reading.humidity is not None else [0.0],
            [float(reading.pressure)] if reading.pressure is not None else [0.0],
        ]

        tablet = Tablet(
            self.device_id,
            self.measurements,
            data_types,
            values,
            [timestamp]
        )

        # Use TSFileWriter to write the binary file
        # Note: TSFile format is designed for bulk writes. Writing single points 
        # by re-initializing the writer is a demonstration of the file format logic.
        writer = TSFileWriter(str(self.buffer_path))
        try:
            writer.write_tablet(tablet)
            logger.info(f"Successfully wrote data to TSFile: {self.buffer_path}")
        finally:
            writer.close()

    async def read_recent(self, limit: int = 100) -> List[SensorReading]:
        if not self.buffer_path.exists():
            logger.info(f"Buffer file not found at {self.buffer_path.absolute()}")
            return []

        if TSFileReader is None:
            logger.warning(f"TSFileReader not available. Reading buffer as line-delimited JSON from {self.buffer_path.name}")
            readings = []
            async with aiofiles.open(self.buffer_path, mode="r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        readings.append(SensorReading.model_validate_json(line))
                    except Exception as e:
                        logger.debug(f"Skipping invalid JSON line in buffer: {e}")
                        continue
            return readings[-limit:]

        logger.info(f"Reading recent points from {self.buffer_path.name}")
        return await asyncio.to_thread(self._read_from_tsfile, self.buffer_path)

    def _read_from_tsfile(self, path: Path) -> List[SensorReading]:
        reader = TSFileReader(str(path))
        readings_map = {}
        try:
            for m in self.measurements:
                # Query entire range for current buffer
                query_res = reader.query(self.device_id, m, 0, 2**63 - 1)
                while query_res.has_next():
                    row = query_res.next()
                    ts = row.get_timestamp()
                    val = row.get_value()
                    if ts not in readings_map:
                        readings_map[ts] = {
                            "timestamp": datetime.fromtimestamp(ts / 1000, timezone.utc),
                            "device_id": self.device_id.split(".")[-1],
                            "electronic_signature": "buffered"
                        }
                    readings_map[ts][m] = val
                query_res.close()
            
            # Sort by timestamp and return as SensorReading models
            sorted_data = [readings_map[t] for t in sorted(readings_map.keys())]
            return [SensorReading(**r) for r in sorted_data]
        except Exception as e:
            logger.error(f"Failed to read TSFile {path}: {e}")
            return []
        finally:
            reader.close()

    async def get_index(self) -> Dict[str, int]:
        index_path = Path(settings.LOCAL_INDEX_FILE)
        if not index_path.exists():
            return {}
        async with aiofiles.open(index_path, mode="r") as f:
            content = await f.read()
            return json.loads(content) if content else {}

    async def update_index(self, path: Path, offset: int):
        index_path = Path(settings.LOCAL_INDEX_FILE)
        index = await self.get_index()
        index[path.name] = offset
        index_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(index_path, mode="w") as f:
            await f.write(json.dumps(index))

    async def total_unprocessed(self) -> int:
        if not self.buffer_path.exists():
            return 0
        index = await self.get_index()
        offset = index.get(self.buffer_path.name, 0)
        return await self.count_unprocessed_lines(self.buffer_path, offset)

    async def count_unprocessed_lines(self, path: Path, offset: int) -> int:
        # In the TSFile context, we treat the file as 1 unprocessed unit 
        # because TSFileWriter overwrites.
        return 1 if path.exists() and offset == 0 else 0

    async def read_batches(self, path: Path, offset: int, batch_size: int) -> AsyncIterator[List[SensorReading]]:
        if offset == 0:
            readings = await self.read_recent()
            if readings:
                logger.info(f"Yielding batch of {len(readings)} readings for sync")
                yield readings

    async def clear_buffer(self):
        index_path = Path(settings.LOCAL_INDEX_FILE)
        if index_path.exists():
            index_path.unlink()
        if self.buffer_path.exists():
            self.buffer_path.unlink()

    async def archive_file(self, path: Path):
        """
        Archive the TSFile once it is synchronized.
        """
        archive_dir = Path(settings.LOCAL_ARCHIVE_DIR)
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = archive_dir / f"{path.stem}_{timestamp}{path.suffix}"
        
        await asyncio.to_thread(shutil.move, str(path), str(dest))
        logger.info("Archived Apache TSFile to %s", dest)