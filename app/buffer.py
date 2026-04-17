import json
import os
import shutil
import asyncio
import aiofiles
from pathlib import Path
from typing import AsyncIterator, Dict, List
from app.models import SensorReading
from app.config import settings

class BufferStore:
    def __init__(self):
        self.buffer_path = Path(settings.LOCAL_TSFILE_PATH)
        self.buffer_dir = self.buffer_path.parent
        self.archive_dir = Path(settings.LOCAL_ARCHIVE_DIR)
        self.index_path = Path(settings.LOCAL_INDEX_FILE)

    async def ensure_directories(self):
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        if not self.index_path.exists():
            await self._write_index({})

    async def _read_index(self) -> Dict[str, int]:
        if not self.index_path.exists():
            return {}
        async with aiofiles.open(self.index_path, mode="r") as f:
            content = await f.read()
            if not content.strip():
                return {}
            return json.loads(content)

    async def _write_index(self, index: Dict[str, int]):
        async with aiofiles.open(self.index_path, mode="w") as f:
            await f.write(json.dumps(index))

    async def append_reading(self, reading: SensorReading):
        await self.ensure_directories()
        async with aiofiles.open(self.buffer_path, mode="a") as f:
            await f.write(reading.model_dump_json() + "\n")

    async def read_recent(self, limit: int = 100) -> List[SensorReading]:
        if not self.buffer_path.exists():
            return []
        lines = []
        async with aiofiles.open(self.buffer_path, mode="r") as f:
            async for line in f:
                if line.strip():
                    lines.append(line)
        lines = [SensorReading.model_validate_json(line) for line in lines[-limit:]]
        return lines

    async def list_buffer_files(self) -> List[Path]:
        await self.ensure_directories()
        return [self.buffer_path]

    async def count_unprocessed_lines(self, path: Path, start_line: int) -> int:
        if not path.exists():
            return 0
        total = 0
        async with aiofiles.open(path, mode="r") as f:
            async for _ in f:
                total += 1
        return max(0, total - start_line)

    async def read_batches(self, path: Path, start_line: int, batch_size: int) -> AsyncIterator[List[SensorReading]]:
        async with aiofiles.open(path, mode="r") as f:
            current_line = 0
            batch: List[SensorReading] = []
            async for raw in f:
                if not raw.strip():
                    current_line += 1
                    continue
                if current_line < start_line:
                    current_line += 1
                    continue
                batch.append(SensorReading.model_validate_json(raw))
                current_line += 1
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    async def archive_file(self, path: Path):
        await self.ensure_directories()
        if not path.exists():
            return
        archive_name = f"{path.stem}-{int(path.stat().st_mtime)}{path.suffix}"
        destination = self.archive_dir / archive_name
        try:
            await asyncio.to_thread(os.rename, path, destination)
        except OSError:
            await asyncio.to_thread(shutil.move, path, destination)
        index = await self._read_index()
        index.pop(path.name, None)
        await self._write_index(index)

    async def update_index(self, path: Path, lines_processed: int):
        index = await self._read_index()
        index[path.name] = lines_processed
        await self._write_index(index)

    async def get_index(self) -> Dict[str, int]:
        return await self._read_index()

    async def total_unprocessed(self) -> int:
        files = await self.list_buffer_files()
        index = await self.get_index()
        count = 0
        for path in files:
            start_line = index.get(path.name, 0)
            count += await self.count_unprocessed_lines(path, start_line)
        return count

    async def clear_buffer(self):
        await self.ensure_directories()
        async with aiofiles.open(self.buffer_path, mode="w") as f:
            await f.write("")
        await self._write_index({})
