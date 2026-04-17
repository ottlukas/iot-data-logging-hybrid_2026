import json
import aiofiles
from pathlib import Path
from app.models import SensorReading
from app.config import settings

async def append_to_tsfile(data: SensorReading):
    async with aiofiles.open(settings.LOCAL_TSFILE_PATH, mode="a") as f:
        await f.write(json.dumps(data.model_dump()) + "\n")

async def read_recent_tsfile(limit: int = 100):
    data = []
    async with aiofiles.open(settings.LOCAL_TSFILE_PATH, mode="r") as f:
        lines = await f.readlines()
        for line in lines[-limit:]:
            data.append(SensorReading.model_validate_json(line))
    return data

async def clear_tsfile():
    async with aiofiles.open(settings.LOCAL_TSFILE_PATH, mode="w") as f:
        await f.write("")