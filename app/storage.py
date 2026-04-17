import json
import aiofiles
from pathlib import Path
from app.models import SensorReading
from app.config import settings

async def append_to_tsfile(data: SensorReading):
    path = Path(settings.LOCAL_TSFILE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(settings.LOCAL_TSFILE_PATH, mode="a") as f:
        await f.write(json.dumps(data.model_dump()) + "\n")

async def read_recent_tsfile(limit: int = 100):
    data = []
    path = Path(settings.LOCAL_TSFILE_PATH)
    if not path.exists():
        return data
    try:
        async with aiofiles.open(settings.LOCAL_TSFILE_PATH, mode="r") as f:
            lines = await f.readlines()
            for line in lines[-limit:]:
                if line.strip():
                    data.append(SensorReading.model_validate_json(line))
    except Exception as e:
        import logging
        logging.error(f"Error reading tsfile: {e}")
    return data

async def clear_tsfile():
    async with aiofiles.open(settings.LOCAL_TSFILE_PATH, mode="w") as f:
        await f.write("")