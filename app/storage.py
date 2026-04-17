from app.buffer import BufferStore
from app.models import SensorReading

buffer_store = BufferStore()

async def append_to_tsfile(data: SensorReading):
    return await buffer_store.append_reading(data)

async def read_recent_tsfile(limit: int = 100):
    return await buffer_store.read_recent(limit)

async def clear_tsfile():
    return await buffer_store.clear_buffer()
