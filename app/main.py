import json
import logging
import time
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import jinja2

from app.auth import auth_router, get_current_user, get_current_user_from_token
from app.storage import buffer_store
from app.dashboard import router as dashboard_router
from app.iotdb_client import IoTDBClient
from app.models import SensorReading, User
from app.config import settings
from app.sensor_data import SensorDataRequest, fetch_and_merge_sensor_data
from app.sync import SyncManager

logging.basicConfig(level=logging.INFO)

templates = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "static")),
    autoescape=jinja2.select_autoescape(["html", "xml"]),
)

def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"

async def rate_limit(request: Request):
    ip = get_client_ip(request)
    if not hasattr(request.app.state, "rate_limits"):
        request.app.state.rate_limits = {}
    bucket = request.app.state.rate_limits.setdefault(ip, {"count": 0, "expires": time.time() + settings.SYNC_RATE_WINDOW})
    now = time.time()
    if now >= bucket["expires"]:
        bucket["count"] = 0
        bucket["expires"] = now + settings.SYNC_RATE_WINDOW
    if bucket["count"] >= settings.SYNC_RATE_LIMIT:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many sync requests")
    bucket["count"] += 1

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the buffer directory exists at startup
    if buffer_store.buffer_path:
        buffer_store.buffer_path.parent.mkdir(parents=True, exist_ok=True)

    app.state.iotdb_client = IoTDBClient()
    try:
        await app.state.iotdb_client.connect()
    except ImportError as exc:
        logging.warning("IoTDB client package not installed: %s", exc)
    except Exception as exc:
        logging.warning("Unable to connect to IoTDB at startup: %s", exc)

    # Shared buffer_store ensures consistency between ingestion and sync
    app.state.sync_manager = SyncManager(buffer_store=buffer_store, iotdb_client=app.state.iotdb_client)
    app.state.rate_limits = {}
    app.state.sync_task = asyncio.create_task(app.state.sync_manager.periodic_sync())
    yield
    app.state.sync_task.cancel()
    await app.state.sync_manager.close()
    await app.state.iotdb_client.close()

app = FastAPI(lifespan=lifespan)

# Import and include the IoTDB router after the app is defined
from app.iotdb_router import router as iotdb_router

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(iotdb_router)  # Include the IoTDB router here

@app.post("/ingest")
async def ingest_data(reading: SensorReading, user: User = Depends(get_current_user)):
    logging.info("Ingesting data for device %s", reading.device_id)
    await buffer_store.append_reading(reading)
    return {"status": "accepted"}

@app.get("/buffer/status")
async def get_buffer_status(user: User = Depends(get_current_user)):
    path = buffer_store.buffer_path
    exists = path.exists()
    size_kb = round(path.stat().st_size / 1024, 2) if exists else 0
    return {"exists": exists, "size_kb": size_kb, "filename": path.name}

@app.post("/sensor-data")
async def sensor_data(request: SensorDataRequest, user: User = Depends(get_current_user)):
    result = await fetch_and_merge_sensor_data(
        request.iotdbConfig.dict() if request.iotdbConfig else None,
        request.tsfilePaths,
        request.measurements,
        request.model_dump(exclude={"iotdbConfig", "tsfilePaths", "measurements"}),
    )
    if request.devicePath:
        result["metadata"]["devicePath"] = request.devicePath
    return result

@app.post("/sync")
async def sync_to_iotdb(request: Request, user: User = Depends(get_current_user), _: None = Depends(rate_limit)):
    job_id = await app.state.sync_manager.trigger_sync()
    return {"status": "queued", "job_id": job_id}

@app.get("/sync/status/{job_id}")
async def sync_status(request: Request, job_id: str, token: str = ""):
    get_current_user_from_token(token)

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break
            status_data = app.state.sync_manager.get_job_status(job_id)
            if status_data is None:
                yield "data: {}\n\n"
                break
            yield f"data: {json.dumps(status_data)}\n\n"
            if status_data.get("status") in {"completed", "failed"}:
                break
            await asyncio.sleep(1.0)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/")
async def read_root(request: Request):
    template = templates.get_template("index.html")
    return HTMLResponse(template.render(request=request))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)