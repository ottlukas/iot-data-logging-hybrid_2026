import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
import asyncio
import logging
from pathlib import Path
import jinja2

from app.models import SensorReading, User
from app.storage import append_to_tsfile, clear_tsfile
from app.sync import SyncManager
from app.dashboard import router as dashboard_router
from app.auth import get_user, oauth2_scheme, fake_users_db
from app.config import settings

templates = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "static")),
    autoescape=jinja2.select_autoescape(["html", "xml"]),
)

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.sync_manager = SyncManager()
    app.state.sync_task = asyncio.create_task(
        app.state.sync_manager.periodic_sync()
    )
    yield
    app.state.sync_task.cancel()
    await app.state.sync_manager.trigger_sync()
    await app.state.sync_manager.close()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(dashboard_router)

@app.post("/ingest")
async def ingest_data(reading: SensorReading, user: User = Depends(get_user)):
    await append_to_tsfile(reading)
    await app.state.sync_manager.add_to_batch(reading)
    return {"status": "accepted"}

@app.post("/sync")
async def sync_to_iotdb(user: User = Depends(get_user)):
    await app.state.sync_manager.trigger_sync()
    return {"status": "sync initiated"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/")
async def read_root(request: Request):
    template = templates.get_template("index.html")
    return HTMLResponse(template.render(request=request))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)