from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.storage import read_recent_tsfile
from pathlib import Path

router = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "static")

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@router.get("/data")
async def get_data():
    data = await read_recent_tsfile(100)
    return {"data": [d.model_dump() for d in data]}