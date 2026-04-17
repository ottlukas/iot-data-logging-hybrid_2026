from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from app.storage import read_recent_tsfile
from pathlib import Path
import jinja2

router = APIRouter()
templates = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(Path(__file__).resolve().parents[0] / "static")),
    autoescape=jinja2.select_autoescape(["html", "xml"]),
)

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    template = templates.get_template("index.html")
    return HTMLResponse(template.render(request=request))

@router.get("/data")
async def get_data():
    data = await read_recent_tsfile(100)
    return {"data": [d.model_dump() for d in data]}