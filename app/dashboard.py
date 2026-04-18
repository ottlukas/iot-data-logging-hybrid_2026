from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse
from app.storage import read_recent_tsfile, buffer_store
from app.auth import get_current_user
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
async def get_data(user=Depends(get_current_user)):
    data = await read_recent_tsfile(100)
    return {
        "exists": buffer_store.buffer_path.exists(),
        "is_empty": len(data) == 0,
        "data": [d.model_dump() for d in data]
    }