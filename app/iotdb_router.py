from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Optional
from app.auth import get_current_user
from app.models import User

router = APIRouter()

@router.get("/iotdb/data")
async def get_iotdb_data(
    request: Request,
    device: Optional[str] = Query(None, description="Filter by device ID"),
    limit: int = Query(200, description="Maximum number of records to return"),
    start_time: Optional[str] = Query(None, description="Start time in ISO format or milliseconds"),
    user: User = Depends(get_current_user),
):
    try:
        client = request.app.state.iotdb_client
        if client.session is None:
            await client.connect()
        data = await client.query_timeseries(device, limit, start_time)
        return {"data": data}
    except ImportError:
        raise HTTPException(status_code=501, detail="IoTDB client not installed")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"IoTDB error: {str(e)}")