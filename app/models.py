from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class SensorReading(BaseModel):
    device_id: str
    timestamp: datetime
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    pressure: Optional[float] = None
    electronic_signature: str = "system_admin"

class User(BaseModel):
    username: str
    password: str
    role: str  # "operator", "supervisor", "admin"