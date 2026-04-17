import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    LOCAL_TSFILE_PATH: str = os.getenv("LOCAL_TSFILE_PATH", "data.tsfile")
    IOTDB_HOST: str = os.getenv("IOTDB_HOST", "localhost")
    IOTDB_PORT: int = int(os.getenv("IOTDB_PORT", 6667))
    IOTDB_USER: str = os.getenv("IOTDB_USER", "root")
    IOTDB_PASSWORD: str = os.getenv("IOTDB_PASSWORD", "root")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 100))
    SYNC_INTERVAL: int = int(os.getenv("SYNC_INTERVAL", 60))

settings = Settings()