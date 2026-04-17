import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    LOCAL_TSFILE_PATH: str = os.getenv("LOCAL_TSFILE_PATH", "data/tsfiles/buffer_current.tsfile")
    LOCAL_ARCHIVE_DIR: str = os.getenv("LOCAL_ARCHIVE_DIR", "data/tsfiles/archive")
    LOCAL_INDEX_FILE: str = os.getenv("LOCAL_INDEX_FILE", "data/tsfiles/index.json")
    IOTDB_HOST: str = os.getenv("IOTDB_HOST", "localhost")
    IOTDB_PORT: int = int(os.getenv("IOTDB_PORT", 6667))
    IOTDB_USER: str = os.getenv("IOTDB_USER", "root")
    IOTDB_PASSWORD: str = os.getenv("IOTDB_PASSWORD", "root")
    IOTDB_CONNECT_RETRIES: int = int(os.getenv("IOTDB_CONNECT_RETRIES", 6))
    IOTDB_CONNECT_BACKOFF_SECONDS: float = float(os.getenv("IOTDB_CONNECT_BACKOFF_SECONDS", 2.0))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 20))
    SYNC_INTERVAL: int = int(os.getenv("SYNC_INTERVAL", 30))
    SYNC_RATE_LIMIT: int = int(os.getenv("SYNC_RATE_LIMIT", 3))
    SYNC_RATE_WINDOW: int = int(os.getenv("SYNC_RATE_WINDOW", 60))
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "change-me-super-secret")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

    @property
    def LOCAL_TSFILE_DIR(self):
        from pathlib import Path
        return Path(self.LOCAL_TSFILE_PATH).parent

settings = Settings()