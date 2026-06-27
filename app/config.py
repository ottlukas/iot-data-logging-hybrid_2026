import os
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    LOCAL_TSFILE_PATH: str = os.getenv("LOCAL_TSFILE_PATH", str(Path("data") / "tsfiles" / "buffer_current.tsfile"))
    LOCAL_ARCHIVE_DIR: str = os.getenv("LOCAL_ARCHIVE_DIR", str(Path("data") / "tsfiles" / "archive"))
    LOCAL_INDEX_FILE: str = os.getenv("LOCAL_INDEX_FILE", str(Path("data") / "tsfiles" / "index.json"))
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

    def __init__(self, **values):
        super().__init__(**values)
        # Normalize environment or default paths to be operating-system-agnostic
        self.LOCAL_TSFILE_PATH = str(Path(self.LOCAL_TSFILE_PATH))
        self.LOCAL_ARCHIVE_DIR = str(Path(self.LOCAL_ARCHIVE_DIR))
        self.LOCAL_INDEX_FILE = str(Path(self.LOCAL_INDEX_FILE))

    @property
    def LOCAL_TSFILE_DIR(self):
        return Path(self.LOCAL_TSFILE_PATH).parent

settings = Settings()