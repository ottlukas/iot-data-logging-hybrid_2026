import sys
import logging
from pathlib import Path
from app.config import settings

logger = logging.getLogger(__name__)

def log_platform_info():
    """
    Logs current platform information and resolved paths to aid cross-platform debugging.
    """
    platform_name = sys.platform
    logger.info(f"Detected Platform: {platform_name}")
    
    # Resolve paths to log absolute locations
    try:
        resolved_tsfile = Path(settings.LOCAL_TSFILE_PATH).resolve()
        resolved_archive = Path(settings.LOCAL_ARCHIVE_DIR).resolve()
        resolved_index = Path(settings.LOCAL_INDEX_FILE).resolve()
        
        logger.info(f"Resolved LOCAL_TSFILE_PATH: {resolved_tsfile}")
        logger.info(f"Resolved LOCAL_ARCHIVE_DIR: {resolved_archive}")
        logger.info(f"Resolved LOCAL_INDEX_FILE: {resolved_index}")
        
        return {
            "platform": platform_name,
            "resolved_tsfile": resolved_tsfile,
            "resolved_archive": resolved_archive,
            "resolved_index": resolved_index
        }
    except Exception as e:
        logger.error(f"Error resolving paths in platform_utils: {e}")
        return {
            "platform": platform_name,
            "error": str(e)
        }
