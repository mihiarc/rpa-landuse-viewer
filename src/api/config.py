import os
from typing import Dict, Any
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

class Config:
    ENV = os.getenv("ENV", "development")
    
    # Redis configurations for different environments
    REDIS_CONFIGS = {
        "development": {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": int(os.getenv("REDIS_DB", "0")),
            "decode_responses": True
        },
        "testing": {
            "host": os.getenv("TEST_REDIS_HOST", "localhost"),
            "port": int(os.getenv("TEST_REDIS_PORT", "6379")),
            "db": int(os.getenv("TEST_REDIS_DB", "1")),
            "decode_responses": True
        }
    }
    
    # SQLite database configurations for different environments
    DB_CONFIGS = {
        "development": {
            "database_path": os.getenv("DB_PATH", str(Path(__file__).parent.parent.parent / "data" / "database" / "rpa_landuse.db"))
        },
        "testing": {
            "database_path": os.getenv("TEST_DB_PATH", str(Path(__file__).parent.parent.parent / "data" / "database" / "test_rpa_landuse.db"))
        }
    }

    @classmethod
    def get_db_config(cls) -> Dict[str, Any]:
        """Get database configuration based on current environment."""
        return cls.DB_CONFIGS.get(cls.ENV, cls.DB_CONFIGS["development"])
        
    @classmethod
    def get_redis_config(cls) -> Dict[str, Any]:
        """Get Redis configuration based on current environment."""
        return cls.REDIS_CONFIGS.get(cls.ENV, cls.REDIS_CONFIGS["development"]) 