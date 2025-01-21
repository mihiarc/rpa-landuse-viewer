import os
from typing import Dict, Any
from dotenv import load_dotenv

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
    
    # Database configurations for different environments
    DB_CONFIGS = {
        "development": {
            "host": os.getenv("DB_HOST", "localhost"),
            "user": os.getenv("DB_USER", "mihiarc"),
            "password": os.getenv("DB_PASSWORD", "survista683"),
            "database": os.getenv("DB_NAME", "rpa_mysql_db"),
            "pool_name": "mypool",
            "pool_size": 5
        },
        "testing": {
            "host": os.getenv("TEST_DB_HOST", "localhost"),
            "user": os.getenv("TEST_DB_USER", "test_user"),
            "password": os.getenv("TEST_DB_PASSWORD", "test_password"),
            "database": os.getenv("TEST_DB_NAME", "test_rpa_db"),
            "pool_name": "test_pool",
            "pool_size": 2
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