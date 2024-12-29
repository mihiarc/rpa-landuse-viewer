from typing import Dict, Any
import mysql.connector
from mysql.connector import pooling
from .config import Config

class DatabaseConnection:
    _pool = None

    @classmethod
    def get_connection_pool(cls):
        if cls._pool is None:
            try:
                cls._pool = mysql.connector.pooling.MySQLConnectionPool(**Config.get_db_config())
            except mysql.connector.Error as err:
                raise Exception(f"Failed to create connection pool: {err}")
        return cls._pool

    @classmethod
    def get_connection(cls):
        try:
            return cls.get_connection_pool().get_connection()
        except mysql.connector.Error as err:
            raise Exception(f"Failed to get connection from pool: {err}")

    @staticmethod
    def close_connection(connection):
        if connection:
            connection.close()

    @classmethod
    def reset_pool(cls):
        """Reset the connection pool - useful for testing"""
        cls._pool = None 