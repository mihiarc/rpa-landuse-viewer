import pytest
import sys
from pathlib import Path

# Add the src directory to the path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))
from src.db.database import DatabaseConnection


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Setup database connection for tests and teardown after tests."""
    # Setup phase - nothing special needed as DatabaseConnection handles this
    print("Setting up database connection for tests")
    
    # The fixture yields control to the tests
    yield
    
    # Teardown phase - no explicit cleanup needed since we close connections after each use
    print("Tearing down database connection")
    # DatabaseConnection handles its own cleanup


@pytest.fixture(scope="session")
def db_connection():
    """Return a database connection for tests."""
    conn = DatabaseConnection.get_connection()
    yield conn
    # Close the connection when the session ends
    DatabaseConnection.close_connection(conn)


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """Return a database cursor for tests."""
    cursor = db_connection.cursor()
    yield cursor
    # No need to close the cursor as it's associated with the connection
    # that will be closed by db_connection fixture 