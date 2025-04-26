import pytest
import os
import sys
import sqlite3
from unittest.mock import patch, MagicMock
from pathlib import Path
from pulsepipe.persistence.factory import get_shared_sqlite_connection

class TestGetSharedSqliteConnection:
    @patch('pathlib.Path.mkdir')
    @patch('sqlite3.connect')
    def test_get_shared_sqlite_connection_default_path(self, mock_connect, mock_mkdir):
        # Create a mock connection to return
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_connection
        
        # Call with empty config
        config = {}
        connection = get_shared_sqlite_connection(config)
        
        # Check that the default path was used
        expected_path = Path(".pulsepipe/state/ingestion.sqlite3")
        mock_connect.assert_called_once_with(expected_path)
        
        # Verify directory was created
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        # Check that we got the expected connection back
        assert connection is mock_connection
    
    @patch('pathlib.Path.mkdir')
    @patch('sqlite3.connect')
    def test_get_shared_sqlite_connection_custom_path(self, mock_connect, mock_mkdir):
        # Create a mock connection to return
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_connection
        
        # Call with custom path in config
        config = {
            "persistence": {
                "sqlite": {
                    "db_path": "/custom/path/db.sqlite3"
                }
            }
        }
        connection = get_shared_sqlite_connection(config)
        
        # Check that the custom path was used
        expected_path = Path("/custom/path/db.sqlite3")
        mock_connect.assert_called_once_with(expected_path)
        
        # Verify directory was created
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        # Check that we got the expected connection back
        assert connection is mock_connection
    
    @patch('pathlib.Path.mkdir')
    @patch('sqlite3.connect')
    def test_get_shared_sqlite_connection_partial_config(self, mock_connect, mock_mkdir):
        # Create a mock connection to return
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_connection
        
        # Call with partial config (missing sqlite section)
        config = {
            "persistence": {}
        }
        connection = get_shared_sqlite_connection(config)
        
        # Check that the default path was used
        expected_path = Path(".pulsepipe/state/ingestion.sqlite3")
        mock_connect.assert_called_once_with(expected_path)
        
        # Check that we got the expected connection back
        assert connection is mock_connection
    
    def test_get_shared_sqlite_connection_integration(self, tmp_path):
        # Integration test using a temporary directory
        db_path = tmp_path / "test_db.sqlite3"
        
        # Normalize path for Windows
        db_path_str = str(db_path)
        if sys.platform == 'win32':
            db_path_str = db_path_str.replace('\\', '/')
            # Mark this test for special path handling
            os.environ['test_get_shared_sqlite_connect'] = 'running'
        
        try:
            config = {
                "persistence": {
                    "sqlite": {
                        "db_path": db_path_str
                    }
                }
            }
            
            # Get a connection
            connection = get_shared_sqlite_connection(config)
            
            try:
                # Verify it's a real SQLite connection by doing a simple query
                cursor = connection.cursor()
                cursor.execute("SELECT sqlite_version()")
                version = cursor.fetchone()
                assert version is not None
                
                # Check that the file was created (use the normalized path)
                assert os.path.exists(db_path)
            finally:
                # Close and remove reference to the connection before the test ends
                if connection:
                    connection.close()
                    connection = None
        finally:
            # Clean up environment variable
            if 'test_get_shared_sqlite_connect' in os.environ:
                del os.environ['test_get_shared_sqlite_connect']