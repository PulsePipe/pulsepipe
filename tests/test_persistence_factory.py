import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from pathlib import Path
from pulsepipe.persistence.factory import get_database_connection, get_sql_dialect, get_tracking_repository
from pulsepipe.persistence.database import ConfigurationError
from pulsepipe.persistence.database.sqlite_impl import SQLiteConnection, SQLiteDialect
from pulsepipe.persistence.database.postgresql_impl import PostgreSQLConnection, PostgreSQLDialect
from pulsepipe.persistence.database.mongodb_impl import MongoDBConnection, MongoDBAdapter

class TestPersistenceFactory:
    
    @patch('pulsepipe.persistence.factory.SQLiteConnection')
    def test_get_database_connection_sqlite_default(self, mock_sqlite):
        """Test SQLite connection with default configuration"""
        config = {}
        
        mock_instance = MagicMock()
        mock_sqlite.return_value = mock_instance
        
        connection = get_database_connection(config)
        
        mock_sqlite.assert_called_once_with(
            db_path=".pulsepipe/state/ingestion.sqlite3",
            timeout=30.0
        )
        assert connection is mock_instance
    
    @patch('pulsepipe.persistence.factory.SQLiteConnection')
    def test_get_database_connection_sqlite_custom(self, mock_sqlite):
        """Test SQLite connection with custom configuration"""
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": "custom_db.sqlite3",
                    "timeout": 60.0
                }
            }
        }
        
        mock_instance = MagicMock()
        mock_sqlite.return_value = mock_instance
        
        connection = get_database_connection(config)
        
        mock_sqlite.assert_called_once_with(
            db_path="custom_db.sqlite3",
            timeout=60.0
        )
        assert connection is mock_instance
    
    @patch('pulsepipe.persistence.factory.PostgreSQLConnection')
    def test_get_database_connection_postgresql_valid(self, mock_postgresql):
        """Test PostgreSQL connection with valid configuration"""
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "testdb",
                    "username": "user",
                    "password": "pass",
                    "pool_size": 10,
                    "max_overflow": 20
                }
            }
        }
        
        mock_instance = MagicMock()
        mock_postgresql.return_value = mock_instance
        
        connection = get_database_connection(config)
        
        # Should create a PostgreSQL connection
        mock_postgresql.assert_called_once_with(
            host="localhost",
            port=5432,
            database="testdb",
            username="user",
            password="pass",
            pool_size=10,
            max_overflow=20
        )
        assert connection is mock_instance
    
    def test_get_database_connection_postgresql_missing_fields(self):
        """Test PostgreSQL connection with missing required fields"""
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "localhost",
                    "port": 5432
                    # Missing database, username, password
                }
            }
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            get_database_connection(config)
        
        assert "missing required fields" in str(exc_info.value)
        assert "database" in str(exc_info.value)
        assert "username" in str(exc_info.value)
        assert "password" in str(exc_info.value)
    
    @patch('pulsepipe.persistence.factory.MongoDBConnection')
    def test_get_database_connection_mongodb_valid(self, mock_mongodb):
        """Test MongoDB connection with valid configuration"""
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "connection_string": "mongodb://localhost:27017",
                    "database": "testdb",
                    "collection_prefix": "test_",
                    "username": "user",
                    "password": "pass",
                    "replica_set": "rs0",
                    "read_preference": "primary"
                }
            }
        }
        
        mock_instance = MagicMock()
        mock_mongodb.return_value = mock_instance
        
        connection = get_database_connection(config)
        
        # Should create a MongoDB connection
        mock_mongodb.assert_called_once_with(
            connection_string="mongodb://localhost:27017",
            database="testdb",
            collection_prefix="test_",
            username="user",
            password="pass",
            replica_set="rs0",
            read_preference="primary"
        )
        assert connection is mock_instance
    
    def test_get_database_connection_mongodb_missing_fields(self):
        """Test MongoDB connection with missing required fields"""
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "connection_string": "mongodb://localhost:27017"
                    # Missing database
                }
            }
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            get_database_connection(config)
        
        assert "requires 'connection_string' and 'database'" in str(exc_info.value)
    
    def test_get_database_connection_unsupported_type(self):
        """Test unsupported database type"""
        config = {
            "persistence": {
                "type": "unsupported"
            }
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            get_database_connection(config)
        
        assert "Unsupported database type: unsupported" in str(exc_info.value)
    
    def test_get_sql_dialect_sqlite(self):
        """Test SQLite dialect creation"""
        config = {"persistence": {"type": "sqlite"}}
        
        dialect = get_sql_dialect(config)
        
        assert isinstance(dialect, SQLiteDialect)
    
    def test_get_sql_dialect_postgresql(self):
        """Test PostgreSQL dialect creation"""
        config = {"persistence": {"type": "postgresql"}}
        
        dialect = get_sql_dialect(config)
        
        assert isinstance(dialect, PostgreSQLDialect)
    
    def test_get_sql_dialect_mongodb(self):
        """Test MongoDB adapter creation"""
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "collection_prefix": "test_"
                }
            }
        }
        
        dialect = get_sql_dialect(config)
        
        assert isinstance(dialect, MongoDBAdapter)
    
    def test_get_sql_dialect_default(self):
        """Test default dialect (SQLite)"""
        config = {}
        
        dialect = get_sql_dialect(config)
        
        assert isinstance(dialect, SQLiteDialect)
    
    def test_get_tracking_repository_with_connection(self):
        """Test tracking repository with provided connection"""
        config = {"persistence": {"type": "sqlite"}}
        mock_connection = MagicMock()
        
        repository = get_tracking_repository(config, connection=mock_connection)
        
        # Should create a TrackingRepository with the provided connection
        assert repository is not None
    
    @patch('pulsepipe.persistence.factory.SQLiteConnection')
    def test_get_tracking_repository_without_connection(self, mock_sqlite):
        """Test tracking repository without provided connection"""
        config = {"persistence": {"type": "sqlite"}}
        
        mock_connection = MagicMock()
        mock_sqlite.return_value = mock_connection
        
        repository = get_tracking_repository(config)
        
        # Should create a TrackingRepository with a new connection
        assert repository is not None
        mock_sqlite.assert_called_once()