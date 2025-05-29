# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Chunk, Embed. Healthcare Data, AI-Ready with RAG.
# https://github.com/PulsePipe/pulsepipe
#
# Copyright (C) 2025 Amir Abrams
#
# This file is part of PulsePipe and is licensed under the GNU Affero General 
# Public License v3.0 (AGPL-3.0). A full copy of this license can be found in 
# the LICENSE file at the root of this repository or online at:
# https://www.gnu.org/licenses/agpl-3.0.html
#
# PulsePipe is distributed WITHOUT ANY WARRANTY; without even the implied 
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# We welcome community contributions — if you make it better, 
# share it back. The whole healthcare ecosystem wins.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

# tests/test_database_factory.py

"""
Unit tests for database factory functions.

Tests the factory functions that create database connections and dialects.
"""

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock

from pulsepipe.persistence.factory import (
    get_database_connection,
    get_sql_dialect,
    get_tracking_repository
)
from pulsepipe.persistence.database.sqlite_impl import SQLiteConnection, SQLiteDialect
from pulsepipe.persistence.database.postgresql_impl import PostgreSQLConnection, PostgreSQLDialect
from pulsepipe.persistence.database.mongodb_impl import MongoDBConnection, MongoDBAdapter
from pulsepipe.persistence.database.exceptions import ConfigurationError
from pulsepipe.persistence.tracking_repository import TrackingRepository


class TestGetDatabaseConnection:
    """Test get_database_connection function."""
    
    def test_sqlite_connection_default(self):
        """Test creating SQLite connection with default configuration."""
        config = {}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Patch the default path to use temp directory
            with patch('pulsepipe.persistence.database.sqlite_impl.Path') as mock_path:
                mock_path_instance = MagicMock()
                mock_path_instance.parent.mkdir = MagicMock()
                mock_path.return_value = mock_path_instance
                
                with patch('sqlite3.connect') as mock_connect:
                    mock_connect.return_value = MagicMock()
                    
                    connection = get_database_connection(config)
                    
                    assert isinstance(connection, SQLiteConnection)
                    assert connection.db_path == ".pulsepipe/state/ingestion.sqlite3"
                    assert connection.timeout == 30.0
                    
                    connection.close()
    
    def test_sqlite_connection_custom_config(self):
        """Test creating SQLite connection with custom configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "custom.db")
            
            config = {
                "persistence": {
                    "type": "sqlite",
                    "sqlite": {
                        "db_path": db_path,
                        "timeout": 60.0
                    }
                }
            }
            
            connection = get_database_connection(config)
            
            assert isinstance(connection, SQLiteConnection)
            assert connection.db_path == db_path
            assert connection.timeout == 60.0
            
            connection.close()
    
    def test_postgresql_connection_missing_required_fields(self):
        """Test PostgreSQL connection with missing required fields."""
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
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_mongodb_connection_valid_config(self, mock_mongo_client):
        """Test creating MongoDB connection with valid configuration."""
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "connection_string": "mongodb://localhost:27017/",
                    "database": "testdb",
                    "collection_prefix": "test_"
                }
            }
        }
        
        connection = get_database_connection(config)
        
        assert isinstance(connection, MongoDBConnection)
        assert connection.connection_string == "mongodb://localhost:27017/"
        assert connection.database_name == "testdb"
        assert connection.collection_prefix == "test_"
        
        connection.close()
    
    def test_unsupported_database_type(self):
        """Test unsupported database type raises error."""
        config = {
            "persistence": {
                "type": "unsupported_db"
            }
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            get_database_connection(config)
        
        assert "Unsupported database type: unsupported_db" in str(exc_info.value)


class TestGetSQLDialect:
    """Test get_sql_dialect function."""
    
    def test_sqlite_dialect_default(self):
        """Test creating SQLite dialect with default configuration."""
        config = {}
        
        dialect = get_sql_dialect(config)
        
        assert isinstance(dialect, SQLiteDialect)
    
    def test_postgresql_dialect(self):
        """Test creating PostgreSQL dialect."""
        config = {
            "persistence": {
                "type": "postgresql"
            }
        }
        
        dialect = get_sql_dialect(config)
        
        assert isinstance(dialect, PostgreSQLDialect)
    
    def test_mongodb_adapter_default_prefix(self):
        """Test creating MongoDB adapter with default prefix."""
        config = {
            "persistence": {
                "type": "mongodb"
            }
        }
        
        adapter = get_sql_dialect(config)
        
        assert isinstance(adapter, MongoDBAdapter)
        assert adapter.collection_prefix == "audit_"


class TestGetTrackingRepository:
    """Test get_tracking_repository function."""
    
    @patch('pulsepipe.persistence.factory.get_database_connection')
    @patch('pulsepipe.persistence.factory.get_sql_dialect')
    def test_create_repository_no_connection(self, mock_get_dialect, mock_get_connection):
        """Test creating repository without existing connection."""
        mock_connection = MagicMock()
        mock_dialect = MagicMock()
        mock_get_connection.return_value = mock_connection
        mock_get_dialect.return_value = mock_dialect
        
        config = {"persistence": {"type": "sqlite"}}
        
        repository = get_tracking_repository(config)
        
        assert isinstance(repository, TrackingRepository)
        assert repository.conn is mock_connection
        assert repository.dialect is mock_dialect