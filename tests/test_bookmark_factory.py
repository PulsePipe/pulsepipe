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
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

import pytest
from unittest.mock import patch, MagicMock

from pulsepipe.adapters.file_watcher_bookmarks.factory import create_bookmark_store
from pulsepipe.adapters.file_watcher_bookmarks.common_store import CommonBookmarkStore
from pulsepipe.persistence.database.exceptions import ConfigurationError

class TestBookmarkStoreFactory:
    def test_create_bookmark_store_sqlite_default(self):
        # Test with default sqlite config (minimal)
        config = {"type": "sqlite"}
        
        bookmark_store = create_bookmark_store(config)
        
        # Factory now returns CommonBookmarkStore using SQLite backend
        assert isinstance(bookmark_store, CommonBookmarkStore)
        assert bookmark_store.conn.get_connection_info()["database_type"] == "sqlite"
        assert bookmark_store.conn.get_connection_info()["db_path"] == "bookmarks.db"

    def test_create_bookmark_store_sqlite_custom_path(self):
        # Test with custom db_path
        config = {
            "type": "sqlite",
            "db_path": "custom_bookmarks.db"
        }
        
        bookmark_store = create_bookmark_store(config)
        
        # Factory now returns CommonBookmarkStore using SQLite backend
        assert isinstance(bookmark_store, CommonBookmarkStore)
        assert bookmark_store.conn.get_connection_info()["database_type"] == "sqlite"
        assert bookmark_store.conn.get_connection_info()["db_path"] == "custom_bookmarks.db"

    def test_create_bookmark_store_default_type(self):
        # Test without specifying a type (should default to sqlite)
        config = {}
        
        bookmark_store = create_bookmark_store(config)
        
        # Factory now returns CommonBookmarkStore using SQLite backend
        assert isinstance(bookmark_store, CommonBookmarkStore)
        assert bookmark_store.conn.get_connection_info()["database_type"] == "sqlite"
        assert bookmark_store.conn.get_connection_info()["db_path"] == "bookmarks.db"
    
    def test_create_bookmark_store_mssql_not_implemented(self):
        config = {"type": "mssql"}
        
        with pytest.raises(NotImplementedError) as excinfo:
            create_bookmark_store(config)
        
        assert "🔒 MS SQL Server bookmark tracking is available in PulsePipe Enterprise" in str(excinfo.value)
    
    def test_create_bookmark_store_s3_not_implemented(self):
        config = {"type": "s3"}
        
        with pytest.raises(NotImplementedError) as excinfo:
            create_bookmark_store(config)
        
        assert "🔒 S3 + DynamoDB scalable bookmark store is available in PulsePilot Enterprise" in str(excinfo.value)
    
    def test_create_bookmark_store_unsupported_type(self):
        config = {"type": "unsupported_type"}
        
        with pytest.raises(ValueError) as excinfo:
            create_bookmark_store(config)
        
        assert "Unsupported bookmark store type: unsupported_type" in str(excinfo.value)
    
    def test_create_bookmark_store_persistence_config_error(self):
        # Test that persistence config errors are raised explicitly (no silent fallback)
        config = {
            "persistence": {
                "database": {
                    "type": "postgresql",
                    "host": "invalid-host",
                    "port": 5432,
                    "username": "invalid-user",
                    "password": "invalid-pass",
                    "database": "invalid-db"
                }
            }
        }
        
        with patch('pulsepipe.adapters.file_watcher_bookmarks.factory.get_database_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")
            
            with pytest.raises(ConfigurationError) as excinfo:
                create_bookmark_store(config)
            
            error_msg = str(excinfo.value)
            assert "🔒 Postgresql bookmark store initialization failed" in error_msg
            assert "Connection attempt duration:" in error_msg
            # The diagnostic system now provides detailed error analysis
            assert (
                "Connection failed" in error_msg or 
                "Database Connection Failed" in error_msg or
                "network" in error_msg.lower() or
                "timeout" in error_msg.lower()
            )
    
    def test_create_bookmark_store_postgresql_legacy_config_error(self):
        # Test that PostgreSQL legacy config errors are raised with helpful messages
        config = {"type": "postgresql"}
        
        with patch('pulsepipe.adapters.file_watcher_bookmarks.factory.get_database_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Database connection error")
            
            with pytest.raises(ConfigurationError) as excinfo:
                create_bookmark_store(config)
            
            error_msg = str(excinfo.value)
            assert "🔒 PostgreSQL bookmark store initialization failed" in error_msg
            assert "Connection attempt duration:" in error_msg
            # The diagnostic system now provides detailed error analysis
            assert (
                "Database connection error" in error_msg or
                "Database Connection Failed" in error_msg or
                "network" in error_msg.lower() or
                "timeout" in error_msg.lower()
            )
    
    def test_create_bookmark_store_mongodb_legacy_config_error(self):
        # Test that MongoDB legacy config errors are raised with helpful messages
        config = {"type": "mongodb"}
        
        with patch('pulsepipe.adapters.file_watcher_bookmarks.factory.get_database_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("MongoDB connection timeout")
            
            with pytest.raises(ConfigurationError) as excinfo:
                create_bookmark_store(config)
            
            error_msg = str(excinfo.value)
            assert "🔒 MongoDB bookmark store initialization failed" in error_msg
            assert "Connection attempt duration:" in error_msg
            # The diagnostic system now provides detailed error analysis
            assert (
                "MongoDB connection timeout" in error_msg or
                "Database Connection Failed" in error_msg or
                "network" in error_msg.lower() or
                "timeout" in error_msg.lower()
            )
    
    def test_create_bookmark_store_persistence_config_unknown_db_type(self):
        # Test error handling when database type is unknown or missing
        config = {
            "persistence": {
                "database": {
                    # Missing 'type' field
                    "host": "localhost"
                }
            }
        }
        
        with patch('pulsepipe.adapters.file_watcher_bookmarks.factory.get_database_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Unknown database type")
            
            with pytest.raises(ConfigurationError) as excinfo:
                create_bookmark_store(config)
            
            error_msg = str(excinfo.value)
            assert "🔒 None bookmark store initialization failed" in error_msg or "🔒 Unknown bookmark store initialization failed" in error_msg
            assert "Connection attempt duration:" in error_msg