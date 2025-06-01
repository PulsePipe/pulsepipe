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
from pulsepipe.adapters.file_watcher_bookmarks.sqlite_store import SQLiteBookmarkStore

class TestBookmarkStoreFactory:
    def test_create_bookmark_store_sqlite_default(self):
        # Test with default sqlite config (minimal)
        config = {"type": "sqlite"}
        
        bookmark_store = create_bookmark_store(config)
        
        assert isinstance(bookmark_store, SQLiteBookmarkStore)
        assert bookmark_store.db_path == "bookmarks.db"
    
    def test_create_bookmark_store_sqlite_custom_path(self):
        # Test with custom db_path
        config = {
            "type": "sqlite",
            "db_path": "custom_bookmarks.db"  # Use a relative path that's writable in the test environment
        }
        
        bookmark_store = create_bookmark_store(config)
        
        assert isinstance(bookmark_store, SQLiteBookmarkStore)
        assert bookmark_store.db_path == "custom_bookmarks.db"
    
    def test_create_bookmark_store_default_type(self):
        # Test without specifying a type (should default to sqlite)
        config = {}
        
        bookmark_store = create_bookmark_store(config)
        
        assert isinstance(bookmark_store, SQLiteBookmarkStore)
        assert bookmark_store.db_path == "bookmarks.db"
    
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