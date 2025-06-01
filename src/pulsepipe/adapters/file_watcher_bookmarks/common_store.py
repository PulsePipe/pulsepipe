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

import os
import sys
from typing import List
from .base import BookmarkStore
from pulsepipe.persistence.database import DatabaseConnection, DatabaseDialect


class CommonBookmarkStore(BookmarkStore):
    """
    Common bookmark store that works with all database backends.
    
    Uses the DatabaseDialect pattern to support SQLite, PostgreSQL, and MongoDB.
    """
    
    def __init__(self, connection: DatabaseConnection, dialect: DatabaseDialect):
        """
        Initialize common bookmark store.
        
        Args:
            connection: Database connection from the adapter system
            dialect: SQL dialect for database-specific operations
        """
        self.conn = connection
        self.dialect = dialect
        self._ensure_schema()

    def _ensure_schema(self):
        """Create bookmarks table if it doesn't exist."""
        if hasattr(self.dialect, 'get_bookmark_table_create'):
            create_sql = self.dialect.get_bookmark_table_create()
            try:
                self.conn.execute(create_sql)
                self.conn.commit()
            except Exception:
                # Table might already exist
                pass

    def is_processed(self, path: str) -> bool:
        """Check if a file path has been processed."""
        # Normalize path for Windows compatibility
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            path = path.replace('\\', '/')
        
        if hasattr(self.dialect, 'get_bookmark_check'):
            sql = self.dialect.get_bookmark_check()
            result = self.conn.execute(sql, (path,))
            return len(result.rows) > 0
        
        return False

    def mark_processed(self, path: str, status: str = "processed"):
        """Mark a file path as processed."""
        # Normalize path for Windows compatibility
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            path = path.replace('\\', '/')
        
        if hasattr(self.dialect, 'get_bookmark_insert'):
            sql = self.dialect.get_bookmark_insert()
            self.conn.execute(sql, (path, status))
            self.conn.commit()

    def get_all(self) -> List[str]:
        """Get all processed file paths."""
        if hasattr(self.dialect, 'get_bookmark_list'):
            sql = self.dialect.get_bookmark_list()
            result = self.conn.execute(sql)
            return [row['path'] for row in result.rows]
        
        return []

    def clear_all(self) -> int:
        """Clear all bookmarks and return count of deleted records."""
        if hasattr(self.dialect, 'get_bookmark_clear'):
            sql = self.dialect.get_bookmark_clear()
            result = self.conn.execute(sql)
            self.conn.commit()
            return result.rowcount or 0
        
        return 0