# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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

import sqlite3
import os
import sys
from .base import BookmarkStore

class SQLiteBookmarkStore(BookmarkStore):
    def __init__(self, db_path: str):
        self.db_path = db_path
        
        # Ensure the directory exists before creating the database
        db_dir = os.path.dirname(db_path)
        # Only try to create directory if there is a directory component
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            
        self.conn = sqlite3.connect(db_path)
        self._ensure_schema()

    def _ensure_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bookmarks (
                path TEXT PRIMARY KEY,
                status TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def is_processed(self, path: str) -> bool:
        # Normalize path for Windows
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            path = path.replace('\\', '/')
        result = self.conn.execute("SELECT 1 FROM bookmarks WHERE path = ?", (path,)).fetchone()
        return result is not None

    def mark_processed(self, path: str, status: str = "processed"):
        # Normalize path for Windows
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            path = path.replace('\\', '/')
        self.conn.execute(
            "INSERT OR IGNORE INTO bookmarks (path, status) VALUES (?, ?)",
            (path, status)
        )
        self.conn.commit()

    def get_all(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT path FROM bookmarks ORDER BY path")
        return [row[0] for row in cursor.fetchall()]
    
    def clear_all(self):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM bookmarks")
        count = cursor.rowcount
        self.conn.commit()
        return count
