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

# src/pulsepipe/adapters/file_watcher.py

import asyncio
from pathlib import Path
from watchfiles import awatch
from .base import Adapter
from pulsepipe.persistence.factory import get_shared_sqlite_connection
from .file_watcher_bookmarks.sqlite_store import SQLiteBookmarkStore
from pulsepipe.utils.log_factory import LogFactory


class FileWatcherAdapter(Adapter):
    def __init__(self, config: dict):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing FileWatcherAdapter")

        self.watch_path = Path(config["watch_path"])
        self.file_extensions = tuple(config.get("extensions", [".json"]))
        self.logger.info(f"🔍 Watch path: {self.watch_path}")
        self.logger.info(f"📦 Watching extensions: {self.file_extensions}")

        from pulsepipe.persistence.factory import get_shared_sqlite_connection
        from pulsepipe.adapters.file_watcher_bookmarks.sqlite_store import SQLiteBookmarkStore

        sqlite_conn = get_shared_sqlite_connection({})
        self.bookmarks = SQLiteBookmarkStore(sqlite_conn)

    async def run(self, queue: asyncio.Queue):
        print(f"🚀 Starting watcher on: {self.watch_path}")
        async for changes in awatch(self.watch_path):
            for _, file_path in changes:
                print(f"📡 Detected file: {file_path}")
                if not file_path.endswith(self.file_extensions):
                    print(f"⛔ Skipping unsupported file type: {file_path}")
                    continue
                if self.bookmarks.is_processed(file_path):
                    print(f"🔁 Already processed: {file_path}")
                    continue
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        raw_data = f.read()
                    await queue.put(raw_data)
                    #print(f"✅ Enqueued: {file_path}")
                    self.bookmarks.mark_processed(file_path)
                except Exception as e:
                    print(f"❌ Error reading {file_path}: {e}")
