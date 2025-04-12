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
        self.continuous = config.get("continuous", True)  # New option to enable/disable continuous watching
        self.logger.info(f"🔍 Watch path: {self.watch_path}")
        self.logger.info(f"📦 Watching extensions: {self.file_extensions}")

        from pulsepipe.persistence.factory import get_shared_sqlite_connection
        from pulsepipe.adapters.file_watcher_bookmarks.sqlite_store import SQLiteBookmarkStore

        sqlite_conn = get_shared_sqlite_connection({})
        self.bookmarks = SQLiteBookmarkStore(sqlite_conn)

    async def run(self, queue: asyncio.Queue):
        self.logger.info(f"🚀 Starting watcher on: {self.watch_path}")
        
        # Ensure the watch directory exists
        if not self.watch_path.exists():
            self.watch_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"📁 Created watch directory: {self.watch_path}")
        
        # Process existing files first
        await self.process_existing_files(queue)
        
        # If continuous mode is enabled, continue watching for new files
        if self.continuous:
            await self.watch_for_changes(queue)
        else:
            self.logger.info("📁 One-time processing completed")

    async def process_existing_files(self, queue: asyncio.Queue):
        """Process existing files in the watch directory"""
        self.logger.info(f"🔍 Checking for existing files in {self.watch_path}")
        files_processed = 0
        
        for file_path in self.watch_path.glob('**/*'):
            if file_path.is_file() and file_path.suffix in self.file_extensions:
                str_path = str(file_path)
                if not self.bookmarks.is_processed(str_path):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            raw_data = f.read()
                        await queue.put(raw_data)
                        self.logger.info(f"✅ Enqueued: {file_path}")
                        self.bookmarks.mark_processed(str_path)
                        files_processed += 1
                    except Exception as e:
                        self.logger.info(f"❌ Error reading {file_path}: {e}")
        
        self.logger.info(f"📋 Processed {files_processed} existing files")
        return files_processed

    async def watch_for_changes(self, queue: asyncio.Queue):
        """Continuously watch for file changes"""
        self.logger.info(f"👀 Watching for changes in {self.watch_path}")
        
        async for changes in awatch(self.watch_path):
            for _, file_path in changes:
                self.logger.info(f"📡 Detected file: {file_path}")
                if not file_path.endswith(self.file_extensions):
                    self.logger.info(f"⛔ Skipping unsupported file type: {file_path}")
                    continue
                if self.bookmarks.is_processed(file_path):
                    self.logger.info(f"🔁 Already processed: {file_path}")
                    continue
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        raw_data = f.read()
                    await queue.put(raw_data)
                    self.logger.info(f"✅ Enqueued: {file_path}")
                    self.bookmarks.mark_processed(file_path)
                except Exception as e:
                    self.logger.info(f"❌ Error reading {file_path}: {e}")
