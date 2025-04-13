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
from pulsepipe.utils.errors import FileWatcherError, FileSystemError


class FileWatcherAdapter(Adapter):
    """
    Monitors a directory for healthcare data files and processes them.
    
    This adapter watches a specified directory for files with supported extensions
    and processes them as they appear, supporting both one-time batch processing
    and continuous monitoring modes.
    """
    
    def __init__(self, config: dict):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing FileWatcherAdapter")
        self._stop_event = asyncio.Event()

        try:
            # Extract configuration options
            self.watch_path = Path(config["watch_path"])
            self.file_extensions = tuple(config.get("extensions", [".json"]))
            self.continuous = config.get("continuous", True)
            
            self.logger.info(f"🔍 Watch path: {self.watch_path}")
            self.logger.info(f"📦 Watching extensions: {self.file_extensions}")

            # Initialize the bookmark store for tracking processed files
            sqlite_conn = get_shared_sqlite_connection({})
            self.bookmarks = SQLiteBookmarkStore(sqlite_conn)
            
        except KeyError as e:
            # Specific error for missing required configuration
            missing_key = str(e).strip("'")
            raise FileWatcherError(
                f"Missing required configuration: {missing_key}",
                details={"config_keys": list(config.keys())}
            ) from e
        except Exception as e:
            # General initialization error
            raise FileWatcherError(
                "Failed to initialize FileWatcherAdapter",
                details={"watch_path": config.get("watch_path", "Not specified")},
                cause=e
            ) from e

    async def run(self, queue: asyncio.Queue):
        self.logger.info(f"🚀 Starting watcher on: {self.watch_path}")
        
        try:
            # Ensure the watch directory exists
            if not self.watch_path.exists():
                try:
                    self.watch_path.mkdir(parents=True, exist_ok=True)
                    self.logger.info(f"📁 Created watch directory: {self.watch_path}")
                except Exception as e:
                    raise FileSystemError(
                        f"Failed to create watch directory: {self.watch_path}",
                        details={"permission_error": str(e)},
                        cause=e
                    ) from e
            
            # Process existing files first
            await self.process_existing_files(queue)
            
            # If continuous mode is enabled, continue watching for new files
            if self.continuous:
                await self.watch_for_changes(queue)
            else:
                self.logger.info("📁 One-time processing completed")
                
        except asyncio.CancelledError:
            self.logger.info("🛑 File watcher task was cancelled")
            raise
        except Exception as e:
            # Catch-all for other errors
            raise FileWatcherError(
                f"Error in file watcher run operation: {str(e)}",
                details={"watch_path": str(self.watch_path)},
                cause=e
            ) from e


    async def stop(self):
        self.logger.info("🛑 Stop event set on FileWatcherAdapter")
        self._stop_event.set()


    async def process_existing_files(self, queue: asyncio.Queue):
        """Process existing files in the watch directory"""
        self.logger.info(f"🔍 Checking for existing files in {self.watch_path}")
        files_processed = 0
        file_errors = []
        
        try:
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
                            error_details = {
                                "file_path": str(file_path),
                                "error_type": type(e).__name__
                            }
                            self.logger.error(f"❌ Error reading {file_path}: {e}")
                            file_errors.append(error_details)
            
            self.logger.info(f"📋 Processed {files_processed} existing files")
            
            # If we encountered errors but processed some files, continue
            if file_errors and files_processed > 0:
                self.logger.warning(
                    f"⚠️ Encountered {len(file_errors)} errors while processing existing files"
                )
            # If we only had errors and processed nothing, raise an exception
            elif file_errors and files_processed == 0:
                raise FileWatcherError(
                    f"Failed to process any existing files ({len(file_errors)} errors)",
                    details={"errors": file_errors}
                )
                
            return files_processed
            
        except Exception as e:
            if not isinstance(e, FileWatcherError):
                raise FileWatcherError(
                    "Error processing existing files",
                    details={
                        "watch_path": str(self.watch_path),
                        "file_errors": file_errors
                    },
                    cause=e
                ) from e
            raise

    async def watch_for_changes(self, queue: asyncio.Queue):
        """Continuously watch for file changes"""
        self.logger.info(f"👀 Watching for changes in {self.watch_path}")
        
        try:
            async for changes in awatch(self.watch_path):
                if self._stop_event.is_set():
                    self.logger.info("🛑 Detected stop event. Exiting watch loop.")
                    break

                for _, file_path in changes:
                    str_path = str(file_path)
                    self.logger.info(f"📡 Detected file: {file_path}")
                    
                    # Skip files with unsupported extensions
                    if not file_path.endswith(self.file_extensions):
                        self.logger.info(f"⛔ Skipping unsupported file type: {file_path}")
                        continue
                        
                    # Skip already processed files
                    if self.bookmarks.is_processed(str_path):
                        self.logger.info(f"🔁 Already processed: {file_path}")
                        continue
                        
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            raw_data = f.read()
                        await queue.put(raw_data)
                        self.logger.info(f"✅ Enqueued: {file_path}")
                        self.bookmarks.mark_processed(str_path)
                    except FileNotFoundError:
                        # This can happen if the file is deleted before we read it
                        self.logger.info(f"🚫 File disappeared before processing: {file_path}")
                    except PermissionError as e:
                        self.logger.error(f"🔒 Permission denied for file: {file_path}")
                    except Exception as e:
                        self.logger.error(f"❌ Error reading {file_path}: {e}")
                        
        except asyncio.CancelledError:
            self.logger.info("🛑 File watcher task was cancelled")
            raise
        except Exception as e:
            raise FileWatcherError(
                f"Error watching for file changes: {str(e)}",
                details={"watch_path": str(self.watch_path)},
                cause=e
            ) from e
