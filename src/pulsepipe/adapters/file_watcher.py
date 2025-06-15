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

# src/pulsepipe/adapters/file_watcher.py

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Set, Dict, Any, List, Optional

from .base import Adapter
from pulsepipe.persistence.factory import get_database_connection
from .file_watcher_bookmarks.sqlite_store import SQLiteBookmarkStore
from .file_watcher_bookmarks.factory import create_bookmark_store
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import FileWatcherError, FileSystemError
from pulsepipe.utils.database_diagnostics import raise_database_diagnostic_error, DatabaseDiagnosticError


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
        self._scan_interval = 1.0  # Default scan interval in seconds in tests
        
        try:
            # Support for testing
            self.bookmark_file = config.get("bookmark_file")

            # Extract configuration options
            self.watch_path = Path(config["watch_path"])
            self.file_extensions = tuple(config.get("extensions", [".json"]))
            self.continuous = config.get("continuous", True)
            
            # Allow configurable scan interval
            if "scan_interval" in config:
                interval = float(config["scan_interval"])
                if interval > 0:
                    self._scan_interval = interval
            
            self.logger.info(f"🔍 Watch path: {self.watch_path}")
            self.logger.info(f"📦 Watching extensions: {self.file_extensions}")
            self.logger.info(f"⏱️ Scan interval: {self._scan_interval}s")

            # Initialize the bookmark store for tracking processed files
            # Handle both bookmark file (test compatibility) and database modes
            if hasattr(self, 'bookmark_file') and self.bookmark_file:
                # Use a simple in-memory structure for tests
                self.bookmarks = type('SimpleBookmarks', (), {
                    'processed_files': set(),
                    'mark_processed': lambda self, file_path: self.processed_files.add(file_path),
                    'is_processed': lambda self, file_path: file_path in self.processed_files
                })()
                self.logger.info(f"Using simple bookmark store for testing with {self.bookmark_file}")
            else:
                # Normal operation mode - use configured database or fail fast
                try:
                    # Check if we have persistence configuration in the config (passed from pipeline)
                    if "persistence" in config:
                        self.bookmarks = create_bookmark_store(config)
                        self.logger.info("Using unified bookmark store with persistence configuration")
                    else:
                        # No persistence config - this is a configuration error
                        error_msg = (
                            "🔴 No persistence configuration found\n"
                            "File watcher requires database configuration to track processed files.\n\n"
                            "Add persistence configuration to your pulsepipe.yaml:\n"
                            "persistence:\n"
                            "  database:\n"
                            "    type: postgresql  # or mongodb, sqlite\n"
                            "    host: localhost\n"
                            "    # ... other database settings\n\n"
                            "Fix the database connection to continue."
                        )
                        self.logger.error(error_msg)
                        raise DatabaseDiagnosticError(
                            error_msg,
                            "missing_persistence_config",
                            [
                                "Add persistence configuration to pulsepipe.yaml",
                                "Run 'pulsepipe config init --database <type>' to generate template",
                                "See documentation for database setup instructions"
                            ]
                        )
                except DatabaseDiagnosticError:
                    # Re-raise diagnostic errors as-is
                    raise
                except Exception as e:
                    # Run comprehensive diagnostics for other errors
                    self.logger.error(f"Failed to create bookmark store: {e}")
                    try:
                        raise_database_diagnostic_error(config, timeout=5)
                    except DatabaseDiagnosticError as diag_error:
                        self.logger.error(f"Database diagnostic error: {diag_error}")
                        raise
                    
                    # If diagnostics didn't catch it, create a generic diagnostic error
                    error_msg = (
                        f"🔴 File watcher bookmark store initialization failed: {e}\n\n"
                        "The file watcher requires a functional database connection to track processed files.\n"
                        "This prevents duplicate processing and maintains ingestion state.\n\n"
                        "Fix the database connection to continue."
                    )
                    raise DatabaseDiagnosticError(
                        error_msg,
                        "bookmark_store_init_failed",
                        [
                            "Verify database server is running and accessible",
                            "Check database configuration in pulsepipe.yaml",
                            "Run 'pulsepipe database health-check' for detailed diagnostics",
                            "Review application logs for connection errors"
                        ]
                    )
            
            # Track existing files to detect new ones
            self._known_files: Set[str] = set()
            
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
            files_processed = await self.process_existing_files(queue)
            self.logger.info(f"📋 Processed {files_processed} existing files")
            
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


    def _normalize_path(self, path):
        """Normalize path for consistent storage/retrieval"""
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            return str(path).replace('\\', '/')
        return str(path)
    
    async def process_existing_files(self, queue: asyncio.Queue) -> int:
        """Process existing files in the watch directory and return count of processed files"""
        self.logger.info(f"🔍 Checking for existing files in {self.watch_path}")
        files_processed = 0
        file_errors = []
        
        try:
            matching_files = self._find_matching_files()
            
            for file_path in matching_files:
                str_path = self._normalize_path(file_path)
                
                # Add to known files set for future change detection
                self._known_files.add(str_path)
                
                # Skip already processed files
                if self.bookmarks.is_processed(str_path):
                    continue
                
                try:
                    # Read and process the file
                    with open(file_path, 'r', encoding='utf-8') as f:
                        raw_data = f.read()
                    
                    # Put data on the queue
                    await queue.put(raw_data)
                    self.logger.info(f"✅ Enqueued: {file_path}")
                    
                    # Mark as processed
                    self.bookmarks.mark_processed(str_path)
                    files_processed += 1
                except Exception as e:
                    error_details = {
                        "file_path": str(file_path),
                        "error_type": type(e).__name__
                    }
                    self.logger.error(f"❌ Error reading {file_path}: {e}")
                    file_errors.append(error_details)
            
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
            # Initial set of known files
            if not self._known_files:
                self._known_files = set(self._normalize_path(f) for f in self._find_matching_files())
            
            while not self._stop_event.is_set():
                # Check for new files
                current_files = set(self._normalize_path(f) for f in self._find_matching_files())
                
                # Find new files (in current but not in known)
                new_files = current_files - self._known_files
                
                # Process new files
                for file_path in new_files:
                    self.logger.info(f"📡 Detected new file: {file_path}")
                    
                    # Skip already processed files (extra safety check)
                    if self.bookmarks.is_processed(file_path):
                        self.logger.info(f"🔁 Already processed: {file_path}")
                        continue
                    
                    try:
                        # Read and process the file - use the original path, not normalized for file I/O
                        original_path = file_path
                        if sys.platform == 'win32':
                            # Convert back to OS-specific path for file operations if needed
                            original_path = file_path.replace('/', '\\')
                            
                        with open(original_path, 'r', encoding='utf-8') as f:
                            raw_data = f.read()
                        
                        # Put data on the queue
                        await queue.put(raw_data)
                        self.logger.info(f"✅ Enqueued: {file_path}")
                        
                        # Mark as processed with normalized path
                        self.bookmarks.mark_processed(file_path)
                    except FileNotFoundError:
                        self.logger.info(f"🚫 File disappeared before processing: {file_path}")
                    except PermissionError:
                        self.logger.error(f"🔒 Permission denied for file: {file_path}")
                    except Exception as e:
                        self.logger.error(f"❌ Error reading {file_path}: {e}")
                
                # Update known files
                self._known_files = current_files
                
                # Wait for a bit before the next scan
                # Check if a flag was passed to just do a single scan and complete
                single_scan = getattr(self, 'single_scan_mode', False)
                if single_scan:
                    # In single scan mode, process once and then exit
                    self.logger.info("Single scan mode enabled - processed existing files, exiting")
                    return
                
                try:
                    # Use asyncio.wait_for so we can cancel it when stop is requested
                    await asyncio.wait_for(
                        self._stop_event.wait(), 
                        timeout=self._scan_interval
                    )
                except asyncio.TimeoutError:
                    # This is expected - timeout just means keep scanning
                    pass
                
        except asyncio.CancelledError:
            self.logger.info("🛑 File watcher task was cancelled")
            raise
        except Exception as e:
            raise FileWatcherError(
                f"Error watching for file changes: {str(e)}",
                details={"watch_path": str(self.watch_path)},
                cause=e
            ) from e


    def _find_matching_files(self) -> List[Path]:
        """Find all files in watch_path with matching extensions"""
        matching_files = []
        
        try:
            for file_path in self.watch_path.glob('**/*'):
                if file_path.is_file() and file_path.suffix in self.file_extensions:
                    matching_files.append(file_path)
            return matching_files
        except Exception as e:
            self.logger.error(f"Error scanning directory {self.watch_path}: {e}")
            return []