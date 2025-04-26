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

# tests/test_file_watcher_adapter.py

import asyncio
import sys
import os
import tempfile
from pathlib import Path
import pytest
from pulsepipe.adapters.file_watcher import FileWatcherAdapter

@pytest.mark.asyncio
async def test_file_watcher_adapter_enqueues_data(tmp_path):
    """Test that the file watcher adapter correctly enqueues data from detected files."""
    # Set environment variable early for Windows - this helps normalize_paths_for_tests fixture
    if sys.platform == 'win32' and 'PYTEST_CURRENT_TEST' in os.environ:
        os.environ['test_file_watcher_adapter_enqu'] = 'running'
    
    # Setup the test directory
    ingest_path = tmp_path / "fixtures"
    ingest_path.mkdir(parents=True, exist_ok=True)
    
    # Normalize path for Windows
    ingest_path_str = str(ingest_path)
    if sys.platform == 'win32':
        ingest_path_str = ingest_path_str.replace('\\', '/')

    adapter_config = {
        "watch_path": ingest_path_str,
        "extensions": [".json"],
        "bookmark_file": ".bookmark.dat",
        # Extra option for Windows compatibility in tests
        "test_mode": True if 'PYTEST_CURRENT_TEST' in os.environ else False
    }

    # Create adapter and queue
    adapter = FileWatcherAdapter(adapter_config)
    queue = asyncio.Queue()

    # Create a run task with proper cleanup
    try:
        task = asyncio.create_task(adapter.run(queue))
        
        # Create and write the test file after a short delay to ensure adapter is running
        test_content = '{"resourceType": "Patient", "id": "test-patient"}'
        test_file = ingest_path / "test_patient.json"
        
        # Normalize test file path for Windows
        test_file_str = str(test_file)
        if sys.platform == 'win32':
            test_file_str = test_file_str.replace('\\', '/')
        
        await asyncio.sleep(0.5)
        
        # Write the test file
        with open(str(test_file), 'w', encoding='utf-8') as f:
            f.write(test_content)
            f.flush()
        
        # Wait for the data to be processed and enqueued
        try:
            raw_data = await asyncio.wait_for(queue.get(), timeout=3)
        except asyncio.TimeoutError:
            pytest.fail("Adapter did not enqueue data in time.")
        
        # Verify the content matches
        assert raw_data == test_content
        
    finally:
        # Always cleanup the task properly
        if 'task' in locals() and not task.done():
            task.cancel()
            try:
                # Use short timeout to avoid blocking test cleanup
                await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                # These are expected during cancellation
                pass
            
        # Clean up environment variable
        if 'test_file_watcher_adapter_enqu' in os.environ:
            del os.environ['test_file_watcher_adapter_enqu']
