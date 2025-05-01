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
import os
import sys
import pytest

from pulsepipe.adapters.file_watcher import FileWatcherAdapter

@pytest.mark.asyncio
async def test_filewatcher_enqueue(tmp_path, monkeypatch):
    """Test FileWatcherAdapter with simple tmp_path."""

    ingest_path = tmp_path / "fixtures"
    ingest_path.mkdir(parents=True, exist_ok=True)


    ingest_path_str = str(ingest_path)
    if sys.platform == 'win32':
        ingest_path_str = ingest_path_str.replace("\\", "/")

    if sys.platform == 'win32':
        monkeypatch.setenv("fwt", "running")

    adapter_config = {
        "watch_path": ingest_path_str,
        "extensions": [".json"],
        "bookmark_file": ".bookmark.dat",
        "test_mode": True,
    }

    adapter = FileWatcherAdapter(adapter_config)
    queue = asyncio.Queue()

    try:
        task = asyncio.create_task(adapter.run(queue))

        # Simulate creating a file
        test_file = ingest_path / "test_patient.json"
        test_content = '{"resourceType": "Patient", "id": "test-patient"}'

        await asyncio.sleep(0.5)

        with open(test_file, "w", encoding="utf-8") as f:
            f.write(test_content)
            f.flush()

        try:
            raw_data = await asyncio.wait_for(queue.get(), timeout=3.0)
        except asyncio.TimeoutError:
            pytest.fail("Adapter did not enqueue data in time.")

        assert raw_data == test_content

    finally:
        if 'task' in locals() and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        if 'test_file_watcher_adapter_enqu' in os.environ:
            del os.environ['test_file_watcher_adapter_enqu']
