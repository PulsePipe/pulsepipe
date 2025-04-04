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

import asyncio
from pathlib import Path
import pytest
from pulsepipe.adapters.file_watcher import FileWatcherAdapter

@pytest.mark.asyncio
async def test_file_watcher_adapter_enqueues_data(tmp_path):
    ingest_path = tmp_path / "fixtures"
    ingest_path.mkdir(parents=True, exist_ok=True)

    adapter_config = {
        "watch_path": str(ingest_path),
        "extensions": [".json"],
        "bookmark_file": ".bookmark.dat"
    }

    adapter = FileWatcherAdapter(adapter_config)
    queue = asyncio.Queue()

    task = asyncio.create_task(adapter.run(queue))

    test_content = '{"resourceType": "Patient", "id": "test-patient"}'
    test_file = ingest_path / "test_patient.json"

    await asyncio.sleep(0.5)
    test_file.write_text(test_content, encoding='utf-8')

    try:
        raw_data = await asyncio.wait_for(queue.get(), timeout=3)
    except asyncio.TimeoutError:
        task.cancel()
        pytest.fail("Adapter did not enqueue data in time.")

    assert raw_data == test_content

    task.cancel()
