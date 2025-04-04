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

class IngestionEngine:
    def __init__(self, adapter, ingester):
        self.adapter = adapter
        self.ingester = ingester
        self.queue = asyncio.Queue()

    async def process(self):
        while True:
            raw_data = await self.queue.get()
            try:
                result = self.ingester.parse(raw_data)

                # ✅ Print PulseClinicalContent nicely
                print("🧪 Common Data Model Results:")
                print(result.json(indent=2))

            except Exception as e:
                print(f"❌ Ingestion error: {e}")
            finally:
                self.queue.task_done()
    
    async def run(self):
        await asyncio.gather(
            self.adapter.run(self.queue),
            *[self.process() for _ in range(4)]  # 4 parallel workers
        )
