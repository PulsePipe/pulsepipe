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
from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.log_factory import LogFactory

def safe_load_config(path):
    try:
        return load_config(path)
    except Exception as e:
        print(f"❌ Failed to load config from {path}: {e}")
        raise

async def main():
    config = safe_load_config("pulsepipe.yaml")
    LogFactory.init_from_config(config.get("logging", {}))

    adapter_config = safe_load_config("adapter.yaml")["adapter"]
    ingester_config = safe_load_config("ingester.yaml")["ingester"]

    adapter = create_adapter(adapter_config)
    ingester = create_ingester(ingester_config)

    engine = IngestionEngine(adapter, ingester)
    await engine.run()

if __name__ == "__main__":
    asyncio.run(main())
