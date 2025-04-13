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

# src/pulsepipe/pipelines/chuncker/operational_chunker.py

from typing import List, Dict, Any
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models.operational_content import PulseOperationalContent


class OperationalEntityChunker:
    def __init__(self, include_metadata: bool = True):
        self.include_metadata = include_metadata
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing OperationalEntityChunker")

    def chunk(self, content: PulseOperationalContent) -> List[Dict[str, Any]]:
        chunks = []

        transaction_type = content.transaction_type or "unknown"
        org_id = content.organization_id or "unknown"

        for field_name, value in content.__dict__.items():
            if isinstance(value, list) and value:
                chunk = {
                    "type": field_name,
                    "content": [v.model_dump() for v in value]
                }
                if self.include_metadata:
                    chunk["metadata"] = {
                        "transaction_type": transaction_type,
                        "organization_id": org_id
                    }
                chunks.append(chunk)

        self.logger.info(f"🧩 OperationalEntityChunker produced {len(chunks)} chunks 🧠 (transaction_type={transaction_type}, org_id={org_id})")
        return chunks
