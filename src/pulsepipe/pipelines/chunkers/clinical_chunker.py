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

# src/pulsepipe/pipelines/chuncker/clinical_chunker.py

from typing import List, Dict, Any
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models.clinical_content import PulseClinicalContent


class ClinicalSectionChunker:
    def __init__(self, include_metadata: bool = True):
        self.include_metadata = include_metadata
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing ClinicalSectionChunker")

    def chunk(self, content: PulseClinicalContent) -> List[Dict[str, Any]]:
        chunks = []

        patient_id = getattr(content.patient, "id", None) if content.patient else None
        encounter_id = getattr(content.encounter, "id", None) if content.encounter else None

        for field_name, value in content.__dict__.items():
            if isinstance(value, list) and value:
                chunk = {
                    "type": field_name,
                    "content": [v.model_dump() for v in value]
                }
                if self.include_metadata:
                    chunk["metadata"] = {
                        "patient_id": patient_id,
                        "encounter_id": encounter_id
                    }
                chunks.append(chunk)

        self.logger.info(f"🧩 ClinicalSectionChunker produced {len(chunks)} chunks 🧠 (patient_id={patient_id}, encounter_id={encounter_id})")
        return chunks
