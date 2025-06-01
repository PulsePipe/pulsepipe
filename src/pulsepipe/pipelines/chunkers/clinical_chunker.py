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

    def _serialize_item(self, item):
        """Safely serialize a single item to dict"""
        if hasattr(item, 'model_dump'):
            return item.model_dump()
        elif hasattr(item, 'dict'):
            return item.dict()
        elif isinstance(item, dict):
            return item  # Already a dict
        elif hasattr(item, '__dict__'):
            return item.__dict__
        else:
            # For primitive types or unsupported objects
            return item

    def chunk(self, content: PulseClinicalContent) -> List[Dict[str, Any]]:
        chunks = []

        patient_id = getattr(content.patient, "id", None) if content.patient else None
        encounter_id = getattr(content.encounter, "id", None) if content.encounter else None

        for field_name, value in content.__dict__.items():
            if isinstance(value, list) and value:
                try:
                    chunk = {
                        "type": field_name,
                        "content": [self._serialize_item(v) for v in value]
                    }
                    if self.include_metadata:
                        chunk["metadata"] = {
                            "patient_id": patient_id,
                            "encounter_id": encounter_id
                        }
                    
                    chunks.append(chunk)
                except Exception as e:
                    self.logger.error(f"Error serializing field {field_name}: {e}")
                    self.logger.error(f"Value type: {type(value)}, First item type: {type(value[0]) if value else 'N/A'}")
                    # Skip this field if serialization fails
                    # ToDo: Log this error for audit metrics
                    continue

        self.logger.info(f"[PIPE] 🧩 Created {len(chunks)} clinical chunks from {len(content.__dict__.items())} FHIR entries 🧠 (patient_id={patient_id}, encounter_id={encounter_id})")
        return chunks
