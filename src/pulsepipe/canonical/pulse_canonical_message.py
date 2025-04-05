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

# src/pulsepipe/canonical/pulse_canonical_message.py

from datetime import datetime
from typing import Optional, Dict, Union
from pydantic import BaseModel, Field
from .schema_version import CANONICAL_SCHEMA_VERSION
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.op_content import PulseOperationalContent

class PulseCanonicalMessage(BaseModel):
    id: Optional[str]                                # UUID or system-generated ID
    source_system: Optional[str]                     # e.g., "Epic", "Cerner", "HL7 Gateway"
    ingestor: Optional[str]                          # e.g., "HL7v2Ingestor", "FHIRIngestor", "X12Ingester"
    received_at: Optional[datetime]                  # Timestamp of ingestion
    processed_at: Optional[datetime]                 # When normalization completed
    deidentified: Optional[bool] = False             # If de-identified
    schema_version: str = CANONICAL_SCHEMA_VERSION   # Versioning
    
    # Content can be either clinical or operational, but not both
    clinical_content: Optional[PulseClinicalContent] = None   # The clinical content (if applicable)
    operational_content: Optional[PulseOperationalContent] = None   # The operational content (if applicable)
    
    metadata: Dict[str, str] = {}          # Extra metadata (batch, file_id, etc.)
    
    class Config:
        validate_assignment = True

    @property
    def content_type(self) -> str:
        """Returns the type of content contained in this message"""
        if self.clinical_content is not None:
            return "clinical"
        elif self.operational_content is not None:
            return "operational"
        else:
            return "unknown"