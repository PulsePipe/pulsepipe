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
from datetime import datetime
from typing import Optional, Dict
from pydantic import BaseModel
from .schema_version import CANONICAL_SCHEMA_VERSION
from pulsepipe.models.clinical_content import PulseClinicalContent

class PulseCanonicalMessage(BaseModel):
    id: Optional[str]                                # UUID or system-generated ID
    source_system: Optional[str]                     # e.g., "Epic", "Cerner", "HL7 Gateway"
    ingestor: Optional[str]                          # e.g., "HL7v2Ingestor", "FHIRIngestor"
    received_at: Optional[datetime]                  # Timestamp of ingestion
    processed_at: Optional[datetime]                 # When normalization completed
    deidentified: Optional[bool] = False             # If de-identified
    schema_version: str = CANONICAL_SCHEMA_VERSION   # Versioning
    content: PulseClinicalContent                    # The actual clinical content
    metadata: Optional[Dict[str, str]] = {}          # Extra metadata (batch, file_id, etc.)
