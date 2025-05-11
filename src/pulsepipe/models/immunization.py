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

# src/pulsepipe/models/immunizations.py

from typing import Optional
from pydantic import BaseModel

class Immunization(BaseModel):
    """
    Represents a vaccination or immunization administered to a patient.
    
    Captures information about vaccines administered to prevent disease,
    including the type of vaccine, when it was given, and its current status.
    This data is essential for tracking immunization compliance, public
    health reporting, and preventive care management.
    """
    vaccine_code: Optional[str]
    coding_method: Optional[str]
    description: Optional[str]
    date_administered: Optional[str]
    status: Optional[str]
    lot_number: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
