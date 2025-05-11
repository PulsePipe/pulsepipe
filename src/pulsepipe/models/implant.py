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

# src/pulsepipe/models/implant.py

from typing import Optional
from pydantic import BaseModel

class Implant(BaseModel):
    """
    Represents a medical device implanted in a patient's body.
    
    Tracks information about implantable medical devices such as pacemakers,
    joint replacements, stents, artificial heart valves, or other devices
    surgically placed inside a patient's body. This information is critical
    for patient safety, follow-up care, and device recalls.
    """
    implant_id: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
    device_name: Optional[str]
    device_code: Optional[str]
    coding_method: Optional[str]              # SNOMED, UDI, local
    implant_date: Optional[str]
    explant_date: Optional[str]
    status: Optional[str]                     # active, removed, expired
    notes: Optional[str]
