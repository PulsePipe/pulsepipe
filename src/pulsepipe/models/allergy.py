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

# src/pulsepipe/models/allergy.py

from typing import Optional
from pydantic import BaseModel

class Allergy(BaseModel):
    """
    Represents an allergic reaction with details about the substance,
    reaction, severity, onset, and associated patient.

    Note:
    - If a patient has **no known allergies**, this should be clearly indicated in their record.
    - If **no recorded allergies** are available, it implies that the patient's allergy information has not been documented.
    """
    substance: Optional[str]
    coding_method: Optional[str]
    reaction: Optional[str]
    severity: Optional[str]
    onset: Optional[str]
    patient_id: Optional[str]
