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

# src/pulsepipe/models/vital_signs.py

from typing import Optional, Union
from pydantic import BaseModel

class VitalSign(BaseModel):
    """
    Represents a physiological measurement or vital sign.
    
    Vital signs are clinical measurements of essential body functions
    that provide critical indicators of a patient's physical state.
    Standard vital signs include temperature, blood pressure, pulse rate,
    respiratory rate, oxygen saturation, height, weight, and pain level.
    
    These measurements form the foundation of patient assessment and
    are used to track changes in a patient's condition over time.
    Abnormal vital signs often serve as early indicators of clinical
    deterioration or improvement.
    """
    code: Optional[str]
    coding_method: Optional[str]
    display: Optional[str]
    value: Union[str, float]
    unit: Optional[str]
    timestamp: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]