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

# src/pulsepipe/models/problem.py

from typing import Optional
from pydantic import BaseModel

class Problem(BaseModel):
    """
    Represents an entry in a patient's problem list.
    
    The problem list is a central component of the medical record that 
    documents a patient's ongoing or chronic medical conditions requiring
    management over time. Unlike encounter-specific diagnoses, problem list
    entries represent longer-term conditions that influence care decisions
    across multiple encounters.
    
    Problem lists help providers maintain continuity of care and ensure
    that all ongoing medical issues are considered in clinical decision-making.
    """
    code: Optional[str] = None
    coding_method: Optional[str] = None
    description: Optional[str] = None
    onset_date: Optional[str] = None
    patient_id: Optional[str] = None
    encounter_id: Optional[str] = None