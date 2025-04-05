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

# src/pulsepipe/models/encounter.py

from typing import List, Optional
from pydantic import BaseModel

class EncounterProvider(BaseModel):
    """
    Represents a provider involved in a clinical encounter.
    """
    id: Optional[str]
    type_code: Optional[str]
    coding_method: Optional[str]
    name: Optional[str]
    specialty: Optional[str]

class EncounterInfo(BaseModel):
    """
    Represents a clinical encounter, such as an inpatient admission, outpatient visit,
    or emergency room encounter.
    """
    id: Optional[str]
    admit_date: Optional[str]
    discharge_date: Optional[str]
    encounter_type: Optional[str]
    type_coding_method: Optional[str]
    location: Optional[str]
    reason_code: Optional[str]
    reason_coding_method: Optional[str]
    providers: Optional[List[EncounterProvider]] = []
    visit_type: Optional[str]
    patient_id: Optional[str]
