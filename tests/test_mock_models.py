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

# Mock models for testing
from typing import List, Optional, Dict
from pydantic import BaseModel

# Mock models for testing only
class MockPatientInfo(BaseModel):
    id: Optional[str] = "test-patient"
    dob_year: Optional[int] = 1980
    over_90: Optional[bool] = False
    gender: Optional[str] = "Unknown"
    geographic_area: Optional[str] = "Test Area"
    identifiers: Optional[Dict[str, str]] = {}
    preferences: Optional[List] = None

class MockEncounterProvider(BaseModel):
    id: Optional[str] = None
    type_code: Optional[str] = None
    coding_method: Optional[str] = None
    name: Optional[str] = None
    specialty: Optional[str] = None

class MockEncounterInfo(BaseModel):
    id: Optional[str] = "test-encounter"
    admit_date: Optional[str] = None
    discharge_date: Optional[str] = None
    encounter_type: Optional[str] = None
    type_coding_method: Optional[str] = None
    location: Optional[str] = None
    reason_code: Optional[str] = None
    reason_coding_method: Optional[str] = None
    providers: Optional[List[MockEncounterProvider]] = []
    visit_type: Optional[str] = None
    patient_id: Optional[str] = None