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


from typing import Optional, List, Dict
from pydantic import BaseModel

class PatientPreferences(BaseModel):
    preferred_language: Optional[str]       # e.g., "English", "Spanish"
    communication_method: Optional[str]     # e.g., "Phone", "In-Person", "Interpreter"
    requires_interpreter: Optional[bool]    # True if interpreter needed
    preferred_contact_time: Optional[str]   # e.g., "Morning", "Afternoon"
    notes: Optional[str]                    # Free text

class PatientInfo(BaseModel):
    id: Optional[str]                           # Internal or pseudo-ID
    dob_year: Optional[int]                     # Only present if <90 years old
    over_90: Optional[bool] = False             # Indicates if patient is >=90 years old
    gender: Optional[str]                       # Retainable under HIPAA
    geographic_area: Optional[str]              # State, region, or partial ZIP
    identifiers: Optional[Dict[str, str]] = {}  # Only internal IDs allowed
    preferences: Optional[List[PatientPreferences]]

