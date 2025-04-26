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

# src/pulsepipe/models/patient.py

from typing import Optional, List, Dict
from pydantic import BaseModel

class PatientPreferences(BaseModel):
    """
    Captures a patient's preferences regarding communication and care delivery.
    
    These preferences help healthcare providers deliver patient-centered care
    by accommodating linguistic needs, communication preferences, and other
    personal requirements that enhance the patient experience and support
    effective communication between patients and their care teams.
    """
    preferred_language: Optional[str]       # e.g., "English", "Spanish"
    communication_method: Optional[str]     # e.g., "Phone", "In-Person", "Interpreter"
    requires_interpreter: Optional[bool]    # True if interpreter needed
    preferred_contact_time: Optional[str]   # e.g., "Morning", "Afternoon"
    notes: Optional[str]                    # Free text

class PatientInfo(BaseModel):
    """
    Represents a patient's essential demographic information.
    
    This model is designed to be HIPAA-compliant when creating de-identified 
    datasets, storing only the minimum necessary demographic information
    allowed under Safe Harbor guidelines.
    
    For patients over 90 years old, the exact birth year is omitted and
    replaced with a flag indicating the patient is over 90, as required
    by HIPAA Safe Harbor. Geographic information is limited to state-level
    or first three digits of ZIP code (when population exceeds 20,000).
    """
    id: Optional[str]                           # Internal or pseudo-ID
    dob_year: Optional[int]                     # Only present if <90 years old
    over_90: Optional[bool] = False             # Indicates if patient is >=90 years old
    gender: Optional[str]                       # Retainable under HIPAA
    geographic_area: Optional[str]              # State, region, or partial ZIP
    identifiers: Optional[Dict[str, str]] = {}  # Only internal IDs allowed
    preferences: Optional[List[PatientPreferences]]

