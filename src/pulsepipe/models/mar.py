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

# src/pulsepipe/models/mar.py

from typing import Optional
from pydantic import BaseModel

class MAR(BaseModel):
    """
    Represents a Medication Administration Record entry.
    
    The Medication Administration Record (MAR) documents the actual 
    administration of medications to a patient, as opposed to medication
    orders or prescriptions. MAR entries provide a detailed log of what
    medications were given, when, and by whom, serving as both a legal
    record and clinical documentation of patient care.
    
    Each entry typically represents a single medication administration event.
    """
    medication_code: Optional[str]
    coding_method: Optional[str]
    medication: Optional[str]
    dosage: Optional[str]
    route: Optional[str]
    frequency: Optional[str]
    administered_at: Optional[str]
    status: Optional[str]
    notes: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]