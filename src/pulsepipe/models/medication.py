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

# src/pulsepipe/models/medication.py

from typing import Optional
from pydantic import BaseModel

class Medication(BaseModel):
    """
    Represents a medication prescribed to a patient.
    
    This model captures medication orders or prescriptions, including details
    about the drug, dosage, administration instructions, and prescription status.
    Unlike the MAR model which records actual administration events, this model
    represents the prescriber's intent for medication therapy.
    
    Medications can be active, discontinued, or completed, and may be associated
    with specific encounters or represent ongoing medication therapy.
    
    This includes both prescription medications and over-the-counter (OTC) 
    medications that patients take at home.
    """
    code: Optional[str]
    coding_method: Optional[str]
    name: Optional[str]
    dose: Optional[str]
    route: Optional[str]
    frequency: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    status: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
    notes: Optional[str]