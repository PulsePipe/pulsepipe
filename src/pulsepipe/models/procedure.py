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

# src/pulsepipe/models/procedure.py

from typing import List, Optional
from pydantic import BaseModel

class ProcedureProvider(BaseModel):
    """
    Represents a healthcare provider involved in performing a procedure.
    
    Procedures often involve multiple healthcare providers in different roles,
    such as the primary surgeon, assistant surgeon, anesthesiologist, or
    other specialists. This model captures the identity and role of each
    provider associated with a procedure.
    """
    provider_id: Optional[str]
    role: Optional[str]

class Procedure(BaseModel):
    """
    Represents a medical or surgical procedure performed on a patient.
    
    Procedures encompass a wide range of interventions, from minor
    diagnostic procedures to major surgeries. They're typically coded
    using CPT, HCPCS, or ICD-10-PCS code sets and documented both for
    clinical care and billing purposes.
    
    This model captures the basic details of a procedure, including what
    was done, when it was performed, who performed it, and the outcome
    or status of the procedure.
    """
    code: Optional[str]
    coding_method: Optional[str]
    description: Optional[str]
    performed_date: Optional[str]
    status: Optional[str]
    providers: List[ProcedureProvider] = []
    patient_id: Optional[str]
    encounter_id: Optional[str]
