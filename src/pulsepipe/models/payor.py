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

# src/pulsepipe/models/payor.py

from typing import Optional
from pydantic import BaseModel

class Payor(BaseModel):
    """
    Represents a health insurance plan or payer organization.
    
    This model captures information about the entity responsible for
    paying for healthcare services, whether a commercial insurance company, 
    government program (Medicare, Medicaid), or other third-party payer.
    
    It includes details about the insurance plan, membership information,
    and eligibility status, all crucial for healthcare revenue cycle 
    management and claims processing.
    
    Note that identifiable member IDs and group numbers are stored as 
    hashed values to protect patient privacy.
    """
    name: Optional[str]
    member_id_hash: Optional[str]
    group_number_hash: Optional[str]
    plan_type: Optional[str]
    type: Optional[str]
    eligibility_status: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]