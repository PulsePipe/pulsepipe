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

# src/pulsepipe/models/care_plan.py

from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class CarePlanActivity(BaseModel):
    """
    Represents a specific planned activity within a care plan.
    
    This includes treatments, investigations, and other activities that are
    planned as part of the care plan.
    """
    id: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None
    code: Optional[str] = None
    code_system: Optional[str] = None
    detail_status: Optional[str] = None  # Status of the detailed activity
    detail_description: Optional[str] = None
    detail_code: Optional[str] = None
    detail_code_system: Optional[str] = None
    category: Optional[str] = None  # Category of the activity
    category_code: Optional[str] = None
    category_system: Optional[str] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    location: Optional[str] = None
    performer: Optional[str] = None
    performer_type: Optional[str] = None  # Type of performer (practitioner, organization)
    notes: Optional[str] = None
    metadata: Dict[str, Any] = {}

class CarePlan(BaseModel):
    """
    Represents a healthcare plan addressing one or more health concerns.
    
    The CarePlan model captures the intentions of clinicians regarding treatments,
    investigative procedures, and other activities related to patient care.
    """
    id: Optional[str] = None
    status: Optional[str] = None
    intent: Optional[str] = None  # Proposal, plan, order, etc.
    title: Optional[str] = None
    description: Optional[str] = None
    patient_id: Optional[str] = None
    encounter_id: Optional[str] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    created: Optional[datetime] = None
    author: Optional[str] = None
    author_type: Optional[str] = None  # Type of author (practitioner, patient, etc.)
    category: Optional[str] = None
    category_code: Optional[str] = None
    category_system: Optional[str] = None
    care_team_id: Optional[str] = None
    addresses: List[str] = []  # Conditions or problems addressed by this plan
    supports: List[str] = []  # Other related care plans
    goals: List[str] = []  # Goals referenced by this plan
    activities: List[CarePlanActivity] = []
    notes: Optional[str] = None
    identifiers: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}