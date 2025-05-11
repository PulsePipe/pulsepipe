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

# src/pulsepipe/models/care_team.py

from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class CareTeamParticipant(BaseModel):
    """
    Represents a member of a care team.
    
    This includes the healthcare provider's identifier, role, and related metadata.
    """
    id: Optional[str] = None
    role: Optional[str] = None
    role_code: Optional[str] = None
    role_system: Optional[str] = None
    name: Optional[str] = None
    organization: Optional[str] = None
    member_type: Optional[str] = None  # Type of participant (practitioner, organization, etc.)
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    onBehalfOf: Optional[str] = None

class CareTeam(BaseModel):
    """
    Represents a group of practitioners and organizations who participate in a patient's care.
    
    The CareTeam model captures the members of the healthcare team, the care context,
    and the reason for the team's existence.
    """
    id: Optional[str] = None
    status: Optional[str] = None
    name: Optional[str] = None
    patient_id: Optional[str] = None
    encounter_id: Optional[str] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    category: Optional[str] = None
    category_code: Optional[str] = None
    category_system: Optional[str] = None
    reason: Optional[str] = None
    reason_code: Optional[str] = None
    reason_system: Optional[str] = None
    managing_organization: Optional[str] = None
    participants: List[CareTeamParticipant] = []
    notes: Optional[str] = None
    identifiers: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}