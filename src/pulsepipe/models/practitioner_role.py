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

# src/pulsepipe/models/practitioner_role.py

from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from datetime import datetime

class AvailableTime(BaseModel):
    """
    Represents the days and times a practitioner is available.
    """
    days_of_week: List[str] = []
    all_day: Optional[bool] = None
    available_start_time: Optional[str] = None
    available_end_time: Optional[str] = None

class NotAvailable(BaseModel):
    """
    Represents a period when a practitioner is not available.
    """
    description: Optional[str] = None
    during_start: Optional[datetime] = None
    during_end: Optional[datetime] = None

class PractitionerRole(BaseModel):
    """
    Represents a specific role that a practitioner performs at an organization.
    
    This includes information about the practitioner, organization, location,
    healthcare services provided, and availability.
    """
    id: Optional[str] = None
    active: Optional[bool] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    practitioner_id: Optional[str] = None
    organization_id: Optional[str] = None
    code: Optional[str] = None
    code_system: Optional[str] = None
    specialty: List[str] = []
    specialty_codes: List[str] = []
    specialty_system: Optional[str] = None
    location_ids: List[str] = []
    healthcare_service_ids: List[str] = []
    telecom: List[str] = []
    available_time: List[AvailableTime] = []
    not_available: List[NotAvailable] = []
    availability_exceptions: Optional[str] = None
    identifiers: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}