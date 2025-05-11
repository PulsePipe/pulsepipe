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

# src/pulsepipe/models/practitioner.py

from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from datetime import datetime

class PractitionerQualification(BaseModel):
    """
    Represents a certification, license, or education qualification of a practitioner.
    """
    code: Optional[str] = None
    code_system: Optional[str] = None
    display: Optional[str] = None
    issuer: Optional[str] = None
    identifier: Optional[str] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None

class Practitioner(BaseModel):
    """
    Represents a person who provides healthcare services.
    
    This includes physicians, nurses, technicians, and other healthcare professionals
    involved in patient care.
    """
    id: Optional[str] = None
    active: Optional[bool] = None
    name_prefix: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    name_suffix: Optional[str] = None
    full_name: Optional[str] = None  # Convenience field with assembled name
    gender: Optional[str] = None
    birth_date: Optional[datetime] = None
    address_line: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    telecom: List[str] = []
    qualifications: List[PractitionerQualification] = []
    communication_languages: List[str] = []
    identifiers: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}