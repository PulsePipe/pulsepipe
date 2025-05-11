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

# src/pulsepipe/models/organization.py

from typing import Optional, Dict, Any, List
from pydantic import BaseModel

class OrganizationContact(BaseModel):
    """
    Represents contact information for an organization.
    """
    purpose: Optional[str] = None
    name: Optional[str] = None
    telecom: Optional[str] = None
    address_line: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None

class Organization(BaseModel):
    """
    Represents a formally or informally recognized grouping of people or organizations.
    
    This includes healthcare providers, departments, insurance companies, and
    other entities involved in healthcare delivery or administration.
    """
    id: Optional[str] = None
    active: Optional[bool] = None
    name: Optional[str] = None
    alias: List[str] = []
    type: Optional[str] = None
    type_code: Optional[str] = None
    type_system: Optional[str] = None
    address_line: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    part_of: Optional[str] = None  # Reference to parent Organization
    telecom: List[str] = []
    contacts: List[OrganizationContact] = []
    identifiers: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}