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

# src/pulsepipe/models/location.py

from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from datetime import datetime

class LocationPosition(BaseModel):
    """
    Represents the geographic position of a location.
    """
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    altitude: Optional[float] = None

class Location(BaseModel):
    """
    Represents a physical place where healthcare services are provided.
    
    This may include rooms, buildings, wards, clinics, hospitals, or 
    other physical locations relevant to healthcare delivery.
    """
    id: Optional[str] = None
    status: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    mode: Optional[str] = None  # instance, kind
    type: Optional[str] = None
    type_code: Optional[str] = None
    type_system: Optional[str] = None
    physical_type: Optional[str] = None
    physical_type_code: Optional[str] = None
    physical_type_system: Optional[str] = None
    address_line: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    position: Optional[LocationPosition] = None
    managing_organization: Optional[str] = None
    part_of: Optional[str] = None
    operational_status: Optional[str] = None
    alias: List[str] = []
    identifiers: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}