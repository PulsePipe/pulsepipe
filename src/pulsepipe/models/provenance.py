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

# src/pulsepipe/models/provenance.py

from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class ProvenanceAgent(BaseModel):
    """
    Represents an agent involved in a provenance record.
    
    An agent can be a person, organization, device or software that participated
    in the activity described by the provenance.
    """
    id: Optional[str] = None
    type: Optional[str] = None  # Practitioner, Organization, Device, etc.
    role: Optional[str] = None
    role_code: Optional[str] = None
    role_system: Optional[str] = None
    name: Optional[str] = None
    onBehalfOf: Optional[str] = None
    onBehalfOf_type: Optional[str] = None

class ProvenanceEntity(BaseModel):
    """
    Represents an entity referenced in a provenance record.
    
    This includes information about resources that were used, created, or derived
    as part of the activity described by the provenance.
    """
    id: Optional[str] = None
    role: Optional[str] = None  # e.g., "derivation", "revision", "source"
    type: Optional[str] = None  # Resource type
    reference: Optional[str] = None
    description: Optional[str] = None

class Provenance(BaseModel):
    """
    Represents metadata about the origins, authoring, history, and processing of a resource.
    
    The Provenance model captures information about who created or performed activities
    on resources, when the activities occurred, where they occurred, and why they were performed.
    """
    id: Optional[str] = None
    target_id: Optional[str] = None  # The resource this provenance record is about
    target_type: Optional[str] = None  # The type of resource
    occurred_start: Optional[datetime] = None
    occurred_end: Optional[datetime] = None
    recorded: Optional[datetime] = None  # When the provenance was recorded
    policy: Optional[str] = None  # Policy or plan that authorized the activity
    location: Optional[str] = None  # Where the activity occurred
    reason: Optional[str] = None
    reason_code: Optional[str] = None
    reason_system: Optional[str] = None
    activity: Optional[str] = None  # Activity that occurred
    activity_code: Optional[str] = None
    activity_system: Optional[str] = None
    agents: List[ProvenanceAgent] = []
    entities: List[ProvenanceEntity] = []
    signature: List[Dict[str, Any]] = []  # Digital signatures
    notes: Optional[str] = None
    metadata: Dict[str, Any] = {}