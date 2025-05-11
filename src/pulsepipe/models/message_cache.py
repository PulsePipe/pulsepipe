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

# src/pulsepipe/models/message_cache.py

from pydantic import BaseModel
from typing import Optional, Dict, Any

class MessageCache(BaseModel):
    """
    Temporary storage container for message processing state.
    
    This model is used during the ingestion process to maintain context and 
    track relationships between different healthcare data elements as they're 
    being processed. It helps associate resources with their primary identifiers
    and maintains a resource index for efficient lookups.
    
    The cache helps resolve references between different parts of a message,
    especially when processing complex documents like FHIR bundles or HL7 
    messages with multiple segments.
    """
    patient_id: Optional[str] = None
    encounter_id: Optional[str] = None
    order_id: Optional[str] = None
    claim_id: Optional[str] = None
    resource_index: Dict[str, Any] = {}
