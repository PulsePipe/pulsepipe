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

# src/pulsepipe/models/appointment.py

from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class AppointmentParticipant(BaseModel):
    """
    Represents a participant in an appointment, such as a patient, practitioner,
    or related person.
    """
    id: Optional[str] = None
    type: Optional[str] = None
    type_code: Optional[str] = None
    type_system: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    role_code: Optional[str] = None
    role_system: Optional[str] = None
    status: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    
class AppointmentInfo(BaseModel):
    """
    Represents a scheduled appointment for a patient with a practitioner
    or other healthcare service.
    """
    id: Optional[str] = None
    status: Optional[str] = None
    service_category: Optional[str] = None
    service_category_code: Optional[str] = None
    service_category_system: Optional[str] = None
    service_type: Optional[str] = None
    service_type_code: Optional[str] = None
    service_type_system: Optional[str] = None
    specialty: Optional[str] = None
    specialty_code: Optional[str] = None
    specialty_system: Optional[str] = None
    appointment_type: Optional[str] = None
    appointment_type_code: Optional[str] = None
    appointment_type_system: Optional[str] = None
    reason: Optional[str] = None
    reason_code: Optional[str] = None
    reason_system: Optional[str] = None
    priority: Optional[int] = None
    description: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    created: Optional[str] = None
    comment: Optional[str] = None
    patient_instruction: Optional[str] = None
    canceled_reason: Optional[str] = None
    location: Optional[str] = None
    participants: List[AppointmentParticipant] = []
    requested_period_start: Optional[str] = None
    requested_period_end: Optional[str] = None
    patient_id: Optional[str] = None
    encounter_id: Optional[str] = None
    identifiers: Dict[str, str] = {}