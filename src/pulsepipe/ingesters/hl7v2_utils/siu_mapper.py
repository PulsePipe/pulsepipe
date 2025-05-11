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

# src/pulsepipe/ingesters/hl7v2_utils/siu_mapper.py

from typing import Dict, Any, List, Optional
from pulsepipe.utils.log_factory import LogFactory
from .message import Segment, Message
from .base_mapper import HL7v2Mapper, register_mapper
from pulsepipe.models.appointment import AppointmentInfo, AppointmentParticipant
from pulsepipe.models.clinical_content import PulseClinicalContent

class SIUAppointmentMapper(HL7v2Mapper):
    """
    Mapper for SIU (Schedule Information Unsolicited) HL7 v2.x messages.
    
    Maps the following segments relevant to appointments:
    - AIG: Appointment Information - General Resource
    - AIL: Appointment Information - Location Resource
    - AIP: Appointment Information - Personnel Resource
    """
    def __init__(self):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2 SIUAppointmentMapper")
        
        # Segments related to appointments
        self.appointment_segments = ["SCH", "AIG", "AIL", "AIP"]
        
        # Status mapping based on SIU message type
        self.status_map = {
            "S12": "booked",         # New appointment
            "S13": "booked",         # Appointment modification
            "S14": "cancelled",      # Appointment cancellation
            "S15": "noshow",         # Appointment discontinuation
            "S16": "cancelled",      # Appointment delete
            "S17": "booked",         # Added ancillary service
            "S18": "cancelled",      # Cancelled ancillary service
            "S19": "cancelled",      # Discontinued ancillary service
            "S20": "cancelled",      # Deleted ancillary service
            "S21": "booked",         # Appointment start time changed
            "S22": "waitlist",       # Appointment suspended
            "S23": "booked",         # Appointment resumed
            "S24": "booked",         # Appointment modifications
            "S25": "cancelled",      # Appointment cancelled (insufficient resources)
            "S26": "waitlist"        # Appointment on waiting list
        }
    
    def accepts(self, message: Message) -> bool:
        # Check if this is an SIU message
        if message.msh:
            message_type = message.msh.get(9)
            if message_type and message_type.startswith("SIU"):
                return True
        return False
    
    def map(self, message: Message, content: PulseClinicalContent, cache: Dict[str, Any]):
        try:
            self.logger.debug(f"SIU Message: {message}")
            
            # Extract message type and trigger event
            if not message.msh:
                self.logger.error("No MSH segment found in SIU message")
                return
                
            # Get message type from MSH-9
            # The position might vary based on message structure - we'll try common patterns
            message_type_raw = None

            # Try different positions
            for pos in [9, 8]:
                message_type_raw = message.msh.get(pos)
                if message_type_raw and "SIU" in message_type_raw:
                    break

            if not message_type_raw:
                self.logger.error("No message type found in MSH segment")
                return

            # Parse message type components (e.g., "SIU^S12")
            message_parts = message_type_raw.split("^")
            message_type = message_parts[0] if len(message_parts) > 0 else None
            trigger_event = message_parts[1] if len(message_parts) > 1 else None

            # Fallback to handle cases where SIU is embedded differently
            if message_type != "SIU" and "SIU" in message_type_raw:
                for part in message_parts:
                    if part.startswith("S") and len(part) == 3 and part[1:].isdigit():
                        trigger_event = part
                        message_type = "SIU"
                        break

            if message_type != "SIU" or not trigger_event:
                self.logger.error(f"Invalid SIU message type: {message_type_raw}")
                return
                
            # Extract the SCH (Scheduling) segment
            sch_segment = None
            for segment in message.segments:
                if segment.id == "SCH":
                    sch_segment = segment
                    break
                    
            if not sch_segment:
                self.logger.error("No SCH segment found in SIU message")
                return
            
            # Create an AppointmentInfo object with minimal fields to avoid validation errors
            appointment_id = sch_segment.get(1) or ""  # Placer Appointment ID

            appointment = AppointmentInfo(
                id=appointment_id,
                status=self.status_map.get(trigger_event, "unknown")
            )

            # Set other fields only if they exist
            if sch_segment.get(7):
                appointment.service_type = sch_segment.get(7)  # Appointment Reason
                appointment.appointment_type = sch_segment.get(7)  # Also using Appointment Reason for type

            if sch_segment.get(8):
                appointment.description = sch_segment.get(8)  # Appointment Description

            if sch_segment.get(11):
                appointment.start = self._format_datetime(sch_segment.get(11))  # Start Date/Time

            if sch_segment.get(12):
                appointment.end = self._format_datetime(sch_segment.get(12))  # End Date/Time

            if message.msh and message.msh.get(7):
                appointment.created = self._format_datetime(message.msh.get(7))  # Message timestamp

            # Extract additional data
            location = self._extract_location(message)
            if location:
                appointment.location = location

            patient_id = self._extract_patient_id(message)
            if patient_id:
                appointment.patient_id = patient_id

            encounter_id = self._extract_encounter_id(message)
            if encounter_id:
                appointment.encounter_id = encounter_id

            # Extract participants
            participants = self._extract_participants(message)
            if participants:
                appointment.participants = participants
            
            # Add appointment to content
            if not hasattr(content, 'appointments'):
                content.appointments = []
            content.appointments.append(appointment)
                
            self.logger.info(f"Mapped SIU appointment: {appointment.id}")
            
        except Exception as e:
            self.logger.exception(f"Error mapping SIU appointment: {e}")
    
    def _format_datetime(self, dt_str: Optional[str]) -> Optional[str]:
        """Format HL7 datetime to ISO format if possible"""
        if not dt_str:
            return None

        # Simple conversion for common HL7 datetime format
        # This should be expanded based on actual format requirements
        try:
            # Validate input is a date-like string
            if not dt_str.isalnum() or len(dt_str) < 8:
                return dt_str

            # Extract date part (first 8 chars) and time part if available
            date_part = dt_str[:8]  # YYYYMMDD
            time_part = dt_str[8:14] if len(dt_str) > 8 else "000000"  # HHMMSS

            # Validate date parts are numeric
            if not date_part.isdigit():
                return dt_str

            year = date_part[:4]
            month = date_part[4:6]
            day = date_part[6:8]

            hour = time_part[:2]
            minute = time_part[2:4]
            second = time_part[4:6]

            return f"{year}-{month}-{day}T{hour}:{minute}:{second}"
        except:
            # Return original if parsing fails
            return None
    
    def _extract_location(self, message: Message) -> Optional[str]:
        """Extract appointment location from AIL segment"""
        for segment in message.segments:
            if segment.id == "AIL":
                # AIL-4 typically contains location info
                return segment.get(4)
        return None
    
    def _extract_patient_id(self, message: Message) -> Optional[str]:
        """Extract patient ID from PID segment"""
        for segment in message.segments:
            if segment.id == "PID":
                # PID-3 typically contains patient identifier
                patient_id_raw = segment.get(3)
                if patient_id_raw:
                    # Handle composite IDs (e.g., "12345^MRN^FACILITY")
                    parts = patient_id_raw.split("^")
                    return parts[0] if parts else patient_id_raw
        return None
    
    def _extract_encounter_id(self, message: Message) -> Optional[str]:
        """Extract encounter ID if available"""
        for segment in message.segments:
            if segment.id == "PV1":
                # PV1-19 typically contains visit number
                return segment.get(19)
        return None
    
    def _extract_participants(self, message: Message) -> List[AppointmentParticipant]:
        """Extract appointment participants from AIP segments"""
        participants = []
        
        # Add patient as participant
        patient_id = self._extract_patient_id(message)
        if patient_id:
            patient_name = self._extract_patient_name(message)
            participants.append(AppointmentParticipant(
                id=patient_id,
                type="Patient",
                name=patient_name,
                role="Patient",
                status="accepted"
            ))
        
        # Extract providers/resources from AIP segments
        for segment in message.segments:
            if segment.id == "AIP":
                # Extract provider ID (AIP-3)
                provider_id = segment.get(3)
                if not provider_id:
                    continue
                    
                # Extract provider name (AIP-4)
                provider_name = segment.get(4)
                
                # Extract provider role (AIP-7)
                provider_role = segment.get(7)
                
                participants.append(AppointmentParticipant(
                    id=provider_id,
                    type="Practitioner",
                    name=provider_name,
                    role=provider_role,
                    status="accepted"
                ))
                
        return participants
    
    def _extract_patient_name(self, message: Message) -> Optional[str]:
        """Extract patient name from PID segment"""
        for segment in message.segments:
            if segment.id == "PID":
                # PID-5 contains patient name
                name_raw = segment.get(5)
                if name_raw:
                    # Parse composite name (e.g., "SMITH^JOHN^A")
                    parts = name_raw.split("^")
                    if len(parts) >= 2:
                        # Format as "Given Family"
                        return f"{parts[1]} {parts[0]}"
                    return name_raw
        return None

# Register the mapper
register_mapper(SIUAppointmentMapper())