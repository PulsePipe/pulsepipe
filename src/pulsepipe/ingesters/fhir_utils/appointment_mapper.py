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

# src/pulsepipe/ingesters/fhir_utils/appointment_mapper.py

from pulsepipe.models.appointment import AppointmentInfo, AppointmentParticipant
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_coding

@fhir_mapper("Appointment")
class AppointmentMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Appointment"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR Appointment resource to the PulsePipe AppointmentInfo model.
        
        Args:
            resource: The FHIR Appointment resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        appointment_id = resource.get("id")
        status = resource.get("status", "unknown")
        
        # Extract service category
        service_category = None
        service_category_code = None
        service_category_system = None
        
        if resource.get("serviceCategory"):
            coding_data = extract_coding(resource.get("serviceCategory", {}))
            if coding_data:
                service_category = coding_data.get("display")
                service_category_code = coding_data.get("code")
                service_category_system = coding_data.get("system")
                
        # Extract service type
        service_type = None
        service_type_code = None
        service_type_system = None
        
        if resource.get("serviceType"):
            for service_type_obj in resource.get("serviceType", []):
                coding_data = extract_coding(service_type_obj)
                if coding_data:
                    service_type = coding_data.get("display")
                    service_type_code = coding_data.get("code")
                    service_type_system = coding_data.get("system")
                    break
        
        # Extract specialty
        specialty = None
        specialty_code = None
        specialty_system = None
        
        if resource.get("specialty"):
            for specialty_obj in resource.get("specialty", []):
                coding_data = extract_coding(specialty_obj)
                if coding_data:
                    specialty = coding_data.get("display")
                    specialty_code = coding_data.get("code")
                    specialty_system = coding_data.get("system")
                    break
        
        # Extract appointment type
        appointment_type = None
        appointment_type_code = None
        appointment_type_system = None
        
        if resource.get("appointmentType"):
            coding_data = extract_coding(resource.get("appointmentType", {}))
            if coding_data:
                appointment_type = coding_data.get("display")
                appointment_type_code = coding_data.get("code")
                appointment_type_system = coding_data.get("system")
        
        # Extract reason
        reason = None
        reason_code = None
        reason_system = None
        
        if resource.get("reasonCode") and len(resource.get("reasonCode", [])) > 0:
            reason_obj = resource.get("reasonCode", [])[0]
            coding_data = extract_coding(reason_obj)
            if coding_data:
                reason = coding_data.get("display")
                reason_code = coding_data.get("code")
                reason_system = coding_data.get("system")
        
        # Extract priority
        priority = resource.get("priority")
        
        # Extract description
        description = resource.get("description")
        
        # Extract start and end times
        start = resource.get("start")
        end = resource.get("end")
        
        # Extract created time
        created = resource.get("created")
        
        # Extract comments and instructions
        comment = resource.get("comment")
        patient_instruction = resource.get("patientInstruction")
        
        # Extract cancelation reason
        canceled_reason = resource.get("cancelationReason")
        
        # Extract location
        location = None
        if resource.get("participant"):
            for participant in resource.get("participant", []):
                if participant.get("actor", {}).get("reference", "").startswith("Location/"):
                    location = participant.get("actor", {}).get("reference", "").split("/")[-1]
                    break
        
        # Extract participants
        participants = []
        patient_id = None
        encounter_id = None
        
        for participant in resource.get("participant", []):
            # Determine participant type and id
            actor_type = None
            actor_id = None
            
            if participant.get("actor", {}).get("reference"):
                reference = participant.get("actor", {}).get("reference")
                if "/" in reference:
                    parts = reference.split("/")
                    actor_type = parts[0]
                    actor_id = parts[1]
                    
                    # If this is a patient reference, set the patient_id
                    if actor_type == "Patient":
                        patient_id = actor_id
                    # If this is an encounter reference, set the encounter_id
                    elif actor_type == "Encounter":
                        encounter_id = actor_id
            
            # Extract participant name if available
            name = participant.get("actor", {}).get("display")
            
            # Extract role information
            role = None
            role_code = None
            role_system = None
            
            if participant.get("type"):
                for type_obj in participant.get("type", []):
                    coding_data = extract_coding(type_obj)
                    if coding_data:
                        role = coding_data.get("display")
                        role_code = coding_data.get("code")
                        role_system = coding_data.get("system")
                        break
            
            # Extract participant status
            status = participant.get("status")
            
            # Extract period information
            period_start = None
            period_end = None
            
            if participant.get("period"):
                period_start = participant.get("period", {}).get("start")
                period_end = participant.get("period", {}).get("end")
            
            # Create participant object
            participants.append(AppointmentParticipant(
                id=actor_id,
                type=actor_type,
                name=name,
                role=role,
                role_code=role_code,
                role_system=role_system,
                status=status,
                period_start=period_start,
                period_end=period_end
            ))
        
        # Extract requested period
        requested_period_start = None
        requested_period_end = None
        
        if resource.get("requestedPeriod") and len(resource.get("requestedPeriod", [])) > 0:
            period = resource.get("requestedPeriod", [])[0]
            requested_period_start = period.get("start")
            requested_period_end = period.get("end")
        
        # Extract identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value
        
        # Create AppointmentInfo object
        appointment = AppointmentInfo(
            id=appointment_id,
            status=status,
            service_category=service_category,
            service_category_code=service_category_code,
            service_category_system=service_category_system,
            service_type=service_type,
            service_type_code=service_type_code,
            service_type_system=service_type_system,
            specialty=specialty,
            specialty_code=specialty_code,
            specialty_system=specialty_system,
            appointment_type=appointment_type,
            appointment_type_code=appointment_type_code,
            appointment_type_system=appointment_type_system,
            reason=reason,
            reason_code=reason_code,
            reason_system=reason_system,
            priority=priority,
            description=description,
            start=start,
            end=end,
            created=created,
            comment=comment,
            patient_instruction=patient_instruction,
            canceled_reason=canceled_reason,
            location=location,
            participants=participants,
            requested_period_start=requested_period_start,
            requested_period_end=requested_period_end,
            patient_id=patient_id,
            encounter_id=encounter_id,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'appointments'):
            content.appointments = []
        content.appointments.append(appointment)