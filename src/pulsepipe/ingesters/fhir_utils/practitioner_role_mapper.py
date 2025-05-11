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

# src/pulsepipe/ingesters/fhir_utils/practitioner_role_mapper.py

from datetime import datetime
from pulsepipe.models.practitioner_role import PractitionerRole, AvailableTime, NotAvailable
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper

@fhir_mapper("PractitionerRole")
class PractitionerRoleMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "PractitionerRole"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR PractitionerRole resource to the PulsePipe PractitionerRole model.
        
        Args:
            resource: The FHIR PractitionerRole resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        role_id = resource.get("id")
        active = resource.get("active")
        
        # Extract period
        period_start = None
        period_end = None
        if resource.get("period"):
            period = resource.get("period", {})
            start_str = period.get("start")
            end_str = period.get("end")
            
            if start_str:
                try:
                    period_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
            
            if end_str:
                try:
                    period_end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
        
        # Extract practitioner reference
        practitioner_id = None
        if resource.get("practitioner"):
            practitioner_ref = resource.get("practitioner", {}).get("reference")
            if practitioner_ref:
                practitioner_id = practitioner_ref.split("/")[-1]
        
        # Extract organization reference
        organization_id = None
        if resource.get("organization"):
            organization_ref = resource.get("organization", {}).get("reference")
            if organization_ref:
                organization_id = organization_ref.split("/")[-1]
        
        # Extract code
        code = None
        code_system = None
        if resource.get("code"):
            for code_obj in resource.get("code", []):
                codings = code_obj.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    code = coding.get("code")
                    code_system = coding.get("system")
                    break
        
        # Extract specialty
        specialty = []
        specialty_codes = []
        specialty_system = None
        if resource.get("specialty"):
            for spec_obj in resource.get("specialty", []):
                codings = spec_obj.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    spec_text = spec_obj.get("text") or coding.get("display")
                    spec_code = coding.get("code")
                    
                    if spec_text:
                        specialty.append(spec_text)
                    if spec_code:
                        specialty_codes.append(spec_code)
                    
                    # Get system from first specialty coding
                    if not specialty_system and coding.get("system"):
                        specialty_system = coding.get("system")
        
        # Extract location references
        location_ids = []
        for location in resource.get("location", []):
            location_ref = location.get("reference")
            if location_ref:
                location_id = location_ref.split("/")[-1]
                location_ids.append(location_id)
        
        # Extract healthcare service references
        healthcare_service_ids = []
        for service in resource.get("healthcareService", []):
            service_ref = service.get("reference")
            if service_ref:
                service_id = service_ref.split("/")[-1]
                healthcare_service_ids.append(service_id)
        
        # Extract telecom
        telecom = []
        for telecom_item in resource.get("telecom", []):
            value = telecom_item.get("value")
            if value:
                telecom.append(value)
        
        # Extract available time
        available_time = []
        for avail_time in resource.get("availableTime", []):
            days_of_week = avail_time.get("daysOfWeek", [])
            all_day = avail_time.get("allDay")
            available_start_time = avail_time.get("availableStartTime")
            available_end_time = avail_time.get("availableEndTime")
            
            available_time.append(AvailableTime(
                days_of_week=days_of_week,
                all_day=all_day,
                available_start_time=available_start_time,
                available_end_time=available_end_time
            ))
        
        # Extract not available
        not_available = []
        for not_avail in resource.get("notAvailable", []):
            description = not_avail.get("description")
            during_start = None
            during_end = None
            
            if not_avail.get("during"):
                during = not_avail.get("during", {})
                start_str = during.get("start")
                end_str = during.get("end")
                
                if start_str:
                    try:
                        during_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        pass
                
                if end_str:
                    try:
                        during_end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        pass
            
            not_available.append(NotAvailable(
                description=description,
                during_start=during_start,
                during_end=during_end
            ))
        
        # Extract availability exceptions
        availability_exceptions = resource.get("availabilityExceptions")
        
        # Extract identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value
        
        # Create PractitionerRole object
        practitioner_role = PractitionerRole(
            id=role_id,
            active=active,
            period_start=period_start,
            period_end=period_end,
            practitioner_id=practitioner_id,
            organization_id=organization_id,
            code=code,
            code_system=code_system,
            specialty=specialty,
            specialty_codes=specialty_codes,
            specialty_system=specialty_system,
            location_ids=location_ids,
            healthcare_service_ids=healthcare_service_ids,
            telecom=telecom,
            available_time=available_time,
            not_available=not_available,
            availability_exceptions=availability_exceptions,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'practitioner_roles'):
            content.practitioner_roles = []
        content.practitioner_roles.append(practitioner_role)