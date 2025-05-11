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

# src/pulsepipe/ingesters/fhir_utils/practitioner_mapper.py

from datetime import datetime
from pulsepipe.models.practitioner import Practitioner, PractitionerQualification
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper

@fhir_mapper("Practitioner")
class PractitionerMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Practitioner"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR Practitioner resource to the PulsePipe Practitioner model.
        
        Args:
            resource: The FHIR Practitioner resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        practitioner_id = resource.get("id")
        active = resource.get("active")
        
        # Extract name components
        name_prefix = None
        first_name = None
        middle_name = None
        last_name = None
        name_suffix = None
        full_name = None
        
        if resource.get("name") and len(resource.get("name", [])) > 0:
            name = resource.get("name", [])[0]
            
            # Get prefix
            if name.get("prefix") and len(name.get("prefix", [])) > 0:
                name_prefix = " ".join(name.get("prefix", []))
            
            # Get given names (first and middle)
            if name.get("given") and len(name.get("given", [])) > 0:
                given_names = name.get("given", [])
                if len(given_names) > 0:
                    first_name = given_names[0]
                if len(given_names) > 1:
                    middle_name = " ".join(given_names[1:])
            
            # Get family name (last name)
            last_name = name.get("family")
            
            # Get suffix
            if name.get("suffix") and len(name.get("suffix", [])) > 0:
                name_suffix = " ".join(name.get("suffix", []))
            
            # Assemble full name
            name_parts = []
            if name_prefix:
                name_parts.append(name_prefix)
            if first_name:
                name_parts.append(first_name)
            if middle_name:
                name_parts.append(middle_name)
            if last_name:
                name_parts.append(last_name)
            if name_suffix:
                name_parts.append(name_suffix)
            
            full_name = " ".join(name_parts).strip()
        
        # Extract gender
        gender = resource.get("gender")
        
        # Extract birth date
        birth_date = None
        if resource.get("birthDate"):
            try:
                birth_date_str = resource.get("birthDate")
                birth_date = datetime.fromisoformat(birth_date_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        # Extract address
        address_line = None
        city = None
        state = None
        postal_code = None
        country = None
        if resource.get("address") and len(resource.get("address", [])) > 0:
            address = resource.get("address", [])[0]
            address_line = address.get("line", [])
            city = address.get("city")
            state = address.get("state")
            postal_code = address.get("postalCode")
            country = address.get("country")
        
        # Extract telecom
        telecom = []
        for telecom_item in resource.get("telecom", []):
            value = telecom_item.get("value")
            if value:
                telecom.append(value)
        
        # Extract qualifications
        qualifications = []
        for qual in resource.get("qualification", []):
            code_value = None
            code_system = None
            display = None
            
            if qual.get("code"):
                code = qual.get("code", {})
                codings = code.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    code_value = coding.get("code")
                    code_system = coding.get("system")
                    display = code.get("text") or coding.get("display")
            
            issuer = None
            if qual.get("issuer"):
                issuer_ref = qual.get("issuer", {}).get("reference")
                if issuer_ref:
                    issuer = issuer_ref.split("/")[-1]
            
            identifier = None
            if qual.get("identifier") and len(qual.get("identifier", [])) > 0:
                identifier_obj = qual.get("identifier", [])[0]
                identifier = identifier_obj.get("value")
            
            period_start = None
            period_end = None
            if qual.get("period"):
                period = qual.get("period", {})
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
            
            qualifications.append(PractitionerQualification(
                code=code_value,
                code_system=code_system,
                display=display,
                issuer=issuer,
                identifier=identifier,
                period_start=period_start,
                period_end=period_end
            ))
        
        # Extract communication languages
        communication_languages = []
        for comm in resource.get("communication", []):
            if comm.get("coding") and len(comm.get("coding", [])) > 0:
                coding = comm.get("coding", [])[0]
                lang = comm.get("text") or coding.get("display")
                if lang:
                    communication_languages.append(lang)
        
        # Extract identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value
        
        # Create Practitioner object
        practitioner = Practitioner(
            id=practitioner_id,
            active=active,
            name_prefix=name_prefix,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            name_suffix=name_suffix,
            full_name=full_name,
            gender=gender,
            birth_date=birth_date,
            address_line=address_line,
            city=city,
            state=state,
            postal_code=postal_code,
            country=country,
            telecom=telecom,
            qualifications=qualifications,
            communication_languages=communication_languages,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'practitioners'):
            content.practitioners = []
        content.practitioners.append(practitioner)