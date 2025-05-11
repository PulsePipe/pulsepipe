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

# src/pulsepipe/ingesters/fhir_utils/organization_mapper.py

from pulsepipe.models.organization import Organization, OrganizationContact
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper

@fhir_mapper("Organization")
class OrganizationMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Organization"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR Organization resource to the PulsePipe Organization model.
        
        Args:
            resource: The FHIR Organization resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        organization_id = resource.get("id")
        active = resource.get("active")
        name = resource.get("name")
        
        # Extract aliases
        alias = []
        for alias_item in resource.get("alias", []):
            if alias_item:
                alias.append(alias_item)
        
        # Extract type
        type_value = None
        type_code = None
        type_system = None
        if resource.get("type"):
            for type_obj in resource.get("type", []):
                codings = type_obj.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    type_value = type_obj.get("text") or coding.get("display")
                    type_code = coding.get("code")
                    type_system = coding.get("system")
                    break
        
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
        
        # Extract part of
        part_of = None
        if resource.get("partOf"):
            part_ref = resource.get("partOf", {}).get("reference")
            if part_ref:
                part_of = part_ref.split("/")[-1]
        
        # Extract telecom
        telecom = []
        for telecom_item in resource.get("telecom", []):
            value = telecom_item.get("value")
            if value:
                telecom.append(value)
        
        # Extract contacts
        contacts = []
        for contact in resource.get("contact", []):
            purpose_value = None
            if contact.get("purpose"):
                purpose_obj = contact.get("purpose", {})
                codings = purpose_obj.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    purpose_value = purpose_obj.get("text") or coding.get("display")
            
            name_value = None
            if contact.get("name"):
                name_obj = contact.get("name", {})
                name_parts = []
                prefix = " ".join(name_obj.get("prefix", []))
                given = " ".join(name_obj.get("given", []))
                family = name_obj.get("family", "")
                suffix = " ".join(name_obj.get("suffix", []))
                
                if prefix:
                    name_parts.append(prefix)
                if given:
                    name_parts.append(given)
                if family:
                    name_parts.append(family)
                if suffix:
                    name_parts.append(suffix)
                
                name_value = " ".join(name_parts).strip()
            
            telecom_value = None
            if contact.get("telecom") and len(contact.get("telecom", [])) > 0:
                telecom_value = contact.get("telecom", [])[0].get("value")
            
            contact_address_line = None
            contact_city = None
            contact_state = None
            contact_postal_code = None
            contact_country = None
            if contact.get("address"):
                address = contact.get("address", {})
                contact_address_line = address.get("line", [])
                contact_city = address.get("city")
                contact_state = address.get("state")
                contact_postal_code = address.get("postalCode")
                contact_country = address.get("country")
            
            contacts.append(OrganizationContact(
                purpose=purpose_value,
                name=name_value,
                telecom=telecom_value,
                address_line=contact_address_line,
                city=contact_city,
                state=contact_state,
                postal_code=contact_postal_code,
                country=contact_country
            ))
        
        # Extract identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value
        
        # Create Organization object
        organization = Organization(
            id=organization_id,
            active=active,
            name=name,
            alias=alias,
            type=type_value,
            type_code=type_code,
            type_system=type_system,
            address_line=address_line,
            city=city,
            state=state,
            postal_code=postal_code,
            country=country,
            part_of=part_of,
            telecom=telecom,
            contacts=contacts,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'organizations'):
            content.organizations = []
        content.organizations.append(organization)