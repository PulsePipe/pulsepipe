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

# src/pulsepipe/ingesters/fhir_utils/location_mapper.py

from pulsepipe.models.location import Location, LocationPosition
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper

@fhir_mapper("Location")
class LocationMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Location"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR Location resource to the PulsePipe Location model.
        
        Args:
            resource: The FHIR Location resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        location_id = resource.get("id")
        status = resource.get("status")
        name = resource.get("name")
        description = resource.get("description")
        mode = resource.get("mode")
        
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
        
        # Extract physical type
        physical_type = None
        physical_type_code = None
        physical_type_system = None
        if resource.get("physicalType"):
            physical_type_obj = resource.get("physicalType", {})
            codings = physical_type_obj.get("coding", [])
            if codings and len(codings) > 0:
                coding = codings[0]
                physical_type = physical_type_obj.get("text") or coding.get("display")
                physical_type_code = coding.get("code")
                physical_type_system = coding.get("system")
        
        # Extract address
        address_line = None
        city = None
        state = None
        postal_code = None
        country = None
        if resource.get("address"):
            address = resource.get("address", {})
            address_line = address.get("line", [])
            city = address.get("city")
            state = address.get("state")
            postal_code = address.get("postalCode")
            country = address.get("country")
        
        # Extract position
        position = None
        if resource.get("position"):
            pos = resource.get("position", {})
            longitude = pos.get("longitude")
            latitude = pos.get("latitude")
            altitude = pos.get("altitude")
            if longitude is not None or latitude is not None or altitude is not None:
                position = LocationPosition(
                    longitude=longitude,
                    latitude=latitude,
                    altitude=altitude
                )
        
        # Extract managing organization
        managing_organization = None
        if resource.get("managingOrganization"):
            org_ref = resource.get("managingOrganization", {}).get("reference")
            if org_ref:
                managing_organization = org_ref.split("/")[-1]
        
        # Extract part of
        part_of = None
        if resource.get("partOf"):
            part_ref = resource.get("partOf", {}).get("reference")
            if part_ref:
                part_of = part_ref.split("/")[-1]
        
        # Extract operational status
        operational_status = None
        if resource.get("operationalStatus"):
            op_status = resource.get("operationalStatus", {})
            codings = op_status.get("coding", [])
            if codings and len(codings) > 0:
                coding = codings[0]
                operational_status = op_status.get("text") or coding.get("display")
        
        # Extract aliases
        alias = []
        for alias_item in resource.get("alias", []):
            if alias_item:
                alias.append(alias_item)
        
        # Extract identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value
        
        # Create Location object
        location = Location(
            id=location_id,
            status=status,
            name=name,
            description=description,
            mode=mode,
            type=type_value,
            type_code=type_code,
            type_system=type_system,
            physical_type=physical_type,
            physical_type_code=physical_type_code,
            physical_type_system=physical_type_system,
            address_line=address_line,
            city=city,
            state=state,
            postal_code=postal_code,
            country=country,
            position=position,
            managing_organization=managing_organization,
            part_of=part_of,
            operational_status=operational_status,
            alias=alias,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'locations'):
            content.locations = []
        content.locations.append(location)