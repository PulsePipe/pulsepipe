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

# src/pulsepipe/ingesters/fhir_utils/device_mapper.py

"""
PulsePipe — Device Mapper for FHIR Resources
"""

from pulsepipe.models.device import Device, DeviceProperty
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("Device")
class DeviceMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Device"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        if not hasattr(content, "devices"):
            setattr(content, "devices", [])
        
        content.devices.append(self.parse_device(resource, cache))
    
    def parse_device(self, resource: dict, cache: MessageCache) -> Device:
        device_id = resource.get("id")
        
        # Extract patient and encounter references
        patient_id = None
        if resource.get("patient", {}).get("reference"):
            patient_id = resource["patient"]["reference"].split("/")[-1]
        else:
            patient_id = extract_patient_reference(resource) or cache.get("patient_id")
            
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract type information
        device_type = None
        if resource.get("type", {}).get("coding"):
            for coding in resource["type"]["coding"]:
                if coding.get("display"):
                    device_type = coding.get("display")
                    break
                elif coding.get("code"):
                    device_type = coding.get("code")
                    break
        
        # Extract manufacturer details
        manufacturer = resource.get("manufacturer")
        model_name = resource.get("modelNumber")
        
        # Extract identifiers such as serial number and lot number
        serial_number = None
        lot_number = None
        for identifier in resource.get("identifier", []):
            if identifier.get("type", {}).get("coding"):
                for coding in identifier["type"]["coding"]:
                    if coding.get("code") == "SNO":
                        serial_number = identifier.get("value")
                    elif coding.get("code") == "LOT":
                        lot_number = identifier.get("value")
        
        # Extract expiration date
        expiration_date = resource.get("expirationDate")
        
        # Extract status
        status = resource.get("status")
        
        # Extract device name
        device_name = resource.get("deviceName", [{}])[0].get("name") if resource.get("deviceName") else None
        
        # Extract version
        version = resource.get("version", [{}])[0].get("value") if resource.get("version") else None
        
        # Extract safety information
        safety_notes = resource.get("note", [{}])[0].get("text") if resource.get("note") else None
        
        # Extract device properties
        properties = []
        for prop in resource.get("property", []):
            property_type = None
            if prop.get("type", {}).get("coding"):
                property_type = prop["type"]["coding"][0].get("display") or prop["type"]["coding"][0].get("code")
            
            # Handle different value formats
            property_value = None
            property_unit = None
            
            if "valueQuantity" in prop:
                property_value = str(prop["valueQuantity"].get("value", ""))
                property_unit = prop["valueQuantity"].get("unit")
            elif "valueCode" in prop:
                property_value = prop["valueCode"].get("text") or prop["valueCode"].get("coding", [{}])[0].get("display") or prop["valueCode"].get("coding", [{}])[0].get("code")
            elif "valueString" in prop:
                property_value = prop["valueString"]
            
            if property_type and property_value:
                properties.append(DeviceProperty(
                    property_type=property_type,
                    property_value=property_value,
                    property_unit=property_unit
                ))
        
        # Create and return the Device object
        return Device(
            device_id=device_id,
            type=device_type,
            manufacturer=manufacturer,
            model=model_name,
            serial_number=serial_number,
            lot_number=lot_number,
            expiration_date=expiration_date,
            status=status,
            name=device_name,
            version=version,
            safety_info=safety_notes,
            properties=properties,
            patient_id=patient_id,
            encounter_id=encounter_id
        )