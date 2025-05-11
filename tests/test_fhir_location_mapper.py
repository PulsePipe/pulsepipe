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

# tests/test_fhir_location_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.location_mapper import LocationMapper

def test_location_mapper_basic():
    # Create a sample FHIR Location resource
    location_resource = {
        "resourceType": "Location",
        "id": "example-hospital",
        "status": "active",
        "name": "Memorial Hospital",
        "description": "Main campus of Memorial Hospital",
        "mode": "instance",
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/location-physical-type",
                        "code": "bu",
                        "display": "Building"
                    }
                ],
                "text": "Hospital Building"
            }
        ],
        "physicalType": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/location-physical-type",
                    "code": "bu",
                    "display": "Building"
                }
            ]
        },
        "address": {
            "line": ["123 Hospital Drive"],
            "city": "Springfield",
            "state": "IL",
            "postalCode": "62701",
            "country": "USA"
        },
        "position": {
            "longitude": -89.65,
            "latitude": 39.78,
            "altitude": 180
        },
        "managingOrganization": {
            "reference": "Organization/456"
        },
        "partOf": {
            "reference": "Location/789"
        },
        "operationalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0116",
                    "code": "O",
                    "display": "Operational"
                }
            ]
        },
        "alias": ["Memorial Main Campus", "MMH"],
        "identifier": [
            {
                "system": "http://hospital.example.org/locations",
                "value": "HL-1"
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = LocationMapper()
    
    # Map the resource
    mapper.map(location_resource, content, cache)
    
    # Verify the mapping
    assert len(content.locations) == 1
    location = content.locations[0]
    
    # Check basic properties
    assert location.id == "example-hospital"
    assert location.status == "active"
    assert location.name == "Memorial Hospital"
    assert location.description == "Main campus of Memorial Hospital"
    assert location.mode == "instance"
    
    # Check type
    assert location.type == "Hospital Building"
    assert location.type_code == "bu"
    assert location.type_system == "http://terminology.hl7.org/CodeSystem/location-physical-type"
    
    # Check physical type
    assert location.physical_type == "Building"
    assert location.physical_type_code == "bu"
    assert location.physical_type_system == "http://terminology.hl7.org/CodeSystem/location-physical-type"
    
    # Check address
    assert location.address_line == ["123 Hospital Drive"]
    assert location.city == "Springfield"
    assert location.state == "IL"
    assert location.postal_code == "62701"
    assert location.country == "USA"
    
    # Check position
    assert location.position is not None
    assert location.position.longitude == -89.65
    assert location.position.latitude == 39.78
    assert location.position.altitude == 180
    
    # Check references
    assert location.managing_organization == "456"
    assert location.part_of == "789"
    
    # Check operational status
    assert location.operational_status == "Operational"
    
    # Check aliases
    assert len(location.alias) == 2
    assert "Memorial Main Campus" in location.alias
    assert "MMH" in location.alias
    
    # Check identifiers
    assert location.identifiers["http://hospital.example.org/locations"] == "HL-1"