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

# tests/test_fhir_practitioner_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.practitioner_mapper import PractitionerMapper

def test_practitioner_mapper_basic():
    # Create a sample FHIR Practitioner resource
    practitioner_resource = {
        "resourceType": "Practitioner",
        "id": "example-doctor",
        "active": True,
        "name": [
            {
                "prefix": ["Dr."],
                "given": ["John", "M"],
                "family": "Smith",
                "suffix": ["MD"]
            }
        ],
        "gender": "male",
        "birthDate": "1970-05-12",
        "address": [
            {
                "line": ["456 Medical Center Blvd"],
                "city": "Springfield",
                "state": "IL",
                "postalCode": "62701",
                "country": "USA"
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": "555-555-1234"
            },
            {
                "system": "email",
                "value": "john.smith@memorialhospital.example.org"
            }
        ],
        "qualification": [
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0360",
                            "code": "MD",
                            "display": "Doctor of Medicine"
                        }
                    ],
                    "text": "Medical Doctor"
                },
                "issuer": {
                    "reference": "Organization/medical-school"
                },
                "identifier": [
                    {
                        "system": "http://example.org/medical-license",
                        "value": "MD12345"
                    }
                ],
                "period": {
                    "start": "2000-01-01",
                    "end": "2030-01-01"
                }
            }
        ],
        "communication": [
            {
                "coding": [
                    {
                        "system": "urn:ietf:bcp:47",
                        "code": "en-US",
                        "display": "English (United States)"
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "urn:ietf:bcp:47",
                        "code": "es",
                        "display": "Spanish"
                    }
                ]
            }
        ],
        "identifier": [
            {
                "system": "http://hospital.example.org/practitioners",
                "value": "PRAC-1"
            },
            {
                "system": "http://nppes.cms.hhs.gov/NPI",
                "value": "1234567890"
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = PractitionerMapper()
    
    # Map the resource
    mapper.map(practitioner_resource, content, cache)
    
    # Verify the mapping
    assert len(content.practitioners) == 1
    practitioner = content.practitioners[0]
    
    # Check basic properties
    assert practitioner.id == "example-doctor"
    assert practitioner.active is True
    assert practitioner.name_prefix == "Dr."
    assert practitioner.first_name == "John"
    assert practitioner.middle_name == "M"
    assert practitioner.last_name == "Smith"
    assert practitioner.name_suffix == "MD"
    assert practitioner.full_name == "Dr. John M Smith MD"
    assert practitioner.gender == "male"
    
    # Check birth date
    assert practitioner.birth_date is not None
    assert practitioner.birth_date.year == 1970
    assert practitioner.birth_date.month == 5
    assert practitioner.birth_date.day == 12
    
    # Check address
    assert practitioner.address_line == ["456 Medical Center Blvd"]
    assert practitioner.city == "Springfield"
    assert practitioner.state == "IL"
    assert practitioner.postal_code == "62701"
    assert practitioner.country == "USA"
    
    # Check telecom
    assert len(practitioner.telecom) == 2
    assert "555-555-1234" in practitioner.telecom
    assert "john.smith@memorialhospital.example.org" in practitioner.telecom
    
    # Check qualifications
    assert len(practitioner.qualifications) == 1
    qualification = practitioner.qualifications[0]
    assert qualification.code == "MD"
    assert qualification.code_system == "http://terminology.hl7.org/CodeSystem/v2-0360"
    assert qualification.display == "Medical Doctor"
    assert qualification.issuer == "medical-school"
    assert qualification.identifier == "MD12345"
    assert qualification.period_start is not None
    assert qualification.period_end is not None
    
    # Check communication languages
    assert len(practitioner.communication_languages) == 2
    assert "English (United States)" in practitioner.communication_languages
    assert "Spanish" in practitioner.communication_languages
    
    # Check identifiers
    assert practitioner.identifiers["http://hospital.example.org/practitioners"] == "PRAC-1"
    assert practitioner.identifiers["http://nppes.cms.hhs.gov/NPI"] == "1234567890"