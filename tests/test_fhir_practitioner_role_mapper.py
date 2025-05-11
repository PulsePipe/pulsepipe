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

# tests/test_fhir_practitioner_role_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.practitioner_role_mapper import PractitionerRoleMapper

def test_practitioner_role_mapper_basic():
    # Create a sample FHIR PractitionerRole resource
    practitioner_role_resource = {
        "resourceType": "PractitionerRole",
        "id": "example-role",
        "active": True,
        "period": {
            "start": "2021-01-01",
            "end": "2023-12-31"
        },
        "practitioner": {
            "reference": "Practitioner/example-doctor"
        },
        "organization": {
            "reference": "Organization/example-hospital"
        },
        "code": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                        "code": "doctor",
                        "display": "Doctor"
                    }
                ]
            }
        ],
        "specialty": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/specialty",
                        "code": "cardio",
                        "display": "Cardiology"
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/specialty",
                        "code": "intmed",
                        "display": "Internal Medicine"
                    }
                ]
            }
        ],
        "location": [
            {
                "reference": "Location/main-hospital"
            },
            {
                "reference": "Location/outpatient-clinic"
            }
        ],
        "healthcareService": [
            {
                "reference": "HealthcareService/cardiology-service"
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": "555-555-2000"
            },
            {
                "system": "email",
                "value": "cardiology@memorialhospital.example.org"
            }
        ],
        "availableTime": [
            {
                "daysOfWeek": ["mon", "tue", "wed"],
                "availableStartTime": "08:00:00",
                "availableEndTime": "17:00:00"
            },
            {
                "daysOfWeek": ["thu", "fri"],
                "availableStartTime": "09:00:00",
                "availableEndTime": "15:00:00"
            }
        ],
        "notAvailable": [
            {
                "description": "Dr. Smith is on vacation",
                "during": {
                    "start": "2023-06-15",
                    "end": "2023-06-30"
                }
            }
        ],
        "availabilityExceptions": "Dr. Smith is not available on holidays",
        "identifier": [
            {
                "system": "http://hospital.example.org/practitioner-roles",
                "value": "ROLE-1"
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = PractitionerRoleMapper()
    
    # Map the resource
    mapper.map(practitioner_role_resource, content, cache)
    
    # Verify the mapping
    assert len(content.practitioner_roles) == 1
    role = content.practitioner_roles[0]
    
    # Check basic properties
    assert role.id == "example-role"
    assert role.active is True
    
    # Check period
    assert role.period_start is not None
    assert role.period_start.year == 2021
    assert role.period_start.month == 1
    assert role.period_start.day == 1
    assert role.period_end is not None
    assert role.period_end.year == 2023
    assert role.period_end.month == 12
    assert role.period_end.day == 31
    
    # Check references
    assert role.practitioner_id == "example-doctor"
    assert role.organization_id == "example-hospital"
    
    # Check code
    assert role.code == "doctor"
    assert role.code_system == "http://terminology.hl7.org/CodeSystem/practitioner-role"
    
    # Check specialty
    assert len(role.specialty) == 2
    assert "Cardiology" in role.specialty
    assert "Internal Medicine" in role.specialty
    assert len(role.specialty_codes) == 2
    assert "cardio" in role.specialty_codes
    assert "intmed" in role.specialty_codes
    assert role.specialty_system == "http://terminology.hl7.org/CodeSystem/specialty"
    
    # Check locations
    assert len(role.location_ids) == 2
    assert "main-hospital" in role.location_ids
    assert "outpatient-clinic" in role.location_ids
    
    # Check healthcare services
    assert len(role.healthcare_service_ids) == 1
    assert "cardiology-service" in role.healthcare_service_ids
    
    # Check telecom
    assert len(role.telecom) == 2
    assert "555-555-2000" in role.telecom
    assert "cardiology@memorialhospital.example.org" in role.telecom
    
    # Check available time
    assert len(role.available_time) == 2
    assert role.available_time[0].days_of_week == ["mon", "tue", "wed"]
    assert role.available_time[0].available_start_time == "08:00:00"
    assert role.available_time[0].available_end_time == "17:00:00"
    assert role.available_time[1].days_of_week == ["thu", "fri"]
    assert role.available_time[1].available_start_time == "09:00:00"
    assert role.available_time[1].available_end_time == "15:00:00"
    
    # Check not available
    assert len(role.not_available) == 1
    assert role.not_available[0].description == "Dr. Smith is on vacation"
    assert role.not_available[0].during_start is not None
    assert role.not_available[0].during_end is not None
    
    # Check availability exceptions
    assert role.availability_exceptions == "Dr. Smith is not available on holidays"
    
    # Check identifiers
    assert role.identifiers["http://hospital.example.org/practitioner-roles"] == "ROLE-1"