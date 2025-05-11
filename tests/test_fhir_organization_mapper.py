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

# tests/test_fhir_organization_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.organization_mapper import OrganizationMapper

def test_organization_mapper_basic():
    # Create a sample FHIR Organization resource
    organization_resource = {
        "resourceType": "Organization",
        "id": "example-hospital",
        "active": True,
        "name": "Memorial Hospital System",
        "alias": ["Memorial Health", "MHS"],
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "prov",
                        "display": "Healthcare Provider"
                    }
                ],
                "text": "Hospital Organization"
            }
        ],
        "address": [
            {
                "line": ["123 Hospital Drive"],
                "city": "Springfield",
                "state": "IL",
                "postalCode": "62701",
                "country": "USA"
            }
        ],
        "partOf": {
            "reference": "Organization/parent-org"
        },
        "telecom": [
            {
                "system": "phone",
                "value": "555-555-5555"
            },
            {
                "system": "email",
                "value": "info@memorialhospital.example.org"
            }
        ],
        "contact": [
            {
                "purpose": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/contactentity-type",
                            "code": "ADMIN",
                            "display": "Administrative"
                        }
                    ]
                },
                "name": {
                    "prefix": ["Ms."],
                    "given": ["Jane"],
                    "family": "Smith",
                    "suffix": ["MBA"]
                },
                "telecom": [
                    {
                        "system": "phone",
                        "value": "555-555-5000"
                    }
                ],
                "address": {
                    "line": ["123 Hospital Drive", "Suite 400"],
                    "city": "Springfield",
                    "state": "IL",
                    "postalCode": "62701",
                    "country": "USA"
                }
            }
        ],
        "identifier": [
            {
                "system": "http://hospital.example.org/organizations",
                "value": "ORG-1"
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = OrganizationMapper()
    
    # Map the resource
    mapper.map(organization_resource, content, cache)
    
    # Verify the mapping
    assert len(content.organizations) == 1
    organization = content.organizations[0]
    
    # Check basic properties
    assert organization.id == "example-hospital"
    assert organization.active is True
    assert organization.name == "Memorial Hospital System"
    
    # Check aliases
    assert len(organization.alias) == 2
    assert "Memorial Health" in organization.alias
    assert "MHS" in organization.alias
    
    # Check type
    assert organization.type == "Hospital Organization"
    assert organization.type_code == "prov"
    assert organization.type_system == "http://terminology.hl7.org/CodeSystem/organization-type"
    
    # Check address
    assert organization.address_line == ["123 Hospital Drive"]
    assert organization.city == "Springfield"
    assert organization.state == "IL"
    assert organization.postal_code == "62701"
    assert organization.country == "USA"
    
    # Check part of
    assert organization.part_of == "parent-org"
    
    # Check telecom
    assert len(organization.telecom) == 2
    assert "555-555-5555" in organization.telecom
    assert "info@memorialhospital.example.org" in organization.telecom
    
    # Check contacts
    assert len(organization.contacts) == 1
    contact = organization.contacts[0]
    assert contact.purpose == "Administrative"
    assert contact.name == "Ms. Jane Smith MBA"
    assert contact.telecom == "555-555-5000"
    assert contact.address_line == ["123 Hospital Drive", "Suite 400"]
    assert contact.city == "Springfield"
    assert contact.state == "IL"
    assert contact.postal_code == "62701"
    assert contact.country == "USA"
    
    # Check identifiers
    assert organization.identifiers["http://hospital.example.org/organizations"] == "ORG-1"