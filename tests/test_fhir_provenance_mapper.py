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

# tests/test_fhir_provenance_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.provenance_mapper import ProvenanceMapper

def test_provenance_mapper_basic():
    # Create a sample FHIR Provenance resource
    provenance_resource = {
        "resourceType": "Provenance",
        "id": "example",
        "target": [
            {
                "reference": "Observation/blood-pressure",
                "display": "Blood Pressure Measurement"
            }
        ],
        "occurredDateTime": "2022-01-01T10:30:00Z",
        "recorded": "2022-01-01T10:35:00Z",
        "activity": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-DataOperation",
                    "code": "CREATE",
                    "display": "create"
                }
            ]
        },
        "agent": [
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/provenance-participant-type",
                            "code": "author",
                            "display": "Author"
                        }
                    ]
                },
                "role": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "AUT",
                                "display": "author"
                            }
                        ]
                    }
                ],
                "who": {
                    "reference": "Practitioner/123",
                    "display": "Dr. Jane Doe"
                }
            },
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/provenance-participant-type",
                            "code": "custodian",
                            "display": "Custodian"
                        }
                    ]
                },
                "who": {
                    "reference": "Organization/456",
                    "display": "General Hospital"
                }
            }
        ],
        "entity": [
            {
                "role": "source",
                "what": {
                    "reference": "Device/bp-monitor",
                    "display": "Blood Pressure Monitor"
                }
            }
        ],
        "location": {
            "reference": "Location/ward1",
            "display": "Ward 1"
        },
        "reason": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                        "code": "TREAT",
                        "display": "Treatment"
                    }
                ]
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = ProvenanceMapper()
    
    # Map the resource
    mapper.map(provenance_resource, content, cache)
    
    # Verify the mapping
    assert len(content.provenances) == 1
    provenance = content.provenances[0]
    
    # Check basic properties
    assert provenance.id == "example"
    assert provenance.target_id == "blood-pressure"
    assert provenance.target_type == "Observation"
    
    # Check dates
    assert provenance.occurred_start is not None
    assert provenance.recorded is not None
    
    # Check activity
    assert provenance.activity == "create"
    assert provenance.activity_code == "CREATE"
    assert provenance.activity_system == "http://terminology.hl7.org/CodeSystem/v3-DataOperation"
    
    # Check agents
    assert len(provenance.agents) == 2
    
    # Check first agent
    assert provenance.agents[0].id == "123"
    assert provenance.agents[0].type == "Practitioner"
    assert provenance.agents[0].role == "author"
    assert provenance.agents[0].role_code == "AUT"
    assert provenance.agents[0].name == "Dr. Jane Doe"
    
    # Check second agent
    assert provenance.agents[1].id == "456"
    assert provenance.agents[1].type == "Organization"
    assert provenance.agents[1].name == "General Hospital"
    
    # Check entities
    assert len(provenance.entities) == 1
    assert provenance.entities[0].id == "bp-monitor"
    assert provenance.entities[0].type == "Device"
    assert provenance.entities[0].role == "source"
    assert provenance.entities[0].description == "Blood Pressure Monitor"
    
    # Check location
    assert provenance.location == "ward1"
    
    # Check reason
    assert provenance.reason == "Treatment"
    assert provenance.reason_code == "TREAT"
    assert provenance.reason_system == "http://terminology.hl7.org/CodeSystem/v3-ActReason"