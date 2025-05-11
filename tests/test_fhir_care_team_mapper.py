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

# tests/test_fhir_care_team_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.care_team_mapper import CareTeamMapper

def test_care_team_mapper_basic():
    # Create a sample FHIR CareTeam resource
    care_team_resource = {
        "resourceType": "CareTeam",
        "id": "example",
        "status": "active",
        "name": "Primary Care Team",
        "subject": {
            "reference": "Patient/123"
        },
        "period": {
            "start": "2022-01-01T00:00:00Z",
            "end": "2022-12-31T23:59:59Z"
        },
        "participant": [
            {
                "role": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/care-team-participant-role",
                                "code": "leader",
                                "display": "Team Leader"
                            }
                        ]
                    }
                ],
                "member": {
                    "reference": "Practitioner/456",
                    "display": "Dr. John Smith"
                }
            },
            {
                "role": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/care-team-participant-role",
                                "code": "member",
                                "display": "Team Member"
                            }
                        ]
                    }
                ],
                "member": {
                    "reference": "Practitioner/789",
                    "display": "Nurse Jane Doe"
                }
            }
        ],
        "note": [
            {
                "text": "Team meets weekly to review patient progress"
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = CareTeamMapper()
    
    # Map the resource
    mapper.map(care_team_resource, content, cache)
    
    # Verify the mapping
    assert len(content.care_teams) == 1
    care_team = content.care_teams[0]
    
    # Check basic properties
    assert care_team.id == "example"
    assert care_team.status == "active"
    assert care_team.name == "Primary Care Team"
    assert care_team.patient_id == "123"
    
    # Check period
    assert care_team.period_start is not None
    assert care_team.period_end is not None
    
    # Check participants
    assert len(care_team.participants) == 2
    
    # Check first participant
    assert care_team.participants[0].role == "Team Leader"
    assert care_team.participants[0].role_code == "leader"
    assert care_team.participants[0].id == "456"
    assert care_team.participants[0].name == "Dr. John Smith"
    
    # Check second participant
    assert care_team.participants[1].role == "Team Member"
    assert care_team.participants[1].role_code == "member"
    assert care_team.participants[1].id == "789"
    assert care_team.participants[1].name == "Nurse Jane Doe"
    
    # Check notes
    assert care_team.notes == "Team meets weekly to review patient progress"