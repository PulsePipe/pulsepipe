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

# tests/test_fhir_care_plan_mapper.py

import pytest
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.ingesters.fhir_utils.care_plan_mapper import CarePlanMapper

def test_care_plan_mapper_basic():
    # Create a sample FHIR CarePlan resource
    care_plan_resource = {
        "resourceType": "CarePlan",
        "id": "example",
        "status": "active",
        "intent": "plan",
        "title": "Diabetes Management Plan",
        "description": "Plan for managing patient's type 2 diabetes",
        "subject": {
            "reference": "Patient/123"
        },
        "encounter": {
            "reference": "Encounter/456"
        },
        "period": {
            "start": "2022-01-01T00:00:00Z",
            "end": "2022-12-31T23:59:59Z"
        },
        "created": "2022-01-01T09:30:00Z",
        "author": {
            "reference": "Practitioner/789",
            "display": "Dr. John Smith"
        },
        "careTeam": [
            {
                "reference": "CareTeam/team1"
            }
        ],
        "addresses": [
            {
                "reference": "Condition/diabetes",
                "display": "Type 2 Diabetes"
            }
        ],
        "goal": [
            {
                "reference": "Goal/weightloss",
                "display": "Weight loss goal"
            },
            {
                "reference": "Goal/glucosecontrol",
                "display": "Glucose control"
            }
        ],
        "activity": [
            {
                "detail": {
                    "status": "not-started",
                    "description": "Daily blood glucose monitoring",
                    "code": {
                        "coding": [
                            {
                                "system": "http://example.org/fhir/CodeSystem/activity-code",
                                "code": "glucose-monitoring",
                                "display": "Blood glucose monitoring"
                            }
                        ]
                    },
                    "scheduledPeriod": {
                        "start": "2022-01-01T00:00:00Z"
                    }
                }
            },
            {
                "detail": {
                    "status": "not-started",
                    "description": "Diabetic diet plan",
                    "code": {
                        "coding": [
                            {
                                "system": "http://example.org/fhir/CodeSystem/activity-code",
                                "code": "diet-plan",
                                "display": "Diet plan"
                            }
                        ]
                    }
                }
            }
        ],
        "note": [
            {
                "text": "Patient is motivated to manage condition."
            }
        ]
    }
    
    # Create a new clinical content instance with required fields and message cache
    content = PulseClinicalContent(patient=None, encounter=None)
    cache = MessageCache()
    
    # Create a mapper instance
    mapper = CarePlanMapper()
    
    # Map the resource
    mapper.map(care_plan_resource, content, cache)
    
    # Verify the mapping
    assert len(content.care_plans) == 1
    care_plan = content.care_plans[0]
    
    # Check basic properties
    assert care_plan.id == "example"
    assert care_plan.status == "active"
    assert care_plan.intent == "plan"
    assert care_plan.title == "Diabetes Management Plan"
    assert care_plan.description == "Plan for managing patient's type 2 diabetes"
    assert care_plan.patient_id == "123"
    assert care_plan.encounter_id == "456"
    
    # Check period
    assert care_plan.period_start is not None
    assert care_plan.period_end is not None
    
    # Check created date
    assert care_plan.created is not None
    
    # Check author
    assert care_plan.author == "789"
    assert care_plan.author_type == "Practitioner"
    
    # Check care team
    assert care_plan.care_team_id == "team1"
    
    # Check addresses
    assert len(care_plan.addresses) == 1
    assert care_plan.addresses[0] == "diabetes"
    
    # Check goals
    assert len(care_plan.goals) == 2
    assert "weightloss" in care_plan.goals
    assert "glucosecontrol" in care_plan.goals
    
    # Check activities
    assert len(care_plan.activities) == 2
    
    # Check first activity
    activity1 = care_plan.activities[0]
    assert activity1.detail_status == "not-started"
    assert activity1.detail_description == "Daily blood glucose monitoring"
    assert activity1.detail_code == "glucose-monitoring"
    assert activity1.period_start is not None
    
    # Check second activity
    activity2 = care_plan.activities[1]
    assert activity2.detail_status == "not-started"
    assert activity2.detail_description == "Diabetic diet plan"
    assert activity2.detail_code == "diet-plan"
    
    # Check notes
    assert care_plan.notes == "Patient is motivated to manage condition."