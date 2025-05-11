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

# tests/test_fhir_allergy_mapper.py

import pytest
from pulsepipe.ingesters.fhir_utils.allergy_mapper import AllergyMapper
from pulsepipe.models import PulseClinicalContent, MessageCache

def test_allergy_mapper_cases():
    mapper = AllergyMapper()

    cache: MessageCache = {
        "patient_id": "C12345",
        "encounter_id": "V09876",
        "order_id": None,
        "resource_index": {},
    }

    # Case 1: Active allergy
    allergy_res = {
        "resourceType": "AllergyIntolerance",
        "clinicalStatus": {"coding": [{"code": "active"}]},
        "code": {"text": "Penicillin"},
        "reaction": [{"description": "Rash", "severity": "mild"}],
        "onsetDateTime": "2020-01-01T00:00:00Z",
        "patient": {"reference": "Patient/patient-1"}
    }

    mapper = AllergyMapper()
    content = PulseClinicalContent(
        patient=None,
        encounter=None,
        vital_signs=[],
        allergies=[],
        immunizations=[],
        diagnoses=[],
        problem_list=[],
        procedures=[],
        medications=[],
        payors=[],
        mar=[],
        notes=[],
        imaging=[],
        lab=[],
        pathology=[],
        diagnostic_test=[],
        microbiology=[],
        blood_bank=[],
        family_history=[],
        social_history=[],
        advance_directives=[],
        functional_status=[],
        order=[],
        implant=[]
    )
    mapper.map(allergy_res, content, cache)
    assert len(content.allergies) == 1
    allergy = content.allergies[0]
    assert allergy.substance == "Penicillin"
    assert allergy.reaction == "Rash"

    # Case 2: No known allergies
    no_allergy_res = {
        "resourceType": "AllergyIntolerance",
        "clinicalStatus": {"coding": [{"code": "inactive"}]},
        "patient": {"reference": "Patient/patient-1"}
    }

    content = PulseClinicalContent(  # fresh content
        patient=None,
        encounter=None,
        vital_signs=[],
        allergies=[],
        immunizations=[],
        diagnoses=[],
        problem_list=[],
        procedures=[],
        medications=[],
        payors=[],
        mar=[],
        notes=[],
        imaging=[],
        lab=[],
        pathology=[],
        diagnostic_test=[],
        microbiology=[],
        blood_bank=[],
        family_history=[],
        social_history=[],
        advance_directives=[],
        functional_status=[],
        order=[],
        implant=[]
    )
    
    mapper = AllergyMapper()
    mapper.map(no_allergy_res, content, cache)
    assert len(content.allergies) == 1
    no_allergy = content.allergies[0]
    assert no_allergy.substance == "No Known Allergies"
    assert no_allergy.reaction is None
