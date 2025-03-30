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
from pulsepipe.ingesters.fhir_utils.allergy_mapper import allergy_mapper

def test_allergy_mapper_cases():
    # Case 1: Active allergy
    allergy_res = {
        "resourceType": "AllergyIntolerance",
        "clinicalStatus": {"coding": [{"code": "active"}]},
        "code": {"text": "Penicillin"},
        "reaction": [{"description": "Rash", "severity": "mild"}],
        "onsetDateTime": "2020-01-01T00:00:00Z",
        "patient": {"reference": "Patient/patient-1"}
    }
    allergy = allergy_mapper.map_allergy(allergy_res)
    assert allergy.substance == "Penicillin"
    assert allergy.reaction == "Rash"

    # Case 2: No known allergies
    no_allergy_res = {
        "resourceType": "AllergyIntolerance",
        "clinicalStatus": {"coding": [{"code": "inactive"}]},
        "patient": {"reference": "Patient/patient-1"}
    }
    no_allergy = allergy_mapper.map_allergy(no_allergy_res)
    assert no_allergy.substance == "No Known Allergies"
    assert no_allergy.reaction is None

    # Case 3: No AllergyIntolerance present handled at the Bundle level by the ingester
    # No AllergyIntolerance resource = content.allergies == []
