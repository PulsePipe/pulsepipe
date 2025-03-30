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
import json
from pulsepipe.ingesters.fhir_ingester import FHIRIngester


def test_parse_bundle_with_multiple_resources():
    ingester = FHIRIngester()
    bundle = {
        "resourceType": "Bundle",
        "entry": [
            {"resource": {"resourceType": "Patient", "id": "patient-1"}},
            {"resource": {"resourceType": "Encounter", "id": "enc-1"}},
            {"resource": {"resourceType": "AllergyIntolerance", "id": "allergy-1"}},
            {"resource": {"resourceType": "Observation", "id": "obs-1", "category": [{"coding": [{"code": "vital-signs"}]}]}},
            {"resource": {"resourceType": "Observation", "id": "obs-2", "category": [{"coding": [{"code": "laboratory"}]}]}},
            {"resource": {"resourceType": "Observation", "id": "obs-3", "category": [{"coding": [{"code": "imaging"}]}]}}
        ]
    }
    content = ingester.parse(json.dumps(bundle))

    assert content.patient is not None
    assert content.encounter is not None
    assert len(content.allergies) == 1
    assert len(content.vital_signs) == 1
    assert len(content.lab) == 1
    assert len(content.imaging) == 1
