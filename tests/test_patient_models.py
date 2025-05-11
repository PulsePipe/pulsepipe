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

# tests/test_patient_models.py

from pulsepipe.models import PulseClinicalContent
from pulsepipe.models.patient import PatientInfo

def test_basic_model_instantiation():
    patient = PatientInfo(
        id="123",
        dob_year=1983,
        gender="female",
        identifiers={"mrn": "456789"},
        preferences=[],
        geographic_area="Unknown"
    )

    content = PulseClinicalContent(
        patient=patient,
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

    assert content.patient.id == "123"
    assert content.patient.dob_year == 1983
    assert content.patient.gender == "female"
