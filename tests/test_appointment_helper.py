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

# Helper methods for appointment tests

from pulsepipe.models.patient import PatientInfo
from pulsepipe.models.encounter import EncounterInfo, EncounterProvider
from pulsepipe.models.clinical_content import PulseClinicalContent

def create_test_clinical_content():
    """Create a PulseClinicalContent instance for testing with all required fields populated."""
    # Create a patient with minimal required info
    patient = PatientInfo(
        id="test-patient",
        dob_year=1980,
        gender="M", 
        geographic_area="Test Area",
        preferences=None
    )
    
    # Create an encounter
    encounter = EncounterInfo(
        id="test-encounter",
        admit_date=None,
        discharge_date=None,
        encounter_type=None,
        type_coding_method=None,
        location=None,
        reason_code=None,
        reason_coding_method=None,
        visit_type=None,
        patient_id=None,
        providers=[]
    )
    
    # Create and return content
    return PulseClinicalContent(
        patient=patient,
        encounter=encounter
    )