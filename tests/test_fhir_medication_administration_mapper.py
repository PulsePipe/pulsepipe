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

# tests/test_fhir_medication_administration_mapper.py

import pytest
from pulsepipe.ingesters.fhir_utils.medication_administration_mapper import MedicationAdministrationMapper
from pulsepipe.models import PulseClinicalContent, MessageCache, MAR

def test_medication_administration_basic_mapping():
    """Test basic medication administration mapping functionality."""
    mapper = MedicationAdministrationMapper()
    
    # Setup cache with patient and encounter information
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Setup a basic MedicationAdministration resource
    med_admin_resource = {
        "resourceType": "MedicationAdministration",
        "id": "med-admin-123",
        "status": "completed",
        "medicationCodeableConcept": {
            "coding": [
                {
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": "1049502",
                    "display": "Acetaminophen 325 MG Oral Tablet"
                }
            ],
            "text": "Tylenol 325mg"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "context": {
            "reference": "Encounter/E67890"
        },
        "effectiveDateTime": "2023-01-15T14:30:00Z",
        "dosage": {
            "dose": {
                "value": 2,
                "unit": "tablets",
                "system": "http://unitsofmeasure.org",
                "code": "TAB"
            },
            "route": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "26643006",
                        "display": "Oral route"
                    }
                ],
                "text": "Oral"
            }
        },
        "performer": [
            {
                "actor": {
                    "reference": "Practitioner/PRACT001",
                    "display": "Nurse Jackie"
                }
            }
        ],
        "note": [
            {
                "text": "Patient reported pain level reduced from 8/10 to 4/10 after administration"
            }
        ]
    }
    
    # Create fresh content container
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
    
    # Call the map method
    mapper.map(med_admin_resource, content, cache)
    
    # Assertions
    assert len(content.mar) == 1
    
    # Get the MAR entry
    mar_entry = content.mar[0]
    
    # Check fields
    assert mar_entry.medication == "Tylenol 325mg"
    assert mar_entry.medication_code == "1049502"
    assert mar_entry.coding_method == "http://www.nlm.nih.gov/research/umls/rxnorm"
    assert mar_entry.dosage == "2 tablets"
    assert mar_entry.route == "Oral route"
    assert mar_entry.administered_at == "2023-01-15T14:30:00Z"
    assert mar_entry.status == "completed"
    assert mar_entry.notes == "Patient reported pain level reduced from 8/10 to 4/10 after administration"
    assert mar_entry.patient_id == "P12345"
    assert mar_entry.encounter_id == "E67890"

def test_medication_administration_with_period():
    """Test medication administration with period instead of dateTime."""
    mapper = MedicationAdministrationMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Setup resource with effectivePeriod instead of effectiveDateTime
    med_admin_resource = {
        "resourceType": "MedicationAdministration",
        "id": "med-admin-456",
        "status": "in-progress",
        "medicationCodeableConcept": {
            "coding": [
                {
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": "313782",
                    "display": "Ibuprofen 400 MG Oral Tablet"
                }
            ],
            "text": "Advil 400mg"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "context": {
            "reference": "Encounter/E67890"
        },
        "effectivePeriod": {
            "start": "2023-01-15T08:00:00Z",
            "end": "2023-01-15T16:00:00Z"
        },
        "dosage": {
            "dose": {
                "value": 1,
                "unit": "tablet",
                "system": "http://unitsofmeasure.org",
                "code": "TAB"
            },
            "route": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "26643006",
                        "display": "Oral route"
                    }
                ]
            }
        }
    }
    
    # Create fresh content container
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
    
    # Call the map method
    mapper.map(med_admin_resource, content, cache)
    
    # Assertions
    assert len(content.mar) == 1
    
    # Get the MAR entry
    mar_entry = content.mar[0]
    
    # Check fields
    assert mar_entry.medication == "Advil 400mg"
    assert mar_entry.medication_code == "313782"
    assert mar_entry.coding_method == "http://www.nlm.nih.gov/research/umls/rxnorm"
    assert mar_entry.dosage == "1 tablet"
    assert mar_entry.route == "Oral route"
    assert mar_entry.administered_at == "2023-01-15T08:00:00Z"  # Should use start time of period
    assert mar_entry.status == "in-progress"
    
def test_medication_administration_minimal_data():
    """Test medication administration mapping with minimal data."""
    mapper = MedicationAdministrationMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Setup minimal resource
    minimal_resource = {
        "resourceType": "MedicationAdministration",
        "id": "med-admin-789",
        "status": "completed",
        "medicationReference": {
            "reference": "Medication/med123",
            "display": "Aspirin 81mg"
        }
    }
    
    # Create fresh content container
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
    
    # Call the map method
    mapper.map(minimal_resource, content, cache)
    
    # Assertions
    assert len(content.mar) == 1
    
    # Get the MAR entry
    mar_entry = content.mar[0]
    
    # Check fields - should fall back to defaults where data is missing
    assert mar_entry.medication == "Aspirin 81mg"
    assert mar_entry.medication_code is None
    assert mar_entry.coding_method is None
    assert mar_entry.dosage == ""  # Required field, set to empty string
    assert mar_entry.route is None
    assert mar_entry.administered_at == ""  # Required field, set to empty string
    assert mar_entry.status == "completed"
    assert mar_entry.patient_id == "P12345"  # From cache
    assert mar_entry.encounter_id == "E67890"  # From cache

def test_medication_administration_with_notes():
    """Test medication administration with multiple notes."""
    mapper = MedicationAdministrationMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Setup resource with multiple notes
    med_admin_resource = {
        "resourceType": "MedicationAdministration",
        "id": "med-admin-notes",
        "status": "completed",
        "medicationCodeableConcept": {
            "text": "Prednisone 10mg"
        },
        "effectiveDateTime": "2023-01-16T09:00:00Z",
        "dosage": {
            "dose": {
                "value": 1,
                "unit": "tablet"
            }
        },
        "note": [
            {
                "text": "Patient complained of bitter taste"
            },
            {
                "text": "Administered with food as directed"
            },
            {
                "text": "Patient tolerated medication well"
            }
        ]
    }
    
    # Create fresh content container
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
    
    # Call the map method
    mapper.map(med_admin_resource, content, cache)
    
    # Assertions
    assert len(content.mar) == 1
    
    # Get the MAR entry
    mar_entry = content.mar[0]
    
    # Check the notes field - should join all notes with semicolons
    assert "Patient complained of bitter taste" in mar_entry.notes
    assert "Administered with food as directed" in mar_entry.notes
    assert "Patient tolerated medication well" in mar_entry.notes
    assert mar_entry.notes == "Patient complained of bitter taste; Administered with food as directed; Patient tolerated medication well"