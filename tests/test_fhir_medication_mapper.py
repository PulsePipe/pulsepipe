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

# tests/test_fhir_medication_mapper.py

import pytest
from pulsepipe.ingesters.fhir_utils.medication_mapper import MedicationStatementMapper, MedicationResourceMapper
from pulsepipe.models import PulseClinicalContent, MessageCache, Medication

def test_medication_statement_mapper_basic():
    """Test MedicationStatement mapper basic functionality."""
    mapper = MedicationStatementMapper()
    
    # Setup cache with patient and encounter information
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a MedicationStatement resource
    med_statement = {
        "resourceType": "MedicationStatement",
        "id": "med-statement-123",
        "status": "active",
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
        "dosage": [
            {
                "doseAndRate": [
                    {
                        "doseQuantity": {
                            "value": 2,
                            "unit": "tablets",
                            "system": "http://unitsofmeasure.org",
                            "code": "TAB"
                        }
                    }
                ],
                "route": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "26643006",
                            "display": "Oral route"
                        }
                    ],
                    "text": "Oral"
                },
                "timing": {
                    "repeat": {
                        "frequency": 3,
                        "period": 1,
                        "periodUnit": "d"
                    }
                }
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
    mapper.map(med_statement, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Tylenol 325mg"
    assert med.code == "1049502"
    assert med.coding_method == "http://www.nlm.nih.gov/research/umls/rxnorm"
    assert med.dose == "2 tablets"
    assert med.route == "Oral"
    assert med.frequency == "3"
    assert med.start_date == "2023-01-15T14:30:00Z"
    assert med.status == "active"
    assert med.patient_id == "P12345"
    assert med.encounter_id == "E67890"

def test_medication_statement_with_minimal_data():
    """Test MedicationStatement mapper with minimal data."""
    mapper = MedicationStatementMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a minimal MedicationStatement resource
    minimal_med_statement = {
        "resourceType": "MedicationStatement",
        "id": "med-statement-456",
        "status": "completed",
        "medicationCodeableConcept": {
            "text": "Aspirin 81mg"
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
    mapper.map(minimal_med_statement, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Aspirin 81mg"
    assert med.code is None
    assert med.coding_method is None
    assert med.dose is None
    assert med.route is None
    assert med.frequency is None
    assert med.status == "completed"
    assert med.patient_id == "P12345"  # From cache
    assert med.encounter_id == "E67890"  # From cache

def test_medication_resource_mapper_basic():
    """Test Medication resource mapper basic functionality."""
    mapper = MedicationResourceMapper()
    
    # Create a Medication resource
    medication_resource = {
        "resourceType": "Medication",
        "id": "med-resource-123",
        "code": {
            "coding": [
                {
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": "1234567",
                    "display": "Lisinopril 10 MG Oral Tablet"
                }
            ],
            "text": "Lisinopril 10mg"
        },
        "form": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "385055001",
                    "display": "Tablet"
                }
            ]
        },
        "status": "active",
        "ingredient": [
            {
                "itemCodeableConcept": {
                    "coding": [
                        {
                            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                            "code": "29046",
                            "display": "Lisinopril"
                        }
                    ]
                },
                "strength": {
                    "numerator": {
                        "value": 10,
                        "unit": "mg"
                    },
                    "denominator": {
                        "value": 1,
                        "unit": "tablet"
                    }
                }
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
    
    cache = {}  # No cache needed for standalone medication
    
    # Call the map method
    mapper.map(medication_resource, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Lisinopril 10mg"
    assert med.code == "1234567"
    assert med.coding_method == "http://www.nlm.nih.gov/research/umls/rxnorm"
    assert med.route == "Tablet"
    assert med.status == "active"
    assert med.patient_id is None  # Not associated with a patient
    assert med.encounter_id is None  # Not associated with an encounter
    
    # Check if ingredients were processed
    assert "Ingredients:" in med.notes
    assert "Lisinopril 10 mg/1 tablet" in med.notes

def test_medication_resource_with_minimal_data():
    """Test Medication resource mapper with minimal data."""
    mapper = MedicationResourceMapper()
    
    # Create a minimal Medication resource
    minimal_medication = {
        "resourceType": "Medication",
        "id": "med-resource-456",
        "code": {
            "coding": [
                {
                    "display": "Metoprolol"
                }
            ]
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
    
    cache = {}  # No cache needed
    
    # Call the map method
    mapper.map(minimal_medication, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Metoprolol"
    assert med.code is None
    assert med.coding_method is None
    assert med.route is None
    assert med.status is None
    assert med.notes is None