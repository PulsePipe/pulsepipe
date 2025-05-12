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

# tests/test_fhir_medication_request_mapper.py

import pytest
from pulsepipe.ingesters.fhir_utils.medication_request_mapper import MedicationRequestMapper
from pulsepipe.models import PulseClinicalContent, MessageCache, Medication

def test_medication_request_basic_mapping():
    """Test MedicationRequest mapper basic functionality."""
    mapper = MedicationRequestMapper()
    
    # Setup cache with patient and encounter information
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a MedicationRequest resource with CodeableConcept
    med_request = {
        "resourceType": "MedicationRequest",
        "id": "med-request-123",
        "status": "active",
        "intent": "order",
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
        "encounter": {
            "reference": "Encounter/E67890"
        },
        "authoredOn": "2023-01-15T14:30:00Z",
        "dosageInstruction": [
            {
                "text": "Take 2 tablets by mouth every 6 hours as needed for pain",
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
                        "frequency": 1,
                        "period": 6,
                        "periodUnit": "h"
                    },
                    "code": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-TimingEvent",
                                "code": "PRN",
                                "display": "as needed"
                            }
                        ]
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
    mapper.map(med_request, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Tylenol 325mg"
    assert med.code == "1049502"
    assert med.coding_method == "http://www.nlm.nih.gov/research/umls/rxnorm"
    assert med.dose == "2 tablets"
    assert med.route == "Oral"
    assert med.frequency == "as needed"
    assert med.start_date == "2023-01-15T14:30:00Z"
    assert med.status == "active"
    assert med.patient_id == "P12345"
    assert med.encounter_id == "E67890"

def test_medication_request_with_reference():
    """Test MedicationRequest mapper with medication reference."""
    mapper = MedicationRequestMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a MedicationRequest resource with Reference
    med_request_ref = {
        "resourceType": "MedicationRequest",
        "id": "med-request-456",
        "status": "completed",
        "intent": "order",
        "medicationReference": {
            "reference": "Medication/med1",
            "display": "Ibuprofen 400mg"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "encounter": {
            "reference": "Encounter/E67890"
        },
        "authoredOn": "2023-01-10T09:00:00Z",
        "dosageInstruction": [
            {
                "doseAndRate": [
                    {
                        "doseQuantity": {
                            "value": 1,
                            "unit": "tablet"
                        }
                    }
                ],
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
    mapper.map(med_request_ref, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Ibuprofen 400mg"  # From the reference display
    assert med.code is None  # No code in a reference
    assert med.coding_method is None  # No coding method in a reference
    assert med.dose == "1 tablet"
    assert med.frequency == "3"
    assert med.start_date == "2023-01-10T09:00:00Z"
    assert med.status == "completed"
    assert med.patient_id == "P12345"
    assert med.encounter_id == "E67890"

def test_medication_request_validity_period():
    """Test MedicationRequest mapper with validity period for dates."""
    mapper = MedicationRequestMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a MedicationRequest resource with validity period
    med_request_period = {
        "resourceType": "MedicationRequest",
        "id": "med-request-789",
        "status": "active",
        "intent": "order",
        "medicationCodeableConcept": {
            "text": "Metformin 500mg"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "authoredOn": "2023-01-01T10:00:00Z",
        "dispenseRequest": {
            "validityPeriod": {
                "start": "2023-01-02T00:00:00Z",
                "end": "2023-12-31T23:59:59Z"
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
    mapper.map(med_request_period, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Metformin 500mg"
    assert med.start_date == "2023-01-02T00:00:00Z"  # From validity period start
    assert med.end_date == "2023-12-31T23:59:59Z"  # From validity period end
    assert med.patient_id == "P12345"
    
def test_medication_request_with_code_timing():
    """Test MedicationRequest mapper with code-based timing information."""
    mapper = MedicationRequestMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a MedicationRequest with code-based timing (BID = twice daily)
    med_request_coded_timing = {
        "resourceType": "MedicationRequest",
        "id": "med-request-coded",
        "status": "active",
        "intent": "order",
        "medicationCodeableConcept": {
            "text": "Lisinopril 10mg"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "authoredOn": "2023-01-15T10:00:00Z",
        "dosageInstruction": [
            {
                "doseAndRate": [
                    {
                        "doseQuantity": {
                            "value": 1,
                            "unit": "tablet"
                        }
                    }
                ],
                "timing": {
                    "code": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-TimingEvent",
                                "code": "BID",
                                "display": "twice daily"
                            }
                        ],
                        "text": "Take twice daily"
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
    mapper.map(med_request_coded_timing, content, cache)
    
    # Assertions
    assert len(content.medications) == 1
    
    med = content.medications[0]
    assert med.name == "Lisinopril 10mg"
    assert med.dose == "1 tablet"
    assert med.frequency == "Take twice daily"  # Should use the text value since we prioritize it
    assert med.start_date == "2023-01-15T10:00:00Z"