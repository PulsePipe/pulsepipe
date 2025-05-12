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

# tests/test_fhir_procedure_mapper.py

import pytest
from pulsepipe.ingesters.fhir_utils.procedure_mapper import ProcedureMapper
from pulsepipe.models import PulseClinicalContent, MessageCache, Procedure, ProcedureProvider

def test_procedure_mapper_basic():
    """Test basic procedure mapping functionality."""
    mapper = ProcedureMapper()
    
    # Setup cache with patient and encounter information
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a basic Procedure resource
    procedure_resource = {
        "resourceType": "Procedure",
        "id": "proc-123",
        "status": "completed",
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "80146002",
                    "display": "Appendectomy"
                }
            ],
            "text": "Appendectomy"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "encounter": {
            "reference": "Encounter/E67890"
        },
        "performedDateTime": "2023-01-15T14:30:00Z",
        "performer": [
            {
                "actor": {
                    "reference": "Practitioner/PRACT001",
                    "display": "Dr. Jane Smith"
                },
                "function": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0443",
                            "code": "PP",
                            "display": "Primary Surgeon"
                        }
                    ]
                }
            },
            {
                "actor": {
                    "reference": "Practitioner/PRACT002",
                    "display": "Dr. John Doe"
                },
                "function": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0443",
                            "code": "AS",
                            "display": "Assistant Surgeon"
                        }
                    ]
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
    mapper.map(procedure_resource, content, cache)
    
    # Assertions
    assert len(content.procedures) == 1
    
    procedure = content.procedures[0]
    assert procedure.code == "80146002"
    assert procedure.coding_method == "http://snomed.info/sct"
    assert procedure.description == "Appendectomy"
    assert procedure.performed_date == "2023-01-15T14:30:00Z"
    assert procedure.status == "completed"
    assert procedure.patient_id == "P12345"
    assert procedure.encounter_id == "E67890"
    
    # Check procedure providers
    assert len(procedure.providers) == 2
    
    # First provider
    assert procedure.providers[0].provider_id == "PRACT001" 
    assert procedure.providers[0].role == "Primary Surgeon"
    
    # Second provider
    assert procedure.providers[1].provider_id == "PRACT002"
    assert procedure.providers[1].role == "Assistant Surgeon"

def test_procedure_with_period():
    """Test procedure mapping with performedPeriod instead of performedDateTime."""
    mapper = ProcedureMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a Procedure resource with period
    procedure_with_period = {
        "resourceType": "Procedure",
        "id": "proc-456",
        "status": "in-progress",
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "80622008",
                    "display": "Coronary artery bypass graft"
                }
            ],
            "text": "CABG"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "performedPeriod": {
            "start": "2023-01-10T09:00:00Z",
            "end": "2023-01-10T14:30:00Z"
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
    mapper.map(procedure_with_period, content, cache)
    
    # Assertions
    assert len(content.procedures) == 1
    
    procedure = content.procedures[0]
    assert procedure.code == "80622008"
    assert procedure.description == "CABG"
    assert procedure.performed_date == "2023-01-10T09:00:00Z"  # Should use start time
    assert procedure.status == "in-progress"
    assert procedure.patient_id == "P12345"
    assert procedure.encounter_id == cache["encounter_id"]  # Should use cache

def test_procedure_with_performer_function_text():
    """Test procedure mapping with text-based performer function."""
    mapper = ProcedureMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a Procedure resource with text-based function
    procedure_with_text_function = {
        "resourceType": "Procedure",
        "id": "proc-789",
        "status": "completed",
        "code": {
            "text": "IV Insertion"
        },
        "subject": {
            "reference": "Patient/P12345"
        },
        "performedDateTime": "2023-01-20T10:15:00Z",
        "performer": [
            {
                "actor": {
                    "reference": "Practitioner/NURSE001"
                },
                "function": {
                    "text": "IV Specialist"
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
    mapper.map(procedure_with_text_function, content, cache)
    
    # Assertions
    assert len(content.procedures) == 1
    
    procedure = content.procedures[0]
    assert procedure.description == "IV Insertion"
    assert procedure.performed_date == "2023-01-20T10:15:00Z"
    
    # Check procedure provider
    assert len(procedure.providers) == 1
    assert procedure.providers[0].provider_id == "NURSE001"
    assert procedure.providers[0].role == "IV Specialist"

def test_procedure_minimal_data():
    """Test procedure mapping with minimal data."""
    mapper = ProcedureMapper()
    
    # Setup cache
    cache = {
        "patient_id": "P12345",
        "encounter_id": "E67890",
        "order_id": None,
        "resource_index": {},
    }
    
    # Create a minimal Procedure resource
    minimal_procedure = {
        "resourceType": "Procedure",
        "id": "proc-minimal",
        "status": "unknown",
        "code": {
            "text": "Unspecified procedure"
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
    mapper.map(minimal_procedure, content, cache)
    
    # Assertions
    assert len(content.procedures) == 1
    
    procedure = content.procedures[0]
    assert procedure.code is None
    assert procedure.coding_method is None
    assert procedure.description == "Unspecified procedure"
    assert procedure.performed_date is None
    assert procedure.status == "unknown"
    assert procedure.patient_id == "P12345"  # From cache
    assert procedure.encounter_id == "E67890"  # From cache
    assert procedure.providers == []  # Empty list, no providers