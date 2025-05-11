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

# tests/test_fhir_document_reference_mapper.py

import base64
import pytest
from pulsepipe.ingesters.fhir_utils.document_reference_mapper import DocumentReferenceMapper
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.models.document_reference import DocumentReference, DocumentAuthor

def test_document_reference_mapper_basic():
    mapper = DocumentReferenceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Basic document reference example
    doc_ref_res = {
        "resourceType": "DocumentReference",
        "id": "doc-001",
        "status": "current",
        "description": "Discharge Summary",
        "type": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "18842-5",
                    "display": "Discharge Summary"
                }
            ]
        },
        "category": [
            {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "clinical-note",
                        "display": "Clinical Note"
                    }
                ]
            }
        ],
        "subject": {
            "reference": "Patient/patient123"
        },
        "date": "2025-04-15T12:30:00Z",
        "content": [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "url": "http://example.org/docs/discharge-summary-001.txt"
                }
            }
        ]
    }
    
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
        implant=[],
        document_references=[]
    )
    
    mapper.map(doc_ref_res, content, cache)
    
    assert len(content.document_references) == 1
    doc = content.document_references[0]
    assert doc.document_id == "doc-001"
    assert doc.title == "Discharge Summary"
    assert doc.document_type == "Discharge Summary"
    assert doc.document_class == "Clinical Note"
    assert doc.status == "current"
    assert doc.content_type == "text/plain"
    assert doc.content_url == "http://example.org/docs/discharge-summary-001.txt"
    assert doc.creation_date == "2025-04-15T12:30:00Z"
    assert doc.patient_id == "patient123"

def test_document_reference_mapper_with_inline_content():
    mapper = DocumentReferenceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Sample text content to encode
    sample_text = "Patient was discharged in stable condition. Follow up in 2 weeks."
    encoded_content = base64.b64encode(sample_text.encode("utf-8")).decode("utf-8")
    
    # Document with inline base64 encoded content
    doc_ref_res = {
        "resourceType": "DocumentReference",
        "id": "doc-002",
        "status": "current",
        "description": "Discharge Note",
        "type": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "18842-5",
                    "display": "Discharge Summary"
                }
            ]
        },
        "subject": {
            "reference": "Patient/patient123"
        },
        "date": "2025-04-16T14:30:00Z",
        "content": [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "data": encoded_content
                }
            }
        ]
    }
    
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
        implant=[],
        document_references=[]
    )
    
    mapper.map(doc_ref_res, content, cache)
    
    assert len(content.document_references) == 1
    doc = content.document_references[0]
    assert doc.document_id == "doc-002"
    assert doc.content_type == "text/plain"
    assert doc.content == sample_text  # Should decode the base64 content

def test_document_reference_mapper_with_authors():
    mapper = DocumentReferenceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Document with authors
    doc_ref_res = {
        "resourceType": "DocumentReference",
        "id": "doc-003",
        "status": "current",
        "description": "Consultation Note",
        "type": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "11488-4",
                    "display": "Consultation Note"
                }
            ]
        },
        "subject": {
            "reference": "Patient/patient123"
        },
        "author": [
            {
                "reference": "Practitioner/p1",
                "display": "Dr. Jane Smith"
            },
            {
                "reference": "Practitioner/p2",
                "display": "Dr. John Doe"
            }
        ],
        "date": "2025-04-17T10:00:00Z",
        "content": [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "url": "http://example.org/docs/consult-001.txt"
                }
            }
        ]
    }
    
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
        implant=[],
        document_references=[]
    )
    
    mapper.map(doc_ref_res, content, cache)
    
    assert len(content.document_references) == 1
    doc = content.document_references[0]
    assert doc.document_id == "doc-003"
    assert len(doc.authors) == 2
    
    # Check first author
    author1 = doc.authors[0]
    assert author1.author_id == "p1"
    assert author1.author_name == "Dr. Jane Smith"
    
    # Check second author
    author2 = doc.authors[1]
    assert author2.author_id == "p2"
    assert author2.author_name == "Dr. John Doe"

def test_document_reference_mapper_with_context():
    mapper = DocumentReferenceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Document with context (encounter, facility, department)
    doc_ref_res = {
        "resourceType": "DocumentReference",
        "id": "doc-004",
        "status": "current",
        "description": "Progress Note",
        "type": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "11506-3",
                    "display": "Progress Note"
                }
            ]
        },
        "subject": {
            "reference": "Patient/patient123"
        },
        "context": {
            "encounter": {
                "reference": "Encounter/enc123"
            },
            "facilityType": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "HOSP",
                        "display": "Hospital"
                    }
                ]
            },
            "practiceSetting": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "CARD",
                        "display": "Cardiology"
                    }
                ]
            }
        },
        "date": "2025-04-18T15:30:00Z",
        "content": [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "url": "http://example.org/docs/progress-001.txt"
                }
            }
        ]
    }
    
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
        implant=[],
        document_references=[]
    )
    
    mapper.map(doc_ref_res, content, cache)
    
    assert len(content.document_references) == 1
    doc = content.document_references[0]
    assert doc.document_id == "doc-004"
    assert doc.related_encounters == ["enc123"]
    assert doc.facility == "Hospital"
    assert doc.department == "Cardiology"

def test_document_reference_mapper_with_security_label():
    mapper = DocumentReferenceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Document with security label
    doc_ref_res = {
        "resourceType": "DocumentReference",
        "id": "doc-005",
        "status": "current",
        "description": "Psychiatric Assessment",
        "type": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "47039-3",
                    "display": "Psychiatric Assessment"
                }
            ]
        },
        "subject": {
            "reference": "Patient/patient123"
        },
        "securityLabel": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-Confidentiality",
                        "code": "R",
                        "display": "Restricted"
                    }
                ]
            }
        ],
        "date": "2025-04-19T09:15:00Z",
        "content": [
            {
                "attachment": {
                    "contentType": "text/plain",
                    "url": "http://example.org/docs/psych-001.txt"
                }
            }
        ]
    }
    
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
        implant=[],
        document_references=[]
    )
    
    mapper.map(doc_ref_res, content, cache)
    
    assert len(content.document_references) == 1
    doc = content.document_references[0]
    assert doc.document_id == "doc-005"
    assert doc.security_label == "Restricted"