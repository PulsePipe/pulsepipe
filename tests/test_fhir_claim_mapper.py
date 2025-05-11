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

# tests/test_fhir_claim_mapper.py

import pytest
from decimal import Decimal
from datetime import datetime
from pulsepipe.ingesters.fhir_utils.claim_mapper import ClaimMapper
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.models.billing import Claim, Charge

def test_claim_mapper_basic():
    mapper = ClaimMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Basic claim example
    claim_res = {
        "resourceType": "Claim",
        "id": "claim001",
        "status": "active",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                    "code": "institutional",
                    "display": "Institutional"
                }
            ]
        },
        "patient": {
            "reference": "Patient/patient123"
        },
        "billablePeriod": {
            "start": "2025-01-15T00:00:00Z",
            "end": "2025-01-17T00:00:00Z"
        },
        "created": "2025-01-20T12:30:45Z",
        "insurer": {
            "reference": "Organization/payer789"
        },
        "item": [
            {
                "sequence": 1,
                "productOrService": {
                    "coding": [
                        {
                            "system": "http://www.ama-assn.org/go/cpt",
                            "code": "99213",
                            "display": "Office visit, established patient, 15 minutes"
                        }
                    ]
                },
                "unitPrice": {
                    "value": 125.00,
                    "currency": "USD"
                },
                "quantity": {
                    "value": 1
                },
                "diagnosisLinkId": ["1", "2"]
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
        implant=[]
    )
    
    mapper.map(claim_res, content, cache)
    
    # Check if claims list was created
    assert hasattr(content, "claims")
    assert len(content.claims) == 1
    
    # Check basic claim fields
    claim = content.claims[0]
    assert claim.claim_id == "claim001"
    assert claim.patient_id == "patient123"
    assert claim.encounter_id == "encounter456"
    assert claim.claim_status == "accepted"  # "active" maps to "accepted"
    assert claim.claim_type == "institutional"
    assert claim.payer_id == "payer789"
    
    # Check dates
    assert claim.claim_date.isoformat().startswith("2025-01-20T12:30:45")
    assert claim.service_start_date.isoformat().startswith("2025-01-15T00:00:00")
    assert claim.service_end_date.isoformat().startswith("2025-01-17T00:00:00")
    
    # Check charges
    assert len(claim.charges) == 1
    charge = claim.charges[0]
    assert charge.charge_id == "claim001-1"
    assert charge.charge_code == "99213"
    assert charge.charge_description == "Office visit, established patient, 15 minutes"
    assert charge.charge_amount == Decimal("125.00")
    assert charge.quantity == 1
    assert charge.diagnosis_pointers == ["1", "2"]

def test_claim_mapper_multiple_items():
    mapper = ClaimMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Claim with multiple line items
    claim_res = {
        "resourceType": "Claim",
        "id": "claim002",
        "status": "active",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                    "code": "professional",
                    "display": "Professional"
                }
            ]
        },
        "patient": {
            "reference": "Patient/patient123"
        },
        "billablePeriod": {
            "start": "2025-02-10T00:00:00Z"
        },
        "created": "2025-02-15T09:15:30Z",
        "insurer": {
            "reference": "Organization/payer789"
        },
        "item": [
            {
                "sequence": 1,
                "productOrService": {
                    "coding": [
                        {
                            "system": "http://www.ama-assn.org/go/cpt",
                            "code": "80053",
                            "display": "Comprehensive metabolic panel"
                        }
                    ]
                },
                "unitPrice": {
                    "value": 80.00,
                    "currency": "USD"
                },
                "quantity": {
                    "value": 1
                }
            },
            {
                "sequence": 2,
                "productOrService": {
                    "coding": [
                        {
                            "system": "http://www.ama-assn.org/go/cpt",
                            "code": "85025",
                            "display": "Complete blood count (CBC)"
                        }
                    ]
                },
                "unitPrice": {
                    "value": 45.00,
                    "currency": "USD"
                },
                "quantity": {
                    "value": 1
                }
            },
            {
                "sequence": 3,
                "productOrService": {
                    "coding": [
                        {
                            "system": "http://www.ama-assn.org/go/cpt",
                            "code": "36415",
                            "display": "Venipuncture"
                        }
                    ]
                },
                "unitPrice": {
                    "value": 15.00,
                    "currency": "USD"
                },
                "quantity": {
                    "value": 1
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
        implant=[]
    )
    
    mapper.map(claim_res, content, cache)
    
    # Check if claims list was created
    assert hasattr(content, "claims")
    assert len(content.claims) == 1
    
    # Check basic claim fields
    claim = content.claims[0]
    assert claim.claim_id == "claim002"
    assert claim.claim_type == "professional"
    
    # Check total amount
    assert claim.total_charge_amount == Decimal("140.00")  # 80 + 45 + 15
    
    # Check charges
    assert len(claim.charges) == 3
    
    # Check first charge
    charge1 = claim.charges[0]
    assert charge1.charge_id == "claim002-1"
    assert charge1.charge_code == "80053"
    assert charge1.charge_description == "Comprehensive metabolic panel"
    assert charge1.charge_amount == Decimal("80.00")
    
    # Check second charge
    charge2 = claim.charges[1]
    assert charge2.charge_id == "claim002-2"
    assert charge2.charge_code == "85025"
    assert charge2.charge_description == "Complete blood count (CBC)"
    assert charge2.charge_amount == Decimal("45.00")
    
    # Check third charge
    charge3 = claim.charges[2]
    assert charge3.charge_id == "claim002-3"
    assert charge3.charge_code == "36415"
    assert charge3.charge_description == "Venipuncture"
    assert charge3.charge_amount == Decimal("15.00")

def test_claim_mapper_different_status():
    mapper = ClaimMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Test different status mappings
    statuses_to_test = [
        {"fhir_status": "active", "expected_status": "accepted"},
        {"fhir_status": "cancelled", "expected_status": "denied"},
        {"fhir_status": "entered-in-error", "expected_status": "denied"},
        {"fhir_status": "draft", "expected_status": "submitted"},  # Default
        {"fhir_status": "", "expected_status": "submitted"},  # Default
    ]
    
    for status_test in statuses_to_test:
        # Claim with the status to test
        claim_res = {
            "resourceType": "Claim",
            "id": f"claim-{status_test['fhir_status']}",
            "status": status_test["fhir_status"],
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                        "code": "institutional"
                    }
                ]
            },
            "patient": {
                "reference": "Patient/patient123"
            },
            "created": "2025-03-15T12:00:00Z",
            "item": [
                {
                    "sequence": 1,
                    "productOrService": {
                        "coding": [
                            {
                                "code": "99213"
                            }
                        ]
                    },
                    "unitPrice": {
                        "value": 100.00
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
            implant=[]
        )
        
        mapper.map(claim_res, content, cache)
        
        # Check if status was mapped correctly
        assert content.claims[0].claim_status == status_test["expected_status"]

def test_claim_mapper_claim_types():
    mapper = ClaimMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Test different claim types
    types_to_test = [
        {"fhir_type": "institutional", "expected_type": "institutional"},
        {"fhir_type": "professional", "expected_type": "professional"},
        {"fhir_type": "oral", "expected_type": "dental"},
        {"fhir_type": "pharmacy", "expected_type": None},  # Not mapped
    ]
    
    for type_test in types_to_test:
        # Claim with the type to test
        claim_res = {
            "resourceType": "Claim",
            "id": f"claim-{type_test['fhir_type']}",
            "status": "active",
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                        "code": type_test["fhir_type"]
                    }
                ]
            },
            "patient": {
                "reference": "Patient/patient123"
            },
            "created": "2025-03-15T12:00:00Z",
            "item": [
                {
                    "sequence": 1,
                    "productOrService": {
                        "coding": [
                            {
                                "code": "99213"
                            }
                        ]
                    },
                    "unitPrice": {
                        "value": 100.00
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
            implant=[]
        )
        
        mapper.map(claim_res, content, cache)
        
        # Check if type was mapped correctly
        assert content.claims[0].claim_type == type_test["expected_type"]

def test_claim_mapper_multiple_quantities():
    mapper = ClaimMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Claim with item having quantity > 1
    claim_res = {
        "resourceType": "Claim",
        "id": "claim-quantity",
        "status": "active",
        "type": {
            "coding": [
                {
                    "code": "professional"
                }
            ]
        },
        "patient": {
            "reference": "Patient/patient123"
        },
        "created": "2025-04-15T12:00:00Z",
        "item": [
            {
                "sequence": 1,
                "productOrService": {
                    "coding": [
                        {
                            "code": "J7050",
                            "display": "Normal saline solution 250ml"
                        }
                    ]
                },
                "unitPrice": {
                    "value": 20.00
                },
                "quantity": {
                    "value": 4
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
        implant=[]
    )
    
    mapper.map(claim_res, content, cache)
    
    # Check quantity and calculated amount
    charge = content.claims[0].charges[0]
    assert charge.quantity == 4
    assert charge.charge_amount == Decimal("80.00")  # 4 * 20.00
    assert content.claims[0].total_charge_amount == Decimal("80.00")