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

# tests/test_fhir_explanation_of_benefit_mapper.py

import pytest
from decimal import Decimal
from datetime import datetime
from pulsepipe.ingesters.fhir_utils.explanation_of_benefit_mapper import ExplanationOfBenefitMapper
from pulsepipe.ingesters.fhir_utils.claim_mapper import ClaimMapper
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.models.billing import Claim, Payment, Adjustment

def test_explanation_of_benefit_mapper_basic():
    mapper = ExplanationOfBenefitMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Basic EOB example
    eob_res = {
        "resourceType": "ExplanationOfBenefit",
        "id": "eob001",
        "status": "active",
        "patient": {
            "reference": "Patient/patient123"
        },
        "claim": {
            "reference": "Claim/claim001"
        },
        "insurer": {
            "reference": "Organization/payer789"
        },
        "outcome": "complete",
        "payment": {
            "date": "2025-02-15T00:00:00Z",
            "amount": {
                "value": 100.00,
                "currency": "USD"
            },
            "identifier": {
                "value": "CHK12345"
            }
        },
        "total": [
            {
                "category": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/adjudication",
                            "code": "deductible",
                            "display": "Deductible"
                        }
                    ]
                },
                "amount": {
                    "value": 20.00,
                    "currency": "USD"
                }
            },
            {
                "category": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/adjudication",
                            "code": "copay",
                            "display": "Copay"
                        }
                    ]
                },
                "amount": {
                    "value": 5.00,
                    "currency": "USD"
                }
            }
        ]
    }
    
    # Create a clinical content with an existing claim
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
        claims=[
            Claim(
                claim_id="claim001",
                patient_id="patient123",
                encounter_id="encounter456",
                claim_date=datetime.fromisoformat("2025-01-15T00:00:00+00:00"),
                payer_id="payer789",
                total_charge_amount=Decimal("125.00"),
                total_payment_amount=Decimal("0.00"),
                claim_status="accepted",
                claim_type="professional",
                # Add required fields to fix validation errors
                service_start_date=datetime.fromisoformat("2025-01-15T00:00:00+00:00"),
                service_end_date=datetime.fromisoformat("2025-01-15T00:00:00+00:00"),
                charges=[],
                organization_id="org123",
                payments=[],
                adjustments=[]
            )
        ]
    )
    
    mapper.map(eob_res, content, cache)
    
    # Check that the claim was found and updated
    assert len(content.claims) == 1
    claim = content.claims[0]
    
    # Check updated claim status
    assert claim.claim_status == "paid"
    
    # Check payment was added
    assert len(claim.payments) == 1
    payment = claim.payments[0]
    assert payment.payment_id == "eob001-payment"
    assert payment.payment_amount == Decimal("100.00")
    assert payment.payment_date.isoformat().startswith("2025-02-15T00:00:00")
    assert payment.check_number == "CHK12345"
    
    # Check that claim total payment was updated
    assert claim.total_payment_amount == Decimal("100.00")
    
    # Check adjustments were added
    assert len(claim.adjustments) == 2
    
    # Check deductible adjustment
    deductible_adj = next((adj for adj in claim.adjustments if adj.adjustment_reason_code == "deductible"), None)
    assert deductible_adj is not None
    assert deductible_adj.adjustment_amount == Decimal("20.00")
    
    # Check copay adjustment
    copay_adj = next((adj for adj in claim.adjustments if adj.adjustment_reason_code == "copay"), None)
    assert copay_adj is not None
    assert copay_adj.adjustment_amount == Decimal("5.00")

def test_explanation_of_benefit_mapper_no_existing_claim():
    mapper = ExplanationOfBenefitMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # EOB for a claim that doesn't exist in the content
    eob_res = {
        "resourceType": "ExplanationOfBenefit",
        "id": "eob002",
        "status": "active",
        "patient": {
            "reference": "Patient/patient123"
        },
        "claim": {
            "reference": "Claim/claim002"
        },
        "insurer": {
            "reference": "Organization/payer789"
        },
        "outcome": "complete",
        "payment": {
            "date": "2025-03-10T00:00:00Z",
            "amount": {
                "value": 75.50,
                "currency": "USD"
            }
        }
    }
    
    # Create content without existing claims
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
    
    mapper.map(eob_res, content, cache)
    
    # Check that a new claim was created
    assert hasattr(content, "claims")
    assert len(content.claims) == 1
    
    # Check claim values
    claim = content.claims[0]
    assert claim.claim_id == "claim002"
    assert claim.patient_id == "patient123"
    assert claim.encounter_id == "encounter456"
    assert claim.payer_id == "payer789"
    assert claim.claim_status == "paid"  # "active" EOB status maps to "paid"
    assert claim.total_payment_amount == Decimal("75.50")
    
    # Check payment was created
    assert len(claim.payments) == 1
    payment = claim.payments[0]
    assert payment.payment_id == "eob002-payment"
    assert payment.payment_amount == Decimal("75.50")
    assert payment.payment_date.isoformat().startswith("2025-03-10T00:00:00")

def test_explanation_of_benefit_mapper_with_item_adjudications():
    mapper = ExplanationOfBenefitMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # EOB with line item adjudications
    eob_res = {
        "resourceType": "ExplanationOfBenefit",
        "id": "eob003",
        "status": "active",
        "patient": {
            "reference": "Patient/patient123"
        },
        "claim": {
            "reference": "Claim/claim003"
        },
        "insurer": {
            "reference": "Organization/payer789"
        },
        "outcome": "complete",
        "payment": {
            "date": "2025-04-15T00:00:00Z",
            "amount": {
                "value": 150.00,
                "currency": "USD"
            }
        },
        "item": [
            {
                "sequence": 1,
                "adjudication": [
                    {
                        "category": {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/adjudication",
                                    "code": "contractual",
                                    "display": "Contractual Obligation"
                                }
                            ]
                        },
                        "amount": {
                            "value": 25.00,
                            "currency": "USD"
                        }
                    }
                ]
            },
            {
                "sequence": 2,
                "adjudication": [
                    {
                        "category": {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/adjudication",
                                    "code": "deductible",
                                    "display": "Deductible"
                                }
                            ]
                        },
                        "amount": {
                            "value": 15.00,
                            "currency": "USD"
                        }
                    },
                    {
                        "category": {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/adjudication",
                                    "code": "copay",
                                    "display": "Copay"
                                }
                            ]
                        },
                        "amount": {
                            "value": 10.00,
                            "currency": "USD"
                        }
                    }
                ]
            }
        ]
    }
    
    # Create content with existing claim
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
        claims=[
            Claim(
                claim_id="claim003",
                patient_id="patient123",
                encounter_id="encounter456",
                claim_date=datetime.fromisoformat("2025-03-15T00:00:00+00:00"),
                payer_id="payer789",
                total_charge_amount=Decimal("200.00"),
                total_payment_amount=Decimal("0.00"),
                claim_status="accepted",
                claim_type="professional",
                # Add required fields to fix validation errors
                service_start_date=datetime.fromisoformat("2025-03-15T00:00:00+00:00"),
                service_end_date=datetime.fromisoformat("2025-03-15T00:00:00+00:00"),
                charges=[],
                organization_id="org123",
                payments=[],
                adjustments=[]
            )
        ]
    )
    
    mapper.map(eob_res, content, cache)
    
    # Check that the claim was updated
    claim = content.claims[0]
    assert claim.total_payment_amount == Decimal("150.00")
    
    # Check that line item adjustments were added
    assert len(claim.adjustments) == 3
    
    # Check each adjustment
    contractual_adj = next((adj for adj in claim.adjustments if adj.adjustment_reason_code == "contractual"), None)
    assert contractual_adj is not None
    assert contractual_adj.adjustment_amount == Decimal("25.00")
    
    deductible_adj = next((adj for adj in claim.adjustments if adj.adjustment_reason_code == "deductible" and "item-2" in adj.adjustment_id), None)
    assert deductible_adj is not None
    assert deductible_adj.adjustment_amount == Decimal("15.00")
    
    copay_adj = next((adj for adj in claim.adjustments if adj.adjustment_reason_code == "copay"), None)
    assert copay_adj is not None
    assert copay_adj.adjustment_amount == Decimal("10.00")

def test_explanation_of_benefit_mapper_zero_payment():
    mapper = ExplanationOfBenefitMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # EOB with zero payment (denial)
    eob_res = {
        "resourceType": "ExplanationOfBenefit",
        "id": "eob004",
        "status": "active",
        "patient": {
            "reference": "Patient/patient123"
        },
        "claim": {
            "reference": "Claim/claim004"
        },
        "insurer": {
            "reference": "Organization/payer789"
        },
        "outcome": "complete",
        "payment": {
            "date": "2025-05-15T00:00:00Z",
            "amount": {
                "value": 0.00,
                "currency": "USD"
            }
        },
        "total": [
            {
                "category": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/adjudication",
                            "code": "noncovered",
                            "display": "Non-covered"
                        }
                    ]
                },
                "amount": {
                    "value": 75.00,
                    "currency": "USD"
                }
            }
        ]
    }
    
    # Create content with existing claim
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
        claims=[
            Claim(
                claim_id="claim004",
                patient_id="patient123",
                encounter_id="encounter456",
                claim_date=datetime.fromisoformat("2025-04-15T00:00:00+00:00"),
                payer_id="payer789",
                total_charge_amount=Decimal("75.00"),
                total_payment_amount=Decimal("0.00"),
                claim_status="accepted",
                claim_type="professional",
                # Add required fields to fix validation errors
                service_start_date=datetime.fromisoformat("2025-04-15T00:00:00+00:00"),
                service_end_date=datetime.fromisoformat("2025-04-15T00:00:00+00:00"),
                charges=[],
                organization_id="org123",
                payments=[],
                adjustments=[]
            )
        ]
    )
    
    mapper.map(eob_res, content, cache)
    
    # Check that the claim was updated
    claim = content.claims[0]
    
    # Status should still be "paid" because EOB status is "active"
    assert claim.claim_status == "paid"
    
    # Payment amount should still be 0
    assert claim.total_payment_amount == Decimal("0.00")
    
    # There should be no payments (since amount is 0)
    assert len(claim.payments) == 0
    
    # But there should be a non-covered adjustment
    assert len(claim.adjustments) == 1
    adjustment = claim.adjustments[0]
    assert adjustment.adjustment_reason_code == "noncovered"
    assert adjustment.adjustment_amount == Decimal("75.00")