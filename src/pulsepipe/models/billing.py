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

# src/pulsepipe/models/billing.py

from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Literal
from pydantic import BaseModel, Field

class Charge(BaseModel):
    """
    Represents a billable service or item provided to a patient.
    
    Charges are the fundamental units of healthcare billing, representing
    specific services, procedures, or items provided to a patient during
    an encounter. Each charge has an associated code, description, and
    monetary amount.
    """
    charge_id: str
    encounter_id: Optional[str]
    patient_id: Optional[str]
    service_date: Optional[datetime]
    charge_code: str
    charge_description: Optional[str]
    charge_amount: Decimal = Field(..., ge=0)
    quantity: Optional[int] = Field(None, ge=0)
    performing_provider_id: Optional[str]
    ordering_provider_id: Optional[str]
    revenue_code: Optional[str]
    cpt_hcpcs_code: Optional[str]
    diagnosis_pointers: Optional[List[str]]
    charge_status: Optional[Literal['posted', 'adjusted', 'voided']]  # safer than free text
    organization_id: Optional[str]


class Payment(BaseModel):
    """
    Represents a financial transaction made against a patient account.
    
    Payments can come from various sources including insurance companies,
    patients, or other third-party payers. Each payment is typically
    associated with one or more charges and may include remittance advice
    information that explains how the payment should be applied.
    """
    payment_id: str
    patient_id: Optional[str]
    encounter_id: Optional[str]
    charge_id: Optional[str]
    payer_id: Optional[str]
    payment_date: Optional[datetime]
    payment_amount: Decimal = Field(..., ge=0)
    payment_type: Optional[Literal['insurance', 'patient', 'adjustment', 'refund']]
    check_number: Optional[str]
    remit_advice_code: Optional[str]
    remit_advice_description: Optional[str]
    organization_id: Optional[str]


class Adjustment(BaseModel):
    """
    Represents a modification to a charge amount that isn't a direct payment.
    
    Adjustments modify the outstanding balance on a patient account without
    representing an actual payment. Common examples include contractual 
    adjustments (insurance discounts), charity care, bad debt write-offs,
    or corrections to billing errors.
    """
    adjustment_id: str
    charge_id: Optional[str] 
    payment_id: Optional[str]
    adjustment_date: Optional[datetime]
    adjustment_reason_code: Optional[str]
    adjustment_reason_description: Optional[str]
    adjustment_amount: Decimal
    adjustment_type: Optional[str]
    organization_id: Optional[str]


class Claim(BaseModel):
    """
    Represents a formal request for payment submitted to a payer.
    
    A claim is a comprehensive document that bundles together multiple charges
    for services provided to a patient, typically during a single encounter
    or over a defined period. Claims are submitted to payers (insurance companies)
    for reimbursement and go through various stages of processing before payment.
    
    Claims can be categorized as professional (from physicians), institutional
    (from hospitals/facilities), or dental.
    """
    claim_id: str
    patient_id: Optional[str]
    encounter_id: Optional[str]
    claim_date: Optional[datetime]
    payer_id: Optional[str]
    total_charge_amount: Decimal = Field(..., ge=0)
    total_payment_amount: Optional[Decimal] = Field(0, ge=0)
    claim_status: Optional[Literal['submitted', 'accepted', 'denied', 'adjusted', 'paid']]
    claim_type: Optional[Literal['professional', 'institutional', 'dental']]
    service_start_date: Optional[datetime]
    service_end_date: Optional[datetime]
    charges: Optional[List[Charge]]
    payments: Optional[List[Payment]]
    adjustments: Optional[List[Adjustment]]
    organization_id: Optional[str]
