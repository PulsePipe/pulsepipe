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

from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Literal
from pydantic import BaseModel, Field

class Charge(BaseModel):
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
