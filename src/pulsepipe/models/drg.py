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

# src/pulsepipe/models/drg.py

from typing import Optional, List
from decimal import Decimal
from pydantic import BaseModel, Field

class DRG(BaseModel):
    """
    Represents a Diagnosis-Related Group (DRG) assignment for an inpatient encounter.
    
    DRGs are a patient classification system that standardizes prospective payment to 
    hospitals and encourages cost containment initiatives. They classify hospital cases 
    into groups expected to have similar hospital resource use, primarily based on 
    diagnoses, procedures, age, sex, and the presence of complications or comorbidities.
    
    DRGs form the basis for hospital payment in many healthcare systems, including 
    Medicare's Inpatient Prospective Payment System (IPPS) in the United States.
    """
    drg_code: str
    drg_description: Optional[str] = None
    drg_type: Optional[str] = Field(None, description="e.g., 'MS-DRG', 'AP-DRG', 'APR-DRG'")
    drg_version: Optional[str] = Field(None, description="Version of the DRG system being used")
    relative_weight: Optional[Decimal] = Field(None, ge=0)
    severity_of_illness: Optional[int] = Field(None, ge=1, le=4, description="For APR-DRGs, 1=Minor, 4=Extreme")
    risk_of_mortality: Optional[int] = Field(None, ge=1, le=4, description="For APR-DRGs, 1=Minor, 4=Extreme")
    average_length_of_stay: Optional[Decimal] = Field(None, ge=0)
    geometric_mean_length_of_stay: Optional[Decimal] = Field(None, ge=0)
    mdc_code: Optional[str] = Field(None, description="Major Diagnostic Category code")
    mdc_description: Optional[str] = None
    principal_diagnosis_code: Optional[str] = None
    procedure_codes: Optional[List[str]] = Field(default_factory=list)
    complication_flag: Optional[bool] = None
    payment_amount: Optional[Decimal] = Field(None, ge=0)
    patient_id: Optional[str] = None
    encounter_id: Optional[str] = None
