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

# src/pulsepipe/models/prior_authorization.py

from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

class PriorAuthorization(BaseModel):
    """
    Represents a prior authorization request or response.
    
    Prior authorizations are requests to health insurance companies for
    approval to provide specific services, medications, or procedures
    before they're delivered to patients. They're a key component of
    healthcare utilization management and cost control.
    
    This model captures both the request details and the payer's response,
    including approval status, authorized services, and relevant timeframes.
    Prior authorizations are typically handled via X12 278 transactions in
    healthcare EDI systems.
    """
    auth_id: Optional[str] = None
    patient_id: Optional[str] = None
    provider_id: Optional[str] = None
    requested_procedure: Optional[str] = None
    auth_type: Optional[str] = None
    review_status: Optional[str] = None
    service_dates: Optional[List[datetime]] = None
    diagnosis_codes: Optional[List[str]] = None
    organization_id: Optional[str] = None
