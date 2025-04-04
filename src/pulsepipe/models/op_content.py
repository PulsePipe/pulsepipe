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

from pydantic import BaseModel
from typing import Optional, List
from .billing import Claim, Charge, Payment, Adjustment
from .prior_authorization import PriorAuthorization


class PulseOperationalContent(BaseModel):
    transaction_type: Optional[str]  # e.g., '837P', '835', '278'
    interchange_control_number: Optional[str]
    functional_group_control_number: Optional[str]
    organization_id: Optional[str]

    claims: List[Claim] = []
    charges: List[Charge] = []
    payments: List[Payment] = []
    adjustments: List[Adjustment] = []
    prior_authorizations: List[PriorAuthorization] = []
