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

# src/pulsepipe/ingesters/x12_utils/clp_mapper.py

from .base_mapper import BaseX12Mapper
from decimal import Decimal
from pulsepipe.models import Claim
from .decimal_utils import parse_x12_decimal

class CLPMapper(BaseX12Mapper):
    def accepts(self, segment_id: str) -> bool:
        return segment_id == "CLP"

    def map(self, segment_id: str, elements: list, content, cache: dict):
        print("CLP elements:", elements)

        claim_status_code = elements[1]
        claim_status_map = {
            "1": "accepted",
            "2": "denied",
            "3": "adjusted",
            "4": "paid"
        }

        claim = Claim(
            claim_id=elements[0],
            patient_id=cache.get("patient_id"),
            encounter_id=None,
            claim_date=None,
            payer_id=cache.get("payer_id"),
            total_charge_amount=parse_x12_decimal(elements[2]),
            total_payment_amount=parse_x12_decimal(elements[3]),
            claim_status=claim_status_map.get(claim_status_code, "submitted"),
            claim_type=None,
            service_start_date=None,
            service_end_date=None,
            charges=[],
            payments=[],
            adjustments=[],
            organization_id=None
        )
        content.claims.append(claim)
        cache["claim_id"] = claim.claim_id
