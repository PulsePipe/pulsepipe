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

# src/pulsepipe/ingesters/x12_utils/cas_mapper.py

from .base_mapper import BaseX12Mapper
from pulsepipe.models import Adjustment
from .decimal_utils import parse_x12_decimal

class CASMapper(BaseX12Mapper):
    def accepts(self, segment_id: str) -> bool:
        return segment_id == "CAS"

    def map(self, segment_id: str, elements: list, content, cache: dict):
        i = 0
        while i + 2 < len(elements):
            group_code = elements[i]
            reason_code = elements[i+1]
            amount = parse_x12_decimal(elements[i+2])

            adjustment = Adjustment(
                adjustment_id=f"{cache.get('claim_id')}_{len(content.adjustments) + 1}",
                charge_id=cache.get("last_charge_id"),
                payment_id=None,
                adjustment_date=None,
                adjustment_reason_code=reason_code,
                adjustment_reason_description=None,
                adjustment_amount=amount,
                adjustment_type=group_code,
                organization_id=None
            )
            content.adjustments.append(adjustment)
            i += 3  # next triplet
