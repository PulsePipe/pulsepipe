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

# src/pulsepipe/ingesters/x12_utils/plb_mapper.py

from .base_mapper import BaseX12Mapper
from decimal import Decimal
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models import Charge
from .decimal_utils import parse_x12_decimal

class PLBMapper(BaseX12Mapper):
    def __init__(self):
        self.typeCode = "PLB"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing X12 PLBMapper")

    def accepts(self, segment_id: str) -> bool:
        return segment_id == self.typeCode

    def map(self, segment_id: str, elements: list, content, cache: dict):
        self.logger.debug("{self.typeCode} elements: {elements}")
        charge = Charge(
            charge_id=f"{cache.get('claim_id')}_{len(content.charges) + 1}",
            encounter_id=None,
            patient_id=cache.get("patient_id"),
            service_date=None,
            charge_code=elements[0],
            charge_description=None,
            charge_amount=parse_x12_decimal(elements[1]),
            quantity=int(elements[3]) if len(elements) > 3 else None,
            performing_provider_id=None,
            ordering_provider_id=None,
            revenue_code=None,
            cpt_hcpcs_code=None,
            diagnosis_pointers=[],
            charge_status="posted",
            organization_id=None
        )
        content.charges.append(charge)
        cache["last_charge_id"] = charge.charge_id
