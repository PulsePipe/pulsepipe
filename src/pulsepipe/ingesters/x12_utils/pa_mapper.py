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

# src/pulsepipe/ingesters/x12_utils/pa_mapper.py

from .base_mapper import BaseX12Mapper
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models import PriorAuthorization
from datetime import datetime

class PriorAuthorizationMapper(BaseX12Mapper):
    def __init__(self):
        self.typeCode = "UM"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing X12 PriorAuthorizationMapper")

    def accepts(self, segment_id: str) -> bool:
        return segment_id == {self.typeCode}

    def map(self, segment_id: str, elements: list, content, cache: dict):
        self.logger.debug("{self.typeCode}: {elements}")

        prior_auth = PriorAuthorization(
            auth_id=elements[0] if len(elements) > 0 else f"UM_{len(content.prior_authorizations) + 1}",
            patient_id=cache.get("patient_id"),
            provider_id=cache.get("provider_id"),
            requested_procedure=cache.get("requested_procedure"),
            auth_type=elements[1] if len(elements) > 1 else None,
            review_status=elements[2] if len(elements) > 2 else None,
            service_dates=[datetime.now()],  # TODO: extract from DTP
            diagnosis_codes=cache.get("diagnosis_codes", []),
            organization_id=None
        )
        content.prior_authorizations.append(prior_auth)
        cache["last_auth_id"] = prior_auth.auth_id
