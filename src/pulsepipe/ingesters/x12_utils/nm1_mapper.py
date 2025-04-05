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

# src/pulsepipe/ingesters/x12_utils/nm1_mapper.py

from .base_mapper import BaseX12Mapper

class NM1Mapper(BaseX12Mapper):
    def accepts(self, segment_id: str) -> bool:
        return segment_id == "NM1"

    def map(self, segment_id: str, elements: list, content, cache: dict):
        entity_id = elements[1]

        if entity_id == "QC":  # Patient
            cache["patient_id"] = elements[9] if len(elements) > 9 else None

        elif entity_id == "82":  # Rendering Provider
            cache["rendering_provider_id"] = elements[9] if len(elements) > 9 else None

        elif entity_id == "PR":  # Payer
            cache["payer_id"] = elements[9] if len(elements) > 9 else None
