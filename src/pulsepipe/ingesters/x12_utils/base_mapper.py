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

# src/pulsepipe/ingesters/x12_utils/base_mapper.py

from pulsepipe.models import MessageCache

MAPPER_REGISTRY = []

class BaseX12Mapper:
    def __init_subclass__(cls):
        super().__init_subclass__()
        MAPPER_REGISTRY.append(cls())

    def accepts(self, segment_id: str) -> bool:
        raise NotImplementedError("Mapper must implement `accepts()`")

    def map(self, segment_id: str, elements: list, content, cache: MessageCache):
        raise NotImplementedError("Mapper must implement `map()`")
