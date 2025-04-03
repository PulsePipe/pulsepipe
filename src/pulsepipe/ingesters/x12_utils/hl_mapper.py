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

from .base_mapper import BaseX12Mapper

class HLMapper(BaseX12Mapper):
    def accepts(self, segment_id: str) -> bool:
        return segment_id == "HL"

    def map(self, segment_id: str, elements: list, content, cache: dict):
        hl_id = elements[0]
        hl_parent = elements[1] if len(elements) > 1 else None
        hl_code = elements[2] if len(elements) > 2 else None

        cache["hl_id"] = hl_id
        cache["hl_parent"] = hl_parent
        cache["hl_code"] = hl_code

        # optionally track hierarchy for later if you need nested relationships
        cache.setdefault("hl_hierarchy", {})[hl_id] = {
            "parent": hl_parent,
            "code": hl_code
        }

        print(f"HL Detected: id={hl_id}, parent={hl_parent}, code={hl_code}")
