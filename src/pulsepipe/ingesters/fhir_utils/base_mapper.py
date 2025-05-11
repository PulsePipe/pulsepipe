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

# src/pulsepipe/ingesters/fhir_utils/base_mapper.py

from typing import Type, List, Optional

MAPPER_REGISTRY: List["BaseFHIRMapper"] = []

class BaseFHIRMapper:
    resource_type: Optional[str] = None

    def accepts(self, resource: dict) -> bool:
        return (resource.get("resourceType") or "").lower() == (getattr(self, "RESOURCE_TYPE", "") or "").lower()

    def map(self, resource: dict, content, cache) -> None:
        raise NotImplementedError
    
    def __repr__(self):
        return f"<{self.__class__.__name__}: maps {getattr(self, 'RESOURCE_TYPE', 'unknown').lower()}>"

    def __str__(self):
        return self.__repr__()


def fhir_mapper(resource_type: str):
    def decorator(cls: Type[BaseFHIRMapper]):
        cls.resource_type = resource_type
        instance = cls()
        MAPPER_REGISTRY.append(instance)
        return cls
    return decorator
