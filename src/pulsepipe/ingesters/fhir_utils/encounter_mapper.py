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
# src/pulsepipe/ingesters/fhir_utils/encounter_mapper.py

from typing import List, Optional
from pulsepipe.models import EncounterInfo, EncounterProvider, PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference

@fhir_mapper("Encounter")
class EncounterMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Encounter"
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        content.encounter = EncounterInfo(
            id=resource.get("id"),
            admit_date=resource.get("period", {}).get("start"),
            discharge_date=resource.get("period", {}).get("end"),
            encounter_type=self.extract_encounter_type(resource),
            type_coding_method=self.extract_encounter_type_coding_method(resource),
            location=self.extract_location(resource),
            reason_code=self.extract_reason_code(resource),
            reason_coding_method=self.extract_reason_coding_method(resource),
            visit_type=self.extract_visit_type(resource),
            patient_id=patient_id,
            providers=self.extract_providers(resource),
        )

    def extract_encounter_type(self, resource: dict) -> Optional[str]:
        return resource.get("type", [{}])[0].get("coding", [{}])[0].get("display")

    def extract_encounter_type_coding_method(self, resource: dict) -> Optional[str]:
        return resource.get("type", [{}])[0].get("coding", [{}])[0].get("system")

    def extract_location(self, resource: dict) -> Optional[str]:
        return resource.get("location", [{}])[0].get("location", {}).get("display")

    def extract_reason_code(self, resource: dict) -> Optional[str]:
        reason = resource.get("reasonCode", [{}])[0]
        return reason.get("text") or reason.get("coding", [{}])[0].get("display")

    def extract_reason_coding_method(self, resource: dict) -> Optional[str]:
        reason = resource.get("reasonCode", [{}])[0]
        return reason.get("coding", [{}])[0].get("system")

    def extract_visit_type(self, resource: dict) -> Optional[str]:
        return resource.get("class", {}).get("code")

    def extract_providers(self, resource: dict) -> Optional[List[EncounterProvider]]:
        providers = []
        for participant in resource.get("participant", []):
            individual = participant.get("individual", {})
            provider_id = individual.get("reference", "").split("/")[-1] if "reference" in individual else None
            provider_name = individual.get("display")
            provider_type = participant.get("type", [{}])[0]
            type_code = provider_type.get("coding", [{}])[0].get("code")
            type_code_method = provider_type.get("coding", [{}])[0].get("system")

            providers.append(EncounterProvider(
                id=provider_id,
                name=provider_name,
                type_code=type_code,
                coding_method=type_code_method,
                specialty=None
            ))
        return providers or None
