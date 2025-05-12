
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

# src/pulsepipe/ingesters/fhir_utils/condition_mapper.py

from pulsepipe.models import PulseClinicalContent, Problem, Diagnosis, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("Condition")
class ConditionMapper(BaseFHIRMapper):
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        categories = resource.get("category", [])
        is_problem = any(
            c.get("coding", [{}])[0].get("code") == "problem-list-item" for c in categories
        )

        if is_problem:
            content.problem_list.append(self.parse_problem(resource, cache))
        else:
            content.diagnoses.append(self.parse_diagnosis(resource, cache))

    def parse_problem(self, resource: dict, cache: MessageCache) -> Problem:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        return Problem(
            # Map to the correct field names from the model
            description=resource.get("code", {}).get("text") or "Unknown",
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

    def parse_diagnosis(self, resource: dict, cache: MessageCache) -> Diagnosis:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        return Diagnosis(
            # Map to the correct field names from the model with all required fields
            code=None,  # Required field
            coding_method=None,  # Required field
            description=resource.get("code", {}).get("text") or "Unknown",
            onset_date=None,  # Required field
            patient_id=patient_id,
            encounter_id=encounter_id,
        )
