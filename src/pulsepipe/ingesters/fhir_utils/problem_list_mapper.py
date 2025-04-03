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

from pulsepipe.models import Problem, PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("Condition")
class ProblemListMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Condition"

    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        # Only accept problem-list items
        categories = [c.get("coding", [{}])[0].get("code") for c in resource.get("category", [])]
        if "problem-list-item" not in categories:
            return

        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        print("🔥 Problem List patient id:", patient_id)
        problem = Problem(
            code=resource.get("code", {}).get("coding", [{}])[0].get("code"),
            coding_method=resource.get("code", {}).get("coding", [{}])[0].get("system"),
            description=resource.get("code", {}).get("text"),
            onset_date=resource.get("onsetDateTime"),
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

        content.problem_list.append(problem)
