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
# src/pulsepipe/ingesters/fhir_utils/allergy_mapper.py

from pulsepipe.models import Allergy, PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference

@fhir_mapper("AllergyIntolerance")
class AllergyMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "AllergyIntolerance"
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:

        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        clinical_status = resource.get("clinicalStatus", {}).get("coding", [{}])[0].get("code")
        print("🔥 Allergy patient id:", patient_id)
        if clinical_status == "inactive":
            allergy = Allergy(
                substance="No Known Allergies",
                coding_method=None,
                reaction=None,
                severity=None,
                onset=None,
                patient_id=patient_id,
            )
        else:
            substance = resource.get("code", {}).get("text") or \
                        resource.get("code", {}).get("coding", [{}])[0].get("display")

            coding_method = resource.get("code", {}).get("coding", [{}])[0].get("system")
            reaction = resource.get("reaction", [{}])[0].get("description")
            severity = resource.get("reaction", [{}])[0].get("severity")
            onset = resource.get("onsetDateTime")

            allergy = Allergy(
                substance=substance or "Unknown",
                coding_method=coding_method,
                reaction=reaction,
                severity=severity,
                onset=onset,
                patient_id=patient_id,
            )

        content.allergies.append(allergy)
