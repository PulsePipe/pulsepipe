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
# src/pulsepipe/ingesters/fhir_utils/immunization_mapper.py

# ---------------------------------------------------------------------------
# PulsePipe — Medication Mapper
# ---------------------------------------------------------------------------

from pulsepipe.models import Medication, PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, get_display

@fhir_mapper("MedicationStatement")
class MedicationMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "MedicationStatement"
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        content.medications.append(self.parse_medication(resource, cache))

    def parse_medication(self, resource: dict, cache: MessageCache) -> Medication:
        med_codeable = resource.get("medicationCodeableConcept", {})
        dosage_list = resource.get("dosage", [])

        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")

        # We'll take the first dosage if present ?
        dose = None
        route = None
        frequency = None
        if dosage_list:
            dosage = dosage_list[0]
            dose = dosage.get("doseAndRate", [{}])[0].get("doseQuantity", {}).get("value")
            route = dosage.get("route", {}).get("text")
            frequency = dosage.get("timing", {}).get("repeat", {}).get("frequency")

        return Medication(
            code=get_code(resource) or get_code(med_codeable),
            coding_method=get_system(resource) or get_system(med_codeable),
            name=med_codeable.get("text"),
            dose=str(dose) if dose else None,
            route=route,
            frequency=str(frequency) if frequency else None,
            start_date=resource.get("effectiveDateTime"),
            end_date=None,  # Optional: may not exist
            status=resource.get("status"),
            patient_id=patient_id,
            encounter_id=encounter_id,
        )
