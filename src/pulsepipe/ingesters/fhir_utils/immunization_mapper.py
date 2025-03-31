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

from pulsepipe.models import Immunization, PulseClinicalContent
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("Immunization")
class ImmunizationMapper(BaseFHIRMapper):
    def map(self, resource: dict, content: PulseClinicalContent) -> None:
        vaccine_coding = resource.get("vaccineCode", {}).get("coding", [{}])[0]

        vaccine_code = vaccine_coding.get("code")
        coding_method = vaccine_coding.get("system")
        description = resource.get("vaccineCode", {}).get("text") or vaccine_coding.get("display")

        date_administered = resource.get("occurrenceDateTime")
        status = resource.get("status")
        lot_number = resource.get("lotNumber")

        patient_id = extract_patient_reference(resource.get("patient", {}))
        encounter_id = extract_encounter_reference(resource.get("encounter", {}))

        immunization = Immunization(
            vaccine_code=vaccine_code or "Unknown",
            coding_method=coding_method,
            description=description,
            date_administered=date_administered,
            status=status,
            lot_number=lot_number,
            patient_id=patient_id,
            encounter_id=encounter_id
        )

        content.immunizations.append(immunization)



