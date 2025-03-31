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
# src/pulsepipe/ingesters/fhir_utils/observation_mapper.py

from pulsepipe.models import (
    VitalSign, LabReport, LabObservation, DiagnosticTest, PathologyReport, 
    MicrobiologyReport, ImagingReport, ImagingFinding, PulseClinicalContent
)
from pulsepipe.utils.config_loader import load_mapping_config
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .observation_helpers import map_simple_imaging, map_simple_lab_observation
from .extractors import (
    extract_patient_reference,
    extract_encounter_reference,
    get_code,
    get_system,
    get_display,
)

DEFAULT_CATEGORY_MAP = {
    "vital-signs": "vital_signs",
    "laboratory": "lab",
    "imaging": "imaging",
    "diagnostic-test": "diagnostic_test",
    "pathology": "pathology",
    "microbiology": "microbiology",
}

OBS_MAPPING_CONFIG = load_mapping_config("observation_mappings.yaml")


@fhir_mapper("Observation")
class ObservationMapper(BaseFHIRMapper):
    def map(self, resource: dict, content: PulseClinicalContent) -> None:
        obs_type = self.get_observation_type(resource)

        if obs_type == "vital_signs":
            content.vital_signs.append(self.parse_vital_sign(resource))

        elif obs_type == "lab":
            content.lab.append(self.parse_lab(resource))

        elif obs_type == "imaging":
            content.imaging.append(self.parse_imaging(resource))

        elif obs_type == "diagnostic_test":
            content.diagnostic_test.append(self.parse_diagnostic(resource))

        elif obs_type == "pathology":
            content.pathology.append(self.parse_pathology(resource))

        elif obs_type == "microbiology":
            content.microbiology.append(self.parse_microbiology(resource))

        # If not classified, we silently skip — you could log if you prefer

    def get_observation_type(self, resource: dict) -> str:
        # Step 1: Check category code
        categories = resource.get("category", [])
        for cat in categories:
            code = cat.get("coding", [{}])[0].get("code")
            if code:
                # External override first
                overridden = OBS_MAPPING_CONFIG.get("overrides", {}).get("category", {}).get(code)
                if overridden:
                    return overridden
                if code in DEFAULT_CATEGORY_MAP:
                    return DEFAULT_CATEGORY_MAP[code]

        # Step 2: Check observation code override
        obs_code = resource.get("code", {}).get("coding", [{}])[0].get("code")
        if obs_code:
            overridden = OBS_MAPPING_CONFIG.get("overrides", {}).get("loinc", {}).get(obs_code)
            if overridden:
                return overridden

        return None  # Unclassified

    def parse_vital_sign(self, resource: dict) -> VitalSign:
        return VitalSign(
            code=get_code(resource) or "Unknown",
            coding_method=get_system(resource),
            display=get_display(resource) or "Unknown",
            value=resource.get("valueQuantity", {}).get("value") or 0,
            unit=resource.get("valueQuantity", {}).get("unit") or "Unknown",
            timestamp=resource.get("effectiveDateTime") or "Unknown",
            patient_id=extract_patient_reference(resource) or "Unknown",
            encounter_id=extract_encounter_reference(resource) or "Unknown",
        )

    def parse_imaging(self, resource: dict) -> ImagingReport:
        finding = map_simple_imaging(resource)

        return ImagingReport(
            report_id=resource.get("id"),
            image_type=resource.get("code", {}).get("text"),
            coding_method=get_system(resource),
            modality=resource.get("modality", {}).get("coding", [{}])[0].get("code"),
            acquisition_date=resource.get("effectiveDateTime"),
            findings=[finding],
            ordering_provider_id = None,
            performing_facility = None,
            narrative=resource.get("text", {}).get("div"),
            patient_id=extract_patient_reference(resource),
            encounter_id=extract_encounter_reference(resource),
        )

    def parse_lab(self, resource: dict) -> LabReport:
        return LabReport(
            report_id=resource.get("id") or None,
            lab_type=None,  # Optional, maybe from `category` later
            code=get_code(resource) or None,
            coding_method=get_system(resource) or None,
            panel_name=resource.get("code", {}).get("text") or None,
            panel_code=None,
            panel_code_method=None,
            is_panel=None,
            ordering_provider_id=None,
            performing_lab=None,
            report_type=None,
            collection_date=resource.get("effectiveDateTime"),
            observations=[map_simple_lab_observation(resource)],
            note=resource.get("text", {}).get("div"),
            patient_id=extract_patient_reference(resource),
            encounter_id=extract_encounter_reference(resource),
        )

    def parse_diagnostic(self, resource: dict) -> DiagnosticTest:
        return DiagnosticTest(
            test_id=resource.get("id") or "Unknown",
            test_type=resource.get("code", {}).get("text")  or "Unknown",
            code=get_code(resource) or "Unknown",
            coding_method=get_system(resource) or "Unknown",
            result_date=resource.get("effectiveDateTime"),
            result_summary=resource.get("valueString"),
            patient_id=extract_patient_reference(resource),
            encounter_id=extract_encounter_reference(resource),
        )

    def parse_pathology(self, resource: dict) -> PathologyReport:
        return PathologyReport(
            report_id=resource.get("id"),
            specimen=resource.get("bodySite", {}).get("text"),
            report_date=resource.get("effectiveDateTime"),
            narrative=resource.get("text", {}).get("div"),
            patient_id=extract_patient_reference(resource),
            encounter_id=extract_encounter_reference(resource),
        )

    def parse_microbiology(self, resource: dict) -> MicrobiologyReport:
        return MicrobiologyReport(
            report_id=resource.get("id"),
            collection_date=resource.get("effectiveDateTime"),
            comment=resource.get("text", {}).get("div"),
            patient_id=extract_patient_reference(resource),
            encounter_id=extract_encounter_reference(resource),
        )
