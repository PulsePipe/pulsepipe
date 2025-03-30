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

from pulsepipe.models import (
    VitalSign, LabReport, DiagnosticTest, PathologyReport, 
    MicrobiologyReport, ImagingReport, ImagingFinding
)
from pulsepipe.utils.config_loader import load_mapping_config
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

def get_observation_type(resource: dict) -> str:
    """Decide which PulseClinicalContent bucket the observation belongs to"""

    # Step 1: Check category code
    categories = resource.get("category", [])
    for cat in categories:
        code = cat.get("coding", [{}])[0].get("code")
        if code:
            # Allow external override
            overridden = OBS_MAPPING_CONFIG.get("overrides", {}).get("category", {}).get(code)
            if overridden:
                return overridden
            if code in DEFAULT_CATEGORY_MAP:
                return DEFAULT_CATEGORY_MAP[code]

    # Step 2: Check observation code
    obs_code = resource.get("code", {}).get("coding", [{}])[0].get("code")
    if obs_code:
        overridden = OBS_MAPPING_CONFIG.get("overrides", {}).get("loinc", {}).get(obs_code)
        if overridden:
            return overridden

    return None  # Unclassified

def map_observation(resource: dict, content):
    obs_type = get_observation_type(resource)

    if obs_type == "vital_signs":
        content.vital_signs.append(parse_vital_sign(resource))

    elif obs_type == "lab":
        content.lab.append(parse_lab(resource))

    elif obs_type == "imaging":
        content.imaging.append(parse_imaging(resource))

    elif obs_type == "diagnostic_test":
        content.diagnostic_test.append(parse_diagnostic(resource))

    elif obs_type == "pathology":
        content.pathology.append(parse_pathology(resource))

    elif obs_type == "microbiology":
        content.microbiology.append(parse_microbiology(resource))


def parse_vital_sign(resource: dict) -> VitalSign:
    return VitalSign(
        code=get_code(resource),
        coding_method=get_system(resource),
        display=get_display(resource),
        value=resource.get("valueQuantity", {}).get("value"),
        unit=resource.get("valueQuantity", {}).get("unit"),
        timestamp=resource.get("effectiveDateTime"),
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )


def parse_imaging(resource: dict) -> ImagingReport:
    finding = ImagingFinding(
        code=get_code(resource),
        coding_method=get_system(resource),
        description=get_display(resource),
        impression=resource.get("valueString"),
        abnormal_flag=resource.get("interpretation", {}).get("coding", [{}])[0].get("code"),
        result_date=resource.get("effectiveDateTime"),
    )

    return ImagingReport(
        report_id=resource.get("id"),
        image_type=resource.get("code", {}).get("text"),
        coding_method=get_system(resource),
        modality=resource.get("modality", {}).get("coding", [{}])[0].get("code"),
        acquisition_date=resource.get("effectiveDateTime"),
        findings=[finding],
        narrative=resource.get("text", {}).get("div"),  # Often narrative report is here
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )

def parse_lab(resource: dict) -> LabReport:
    return LabReport(
        report_id=resource.get("id"),
        panel_name=resource.get("code", {}).get("text"),
        code=get_code(resource),
        coding_method=get_system(resource),
        collection_date=resource.get("effectiveDateTime"),
        observations=[resource],  # we will refine later to structured observations
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )

def parse_diagnostic(resource: dict) -> DiagnosticTest:
    return DiagnosticTest(
        test_id=resource.get("id"),
        test_type=resource.get("code", {}).get("text"),
        code=get_code(resource),
        coding_method=get_system(resource),
        result_date=resource.get("effectiveDateTime"),
        result_summary=resource.get("valueString"),
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )

def parse_pathology(resource: dict) -> PathologyReport:
    return PathologyReport(
        report_id=resource.get("id"),
        specimen=resource.get("bodySite", {}).get("text"),
        report_date=resource.get("effectiveDateTime"),
        narrative=resource.get("text", {}).get("div"),
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )

def parse_microbiology(resource: dict) -> MicrobiologyReport:
    return MicrobiologyReport(
        report_id=resource.get("id"),
        collection_date=resource.get("effectiveDateTime"),
        comment=resource.get("text", {}).get("div"),
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )
