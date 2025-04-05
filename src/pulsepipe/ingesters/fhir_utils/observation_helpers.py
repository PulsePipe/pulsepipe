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

# src/pulsepipe/ingesters/fhir_utils/observation_helpers.py

from pulsepipe.models import (
    LabObservation,
    ImagingFinding,
    VitalSign,
    DiagnosticTest
)
from .extractors import (
    get_code,
    get_system,
    get_display,
    extract_patient_reference,
    extract_encounter_reference
)


def map_simple_lab_observation(resource: dict) -> LabObservation:
    return LabObservation(
        code=get_code(resource) or "Unknown",
        coding_method=get_system(resource) or "Unknown",
        name=resource.get("code", {}).get("text") or "Unknown",
        description=resource.get("code", {}).get("text") or "Unknown",
        value=(
            str(resource.get("valueQuantity", {}).get("value"))
            if resource.get("valueQuantity", {}).get("value") is not None
            else resource.get("valueString") or "Unknown"
        ),
        unit=resource.get("valueQuantity", {}).get("unit"),
        reference_range=None,
        abnormal_flag=resource.get("interpretation", {}).get("coding", [{}])[0].get("code"),
        result_date=resource.get("effectiveDateTime"),
    )


def map_simple_imaging(resource: dict) -> ImagingFinding:
    return ImagingFinding(
        code=get_code(resource) or "Unknown",
        coding_method=get_system(resource) or "Unknown",
        description=get_display(resource) or "Unknown",
        impression=resource.get("valueString") or "Unknown",
        abnormal_flag=resource.get("interpretation", {}).get("coding", [{}])[0].get("code"),
        result_date=resource.get("effectiveDateTime"),
    )


def map_simple_vital(resource: dict) -> VitalSign:
    return VitalSign(
        code=get_code(resource) or "Unknown",
        coding_method=get_system(resource) or "Unknown",
        display=get_display(resource) or "Unknown",
        value=resource.get("valueQuantity", {}).get("value") or 0,
        unit=resource.get("valueQuantity", {}).get("unit"),
        timestamp=resource.get("effectiveDateTime"),
        patient_id=extract_patient_reference(resource),
        encounter_id=extract_encounter_reference(resource),
    )


def map_simple_diagnostic(resource: dict) -> DiagnosticTest:
    return DiagnosticTest(
        code=get_code(resource) or "Unknown",
        coding_method=get_system(resource) or "Unknown",
        name=resource.get("code", {}).get("text") or "Unknown",
        description=get_display(resource) or "Unknown",
        value=resource.get("valueString") or resource.get("valueQuantity", {}).get("value"),
        unit=resource.get("valueQuantity", {}).get("unit"),
        abnormal_flag=resource.get("interpretation", {}).get("coding", [{}])[0].get("code"),
        result_date=resource.get("effectiveDateTime"),
    )
