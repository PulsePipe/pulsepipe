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

from pulsepipe.models import EncounterInfo, EncounterProvider
from .extractors import (
    extract_patient_reference,
)

def map_encounter(resource: dict) -> EncounterInfo:
    return EncounterInfo(
        id=resource.get("id"),
        admit_date=resource.get("period", {}).get("start"),
        discharge_date=resource.get("period", {}).get("end"),
        encounter_type=extract_encounter_type(resource),
        type_coding_method=extract_encounter_type_coding_method(resource),
        location=extract_location(resource),
        reason_code=extract_reason_code(resource),
        reason_coding_method=extract_reason_coding_method(resource),
        visit_type=extract_visit_type(resource),
        patient_id=extract_patient_reference(resource),
        providers=extract_providers(resource),
    )


def extract_encounter_type(resource: dict) -> str:
    if "type" in resource:
        return resource["type"][0].get("coding", [{}])[0].get("display")
    return None


def extract_encounter_type_coding_method(resource: dict) -> str:
    if "type" in resource:
        return resource["type"][0].get("coding", [{}])[0].get("system")
    return None


def extract_location(resource: dict) -> str:
    if "location" in resource and resource["location"]:
        return resource["location"][0].get("location", {}).get("display")
    return None


def extract_reason_code(resource: dict) -> str:
    reason = resource.get("reasonCode", [{}])[0]
    return reason.get("text") or reason.get("coding", [{}])[0].get("display")


def extract_reason_coding_method(resource: dict) -> str:
    reason = resource.get("reasonCode", [{}])[0]
    return reason.get("coding", [{}])[0].get("system")


def extract_visit_type(resource: dict) -> str:
    if "class" in resource:
        return resource["class"].get("code")
    return None


def extract_patient_reference(resource: dict) -> str:
    subject = resource.get("subject", {})
    if "reference" in subject:
        return subject["reference"].split("/")[-1]
    return None


def extract_providers(resource: dict) -> list:
    providers = []
    for participant in resource.get("participant", []):
        individual = participant.get("individual", {})
        provider_id = None
        provider_name = None
        if "reference" in individual:
            provider_id = individual["reference"].split("/")[-1]
        if "display" in individual:
            provider_name = individual["display"]

        provider_type = participant.get("type", [{}])[0]
        type_code = provider_type.get("coding", [{}])[0].get("code")
        type_code_method = provider_type.get("coding", [{}])[0].get("system")
    
        # Specialty is often available via extensions or separate Practitioner resource
        # Here we leave it None unless you later resolve it externally
        providers.append(EncounterProvider(
            id=provider_id,
            name=provider_name,
            type_code=type_code,
            coding_method = type_code_method,
            specialty=None
        ))
    return providers or None
