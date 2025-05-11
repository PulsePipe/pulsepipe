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

# src/pulsepipe/ingesters/fhir_utils/extractors.py

def extract_patient_reference(resource: dict) -> str:
    ref = resource.get("subject", {}).get("reference")
    if ref:
        return ref.split("/")[-1]
    return None

def extract_encounter_reference(resource: dict) -> str:
    ref = resource.get("encounter", {}).get("reference")
    if ref:
        return ref.split("/")[-1]
    return None

def get_code(resource: dict) -> str:
    return resource.get("code", {}).get("coding", [{}])[0].get("code")

def get_system(resource: dict) -> str:
    return resource.get("code", {}).get("coding", [{}])[0].get("system")

def get_display(resource: dict) -> str:
    return resource.get("code", {}).get("text") or resource.get("code", {}).get("coding", [{}])[0].get("display")

def extract_effective_date(resource: dict) -> str:
    return resource.get("effectiveDateTime") or resource.get("issued") or resource.get("performedDateTime")

def extract_reference_id(reference_obj: dict) -> str:
    if "reference" in reference_obj:
        return reference_obj["reference"].split("/")[-1]
    return None

def extract_coding(coding_container: dict | list) -> dict:
    """
    Extracts coding information from a FHIR coding container.

    Args:
        coding_container: A FHIR element with coding (CodeableConcept or similar),
                        or a list of such elements

    Returns:
        dict: A dictionary with code, system, and display values if found
    """
    result = {
        "code": None,
        "system": None,
        "display": None
    }

    # Handle the case when a list is passed instead of a single container
    if isinstance(coding_container, list) and len(coding_container) > 0:
        # Take the first item from the list
        return extract_coding(coding_container[0])

    # Extract from text if available
    if isinstance(coding_container, dict) and coding_container.get("text"):
        result["display"] = coding_container.get("text")

    # Extract from coding array if available
    if isinstance(coding_container, dict) and coding_container.get("coding") and len(coding_container.get("coding", [])) > 0:
        coding = coding_container.get("coding", [])[0]
        result["code"] = coding.get("code")
        result["system"] = coding.get("system")

        # Use coding display if text wasn't available
        if not result["display"]:
            result["display"] = coding.get("display")

    return result