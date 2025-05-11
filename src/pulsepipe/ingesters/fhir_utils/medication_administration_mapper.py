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

# src/pulsepipe/ingesters/fhir_utils/medication_administration_mapper.py

"""
PulsePipe — MedicationAdministration Mapper for FHIR Resources
"""

from pulsepipe.models.mar import MAR
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, extract_effective_date

@fhir_mapper("MedicationAdministration")
class MedicationAdministrationMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "MedicationAdministration"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        mar_entry = self.parse_medication_administration(resource, cache)
        content.mar.append(mar_entry)
    
    def parse_medication_administration(self, resource: dict, cache: MessageCache) -> MAR:
        # Extract basic identifiers
        mar_id = resource.get("id")
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract status
        status = resource.get("status")  # completed, in-progress, not-done, on-hold, etc.
        
        # Extract administration date
        administration_date = None
        if resource.get("effectiveDateTime"):
            administration_date = resource["effectiveDateTime"]
        elif resource.get("effectivePeriod", {}).get("start"):
            administration_date = resource["effectivePeriod"]["start"]
        
        # Extract medication information
        medication_name = None
        medication_code = None
        coding_method = None
        
        # Check if medication is a reference or CodeableConcept
        if resource.get("medicationCodeableConcept"):
            med_codeable = resource["medicationCodeableConcept"]
            medication_code = get_code(med_codeable)
            coding_method = get_system(med_codeable)
            medication_name = med_codeable.get("text")
            
            if not medication_name and med_codeable.get("coding"):
                for coding in med_codeable["coding"]:
                    if coding.get("display"):
                        medication_name = coding["display"]
                        break
        
        # Check for medication reference
        elif resource.get("medicationReference", {}).get("reference"):
            med_ref = resource["medicationReference"]["reference"]
            med_id = med_ref.split("/")[-1]
            
            # Just use the display text from the reference
            medication_name = resource["medicationReference"].get("display")
        
        # Extract dosage information
        dose = None
        route = None
        
        if resource.get("dosage", {}).get("dose", {}).get("value"):
            dose_value = resource["dosage"]["dose"]["value"]
            dose_unit = resource["dosage"]["dose"].get("unit", "")
            dose = f"{dose_value} {dose_unit}".strip()
        
        if resource.get("dosage", {}).get("route", {}).get("coding"):
            for coding in resource["dosage"]["route"]["coding"]:
                if coding.get("display"):
                    route = coding["display"]
                    break
                elif coding.get("code"):
                    route = coding["code"]
                    break
        elif resource.get("dosage", {}).get("route", {}).get("text"):
            route = resource["dosage"]["route"]["text"]
        
        # Extract performer/administrator information
        administrator_id = None
        for performer in resource.get("performer", []):
            if performer.get("actor", {}).get("reference"):
                actor_ref = performer["actor"]["reference"]
                if "Practitioner" in actor_ref:
                    administrator_id = actor_ref.split("/")[-1]
                    break
        
        # Extract reason for administration
        reason = None
        if resource.get("reasonCode") and len(resource["reasonCode"]) > 0:
            for coding in resource["reasonCode"][0].get("coding", []):
                if coding.get("display"):
                    reason = coding["display"]
                    break
            
            # If no coding display, try text
            if not reason and resource["reasonCode"][0].get("text"):
                reason = resource["reasonCode"][0]["text"]
        
        # Extract notes/comments
        notes = None
        if resource.get("note") and len(resource["note"]) > 0:
            note_texts = []
            for note in resource["note"]:
                if note.get("text"):
                    note_texts.append(note["text"])
            
            if note_texts:
                notes = "; ".join(note_texts)
        
        # Create MAR entry
        return MAR(
            medication_code=medication_code,
            coding_method=coding_method,
            medication=medication_name,  # This matches the required field in MAR model
            dosage=dose or "",  # Ensure required field is not None
            route=route,
            frequency="",  # Required field, set to empty string if None
            administered_at=administration_date or "",  # Required field
            status=status,
            notes=notes,
            patient_id=patient_id,
            encounter_id=encounter_id
        )