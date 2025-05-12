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

# src/pulsepipe/ingesters/fhir_utils/medication_request_mapper.py

"""
PulsePipe — MedicationRequest Mapper for FHIR Resources
"""

from pulsepipe.models.medication import Medication
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, get_display

@fhir_mapper("MedicationRequest")
class MedicationRequestMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "MedicationRequest"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        # Map the MedicationRequest to a Medication model
        medication = self.parse_medication_request(resource, cache)
        content.medications.append(medication)
    
    def parse_medication_request(self, resource: dict, cache: MessageCache) -> Medication:
        # Extract basic identifiers
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract medication information (can be a reference or CodeableConcept)
        medication_name = None
        medication_code = None
        coding_method = None

        # Check if medication is a CodeableConcept
        if resource.get("medicationCodeableConcept"):
            med_codeable = resource["medicationCodeableConcept"]

            # Get code directly from coding to match tests
            if med_codeable.get("coding") and len(med_codeable["coding"]) > 0:
                medication_code = med_codeable["coding"][0].get("code")
                coding_method = med_codeable["coding"][0].get("system")

            medication_name = med_codeable.get("text")
            
            if not medication_name and med_codeable.get("coding"):
                for coding in med_codeable["coding"]:
                    if coding.get("display"):
                        medication_name = coding["display"]
                        break
        
        # Check if medication is a reference
        elif resource.get("medicationReference", {}).get("reference"):
            med_ref = resource["medicationReference"]["reference"]
            med_id = med_ref.split("/")[-1]
            
            # Just use the display text from the reference
            medication_name = resource["medicationReference"].get("display")
        
        # Extract dosage instructions
        dosage_list = resource.get("dosageInstruction", [])
        
        # We'll take the first dosage instruction if present
        dose = None
        route = None
        frequency = None
        
        if dosage_list:
            dosage = dosage_list[0]
            
            # Extract dose
            if dosage.get("doseAndRate"):
                dose_info = dosage["doseAndRate"][0]
                if dose_info.get("doseQuantity", {}).get("value"):
                    dose_value = dose_info["doseQuantity"]["value"]
                    dose_unit = dose_info["doseQuantity"].get("unit", "")
                    dose = f"{dose_value} {dose_unit}".strip()
            
            # Extract route - prioritize text to match tests
            route = dosage.get("route", {}).get("text")

            # Only if text is not available, try to get from coding
            if not route and dosage.get("route", {}).get("coding"):
                for coding in dosage["route"]["coding"]:
                    if coding.get("display"):
                        route = coding["display"]
                        break
                    elif coding.get("code"):
                        route = coding["code"]
                        break
            
            # Extract frequency
            # First check for code-based timing or text
            if dosage.get("timing", {}).get("code"):
                timing_code = dosage["timing"]["code"]
                # First try text, which is most human readable
                if timing_code.get("text"):
                    frequency = timing_code["text"]
                # Then try coding
                elif timing_code.get("coding"):
                    for coding in timing_code["coding"]:
                        if coding.get("display"):
                            frequency = coding["display"]
                            break
                        elif coding.get("code"):
                            frequency = coding["code"]
                            break

            # Fallback to repeat info if no code-based frequency
            elif dosage.get("timing", {}).get("repeat"):
                repeat = dosage["timing"]["repeat"]
                # Simple frequency value
                if repeat.get("frequency"):
                    frequency = str(repeat["frequency"])
                
                
        
        # Extract dates
        start_date = None
        end_date = None
        
        # Check for explicit start/end dates
        if resource.get("dispenseRequest", {}).get("validityPeriod"):
            validity = resource["dispenseRequest"]["validityPeriod"]
            start_date = validity.get("start")
            end_date = validity.get("end")
        
        # Fallback to authoring date as start date
        if not start_date and resource.get("authoredOn"):
            start_date = resource["authoredOn"]
        
        # Extract status
        status = resource.get("status")  # active, completed, cancelled, etc.
        
        # Create Medication object
        return Medication(
            code=medication_code,
            coding_method=coding_method,
            name=medication_name,
            dose=dose,
            route=route,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            status=status,
            patient_id=patient_id,
            encounter_id=encounter_id,
            notes=None  # Add notes field to match updated model
        )