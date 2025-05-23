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

# src/pulsepipe/ingesters/fhir_utils/medication_mapper.py

"""
PulsePipe — Medication Mapper
Handles both Medication resources and MedicationStatement resources
"""

from pulsepipe.models import Medication, PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, get_display

@fhir_mapper("MedicationStatement")
class MedicationStatementMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "MedicationStatement"
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        content.medications.append(self.parse_medication(resource, cache))

    def parse_medication(self, resource: dict, cache: MessageCache) -> Medication:
        med_codeable = resource.get("medicationCodeableConcept", {})
        dosage_list = resource.get("dosage", [])

        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")

        # We'll take the first dosage if present
        dose = None
        dose_unit = None
        route = None
        frequency = None
        if dosage_list:
            dosage = dosage_list[0]

            # Get dose value and unit
            dose_data = dosage.get("doseAndRate", [{}])[0].get("doseQuantity", {})
            dose = dose_data.get("value")
            dose_unit = dose_data.get("unit")

            route = dosage.get("route", {}).get("text")
            frequency = dosage.get("timing", {}).get("repeat", {}).get("frequency")

            # For route, try to get display from coding if text is not available
            if not route and dosage.get("route", {}).get("coding"):
                for coding in dosage["route"]["coding"]:
                    if coding.get("display"):
                        route = coding["display"]
                        break

        # Get code and coding method directly from coding to match tests
        code = None
        coding_method = None
        if med_codeable.get("coding") and len(med_codeable["coding"]) > 0:
            code = med_codeable["coding"][0].get("code")
            coding_method = med_codeable["coding"][0].get("system")

        return Medication(
            code=code,
            coding_method=coding_method,
            name=med_codeable.get("text"),
            dose=f"{dose} {dose_unit}".strip() if dose else None,
            route=route,
            frequency=str(frequency) if frequency else None,
            start_date=resource.get("effectiveDateTime"),
            end_date=None,  # Optional: may not exist
            status=resource.get("status"),
            patient_id=patient_id,
            encounter_id=encounter_id,
            notes=None,  # Add notes field to match model
        )


@fhir_mapper("Medication")
class MedicationResourceMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Medication"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        For standalone Medication resources (as opposed to MedicationStatement or MedicationRequest),
        we'll add a simplified entry to the medications list.
        """
        # Create a basic Medication object with available info
        medication = self.parse_medication(resource)
        
        # Add to content's medications list
        content.medications.append(medication)
    
    def parse_medication(self, resource: dict) -> Medication:
        """
        Parse a Medication resource into a simplified Medication model
        """
        # Extract code information directly from coding to match tests
        code = None
        coding_method = None
        if resource.get("code", {}).get("coding") and len(resource["code"]["coding"]) > 0:
            code = resource["code"]["coding"][0].get("code")
            coding_method = resource["code"]["coding"][0].get("system")
        
        # Extract medication name
        name = resource.get("code", {}).get("text")
        if not name and resource.get("code", {}).get("coding"):
            for coding in resource["code"]["coding"]:
                if coding.get("display"):
                    name = coding["display"]
                    break
        
        # Get form information if available
        form = None
        if resource.get("form", {}).get("coding"):
            for coding in resource["form"]["coding"]:
                if coding.get("display"):
                    form = coding["display"]
                    break
        
        # Check status
        status = resource.get("status")
        
        # Extract a description from ingredients if available
        notes = None
        ingredients = []
        for ingredient in resource.get("ingredient", []):
            ingredient_name = None
            
            # Check if it's a coded ingredient or a reference
            if ingredient.get("itemCodeableConcept", {}).get("coding"):
                for coding in ingredient["itemCodeableConcept"]["coding"]:
                    if coding.get("display"):
                        ingredient_name = coding["display"]
                        break
            
            # Get strength if available
            strength = None
            if ingredient.get("strength", {}).get("numerator") and ingredient.get("strength", {}).get("denominator"):
                num = ingredient["strength"]["numerator"]
                denom = ingredient["strength"]["denominator"]
                
                num_value = num.get("value")
                num_unit = num.get("unit", "")
                denom_value = denom.get("value")
                denom_unit = denom.get("unit", "")
                
                if num_value and denom_value:
                    strength = f"{num_value} {num_unit}/{denom_value} {denom_unit}"
                elif num_value:
                    strength = f"{num_value} {num_unit}"
            
            if ingredient_name:
                ingredients.append(f"{ingredient_name} {strength if strength else ''}")
        
        # Build additional information from ingredients
        if ingredients:
            notes = f"Ingredients: {'; '.join(ingredients)}"
        
        # Create Medication object
        return Medication(
            code=code,
            coding_method=coding_method,
            name=name,
            dose=None,
            route=form,
            frequency=None,
            start_date=None,
            end_date=None,
            status=status,
            patient_id=None,
            encounter_id=None,
            notes=notes,
        )
