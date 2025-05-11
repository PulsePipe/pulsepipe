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

# src/pulsepipe/ingesters/fhir_utils/supply_delivery_mapper.py

"""
PulsePipe — SupplyDelivery Mapper for FHIR Resources
"""

from pulsepipe.models.supply_delivery import SupplyDelivery, SupplyDeliveryItem
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, get_display

@fhir_mapper("SupplyDelivery")
class SupplyDeliveryMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "SupplyDelivery"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        # If supply_deliveries field doesn't exist, create it
        if not hasattr(content, "supply_deliveries"):
            setattr(content, "supply_deliveries", [])
            
        # Parse and add the supply delivery
        supply_delivery = self.parse_supply_delivery(resource, cache)
        content.supply_deliveries.append(supply_delivery)
    
    def parse_supply_delivery(self, resource: dict, cache: MessageCache) -> SupplyDelivery:
        # Extract basic identifiers
        delivery_id = resource.get("id")
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract status
        status = resource.get("status")  # in-progress, completed, abandoned, etc.
        
        # Extract delivery type
        delivery_type = None
        if resource.get("type", {}).get("coding"):
            for coding in resource["type"]["coding"]:
                if coding.get("display"):
                    delivery_type = coding["display"]
                    break
                elif coding.get("code"):
                    delivery_type = coding["code"]
                    break
        elif resource.get("type", {}).get("text"):
            delivery_type = resource["type"]["text"]
        
        # Extract delivery date
        delivered_on = None
        if resource.get("occurrenceDateTime"):
            delivered_on = resource["occurrenceDateTime"]
        elif resource.get("occurrencePeriod", {}).get("start"):
            delivered_on = resource["occurrencePeriod"]["start"]
        elif resource.get("occurrenceTiming", {}).get("event"):
            # Take the first event if it's a list of events
            if isinstance(resource["occurrenceTiming"]["event"], list):
                delivered_on = resource["occurrenceTiming"]["event"][0]
            else:
                delivered_on = resource["occurrenceTiming"]["event"]
        
        # Extract destination (location or facility)
        destination = None
        if resource.get("destination", {}).get("reference"):
            destination_ref = resource["destination"]["reference"]
            destination = destination_ref.split("/")[-1]
        
        # Extract supplier
        supplier = None
        if resource.get("supplier", {}).get("reference"):
            supplier_ref = resource["supplier"]["reference"]
            supplier = supplier_ref.split("/")[-1]
        
        # Extract notes
        notes = None
        if resource.get("note") and len(resource["note"]) > 0:
            note_texts = []
            for note in resource["note"]:
                if note.get("text"):
                    note_texts.append(note["text"])
            
            if note_texts:
                notes = "; ".join(note_texts)
        
        # Extract supply items
        items = []
        
        # Check if we have a suppliedItem
        if resource.get("suppliedItem"):
            item = resource["suppliedItem"]
            
            # Extract item code and name
            item_code = None
            item_name = None
            coding_method = None
            
            # Check if item is a CodeableConcept
            if item.get("itemCodeableConcept", {}).get("coding"):
                for coding in item["itemCodeableConcept"]["coding"]:
                    if not item_code and coding.get("code"):
                        item_code = coding["code"]
                        coding_method = coding.get("system")
                    
                    if not item_name and coding.get("display"):
                        item_name = coding["display"]
                
                # Fallback to text if no display found
                if not item_name and item["itemCodeableConcept"].get("text"):
                    item_name = item["itemCodeableConcept"]["text"]
            
            # Check if item is a reference
            elif item.get("itemReference", {}).get("reference"):
                item_ref = item["itemReference"]["reference"]
                item_id = item_ref.split("/")[-1]
                item_name = item["itemReference"].get("display")
            
            # Extract quantity
            quantity = None
            quantity_unit = None
            if item.get("quantity", {}).get("value"):
                quantity = str(item["quantity"]["value"])
                quantity_unit = item["quantity"].get("unit", "")
            
            items.append(SupplyDeliveryItem(
                item_code=item_code,
                item_name=item_name,
                coding_method=coding_method,
                quantity=quantity,
                quantity_unit=quantity_unit,
                lot_number=None,  # Not directly available in FHIR SupplyDelivery
                expiration_date=None  # Not directly available in FHIR SupplyDelivery
            ))
        
        # Create and return the SupplyDelivery
        return SupplyDelivery(
            delivery_id=delivery_id,
            status=status,
            delivery_type=delivery_type,
            delivered_on=delivered_on,
            destination=destination,
            supplier=supplier,
            items=items,
            notes=notes,
            patient_id=patient_id,
            encounter_id=encounter_id
        )