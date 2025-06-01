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

# src/pulsepipe/ingesters/fhir_utils/claim_mapper.py

"""
PulsePipe — Claim Mapper for FHIR Resources
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from pulsepipe.models.billing import Claim, Charge
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, get_display

@fhir_mapper("Claim")
class ClaimMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Claim"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract basic claim information
        claim_id = resource.get("id")
        
        # Map FHIR status to our model status
        # FHIR uses: active, cancelled, draft, entered-in-error
        # Our model uses: submitted, accepted, denied, adjusted, paid
        raw_status = resource.get("status", "")
        claim_status = "submitted"  # Default
        
        # Map status values
        if raw_status == "active":
            claim_status = "accepted"
        elif raw_status == "cancelled":
            claim_status = "denied"
        elif raw_status == "entered-in-error":
            claim_status = "denied"
        
        # Extract claim type
        claim_type = None
        if resource.get("type", {}).get("coding"):
            for coding in resource["type"]["coding"]:
                if coding.get("code") == "institutional":
                    claim_type = "institutional"
                elif coding.get("code") == "professional":
                    claim_type = "professional"
                elif coding.get("code") == "oral":
                    claim_type = "dental"
                
        # Extract dates
        claim_date = resource.get("created")
        service_start = resource.get("billablePeriod", {}).get("start")
        service_end = resource.get("billablePeriod", {}).get("end")
        
        # Process date strings to datetime objects if present
        claim_date_obj = None
        service_start_obj = None
        service_end_obj = None
        
        if claim_date:
            try:
                claim_date_obj = datetime.fromisoformat(claim_date.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        if service_start:
            try:
                service_start_obj = datetime.fromisoformat(service_start.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
                
        if service_end:
            try:
                service_end_obj = datetime.fromisoformat(service_end.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        # Extract payer information
        payer_id = None
        if resource.get("insurer", {}).get("reference"):
            payer_id = resource["insurer"]["reference"].split("/")[-1]
        
        # Process line items (charges)
        charges = []
        total_charge_amount = Decimal('0.00')
        
        for item in resource.get("item", []):
            charge_code = None
            charge_description = None
            
            # Extract procedure/service codes
            if item.get("productOrService", {}).get("coding"):
                coding = item["productOrService"]["coding"][0]
                charge_code = coding.get("code")
                charge_description = coding.get("display") or item["productOrService"].get("text")
            
            # Extract amount
            quantity = item.get("quantity", {}).get("value", 1)
            unit_price = Decimal(str(item.get("unitPrice", {}).get("value", 0)))
            charge_amount = unit_price * Decimal(str(quantity))
            total_charge_amount += charge_amount
            
            # Extract diagnosis pointers
            diagnosis_pointers = []
            for diag_link in item.get("diagnosisLinkId", []):
                if isinstance(diag_link, str):
                    diagnosis_pointers.append(diag_link)
                elif isinstance(diag_link, dict) and "value" in diag_link:
                    diagnosis_pointers.append(diag_link["value"])
            
            # Create charge object
            charge = Charge(
                charge_id=f"{claim_id}-{item.get('sequence', '')}",
                encounter_id=encounter_id,
                patient_id=patient_id,
                service_date=service_start_obj,
                charge_code=charge_code or "",
                charge_description=charge_description,
                charge_amount=charge_amount,
                quantity=quantity,
                performing_provider_id=None,  # Could extract from careTeam if needed
                ordering_provider_id=None,
                revenue_code=None,
                cpt_hcpcs_code=charge_code,  # Assuming the code is a CPT/HCPCS code
                diagnosis_pointers=diagnosis_pointers if diagnosis_pointers else None,
                charge_status="posted",
                organization_id=None,
            )
            charges.append(charge)
        
        # Create Claim object
        claim = Claim(
            claim_id=claim_id,
            patient_id=patient_id,
            encounter_id=encounter_id,
            claim_date=claim_date_obj,
            payer_id=payer_id,
            total_charge_amount=total_charge_amount,
            total_payment_amount=Decimal('0.00'),  # Payments would come from ExplanationOfBenefit
            claim_status=claim_status,
            claim_type=claim_type,
            service_start_date=service_start_obj,
            service_end_date=service_end_obj,
            charges=charges,
            payments=None,  # Payments would come from ExplanationOfBenefit
            adjustments=None,  # Adjustments would come from ExplanationOfBenefit
            organization_id=None,
        )
        
        # FHIR Claims should be routed to operational content, not clinical content
        # For now, we need to create a way to signal that this should be operational data
        # TODO: Implement proper operational content routing for FHIR Claims
        # As a temporary solution, we'll add it to clinical but mark it for operational processing
        if not hasattr(content, "claims"):
            setattr(content, "claims", [])
        content.claims.append(claim)
