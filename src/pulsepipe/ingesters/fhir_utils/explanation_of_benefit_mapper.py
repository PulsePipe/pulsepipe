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

# src/pulsepipe/ingesters/fhir_utils/explanation_of_benefit_mapper.py

"""
PulsePipe — ExplanationOfBenefit Mapper for FHIR Resources
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from pulsepipe.models.billing import Claim, Charge, Payment, Adjustment
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("ExplanationOfBenefit")
class ExplanationOfBenefitMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "ExplanationOfBenefit"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        # Process the ExplanationOfBenefit
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract the claim reference to link to the relevant claim
        claim_id = None
        if resource.get("claim", {}).get("reference"):
            claim_id = resource["claim"]["reference"].split("/")[-1]
        
        # Extract payer information
        payer_id = None
        if resource.get("insurer", {}).get("reference"):
            payer_id = resource["insurer"]["reference"].split("/")[-1]
        
        # Extract EOB ID
        eob_id = resource.get("id")
        
        # Extract payment information
        payment_info = resource.get("payment", {})
        payment_date_str = payment_info.get("date")
        payment_date = None
        if payment_date_str:
            try:
                payment_date = datetime.fromisoformat(payment_date_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        payment_amount = Decimal('0.00')
        if payment_info.get("amount", {}).get("value"):
            payment_amount = Decimal(str(payment_info["amount"]["value"]))
        
        # Create a payment object
        payment = None
        if payment_amount > Decimal('0.00'):
            payment = Payment(
                payment_id=f"{eob_id}-payment",
                patient_id=patient_id,
                encounter_id=encounter_id,
                charge_id=None,  # We don't have a direct link to the charge
                payer_id=payer_id,
                payment_date=payment_date,
                payment_amount=payment_amount,
                payment_type="insurance",
                check_number=payment_info.get("identifier", {}).get("value"),
                remit_advice_code=None,
                remit_advice_description=None,
                organization_id=None
            )
        
        # Process adjustments from the adjudication
        adjustments = []
        
        # Process total adjustments
        for total in resource.get("total", []):
            category_coding = total.get("category", {}).get("coding", [{}])[0]
            category_code = category_coding.get("code")
            category_display = category_coding.get("display")
            
            if category_code in ["benefit", "deductible", "copay", "coinsurance", "noncovered"]:
                adjustment_type = category_code
                adjustment_desc = category_display
                
                amount_value = Decimal('0.00')
                if total.get("amount", {}).get("value"):
                    amount_value = Decimal(str(total["amount"]["value"]))
                
                # Only create adjustment if there's an amount
                if amount_value > Decimal('0.00'):
                    adjustments.append(Adjustment(
                        adjustment_id=f"{eob_id}-adj-{category_code}",
                        charge_id=None,
                        payment_id=payment.payment_id if payment else None,
                        adjustment_date=payment_date,
                        adjustment_reason_code=category_code,
                        adjustment_reason_description=adjustment_desc,
                        adjustment_amount=amount_value,
                        adjustment_type=adjustment_type,
                        organization_id=None
                    ))
        
        # Process line-item adjustments
        for item in resource.get("item", []):
            for adjudication in item.get("adjudication", []):
                category_coding = adjudication.get("category", {}).get("coding", [{}])[0]
                category_code = category_coding.get("code")
                category_display = category_coding.get("display")
                
                if category_code in ["contractual", "deductible", "copay", "coinsurance", "noncovered"]:
                    adjustment_type = category_code
                    adjustment_desc = category_display
                    
                    amount_value = Decimal('0.00')
                    if adjudication.get("amount", {}).get("value"):
                        amount_value = Decimal(str(adjudication["amount"]["value"]))
                    
                    # Only create adjustment if there's an amount
                    if amount_value > Decimal('0.00'):
                        sequence = item.get("sequence")
                        adjustments.append(Adjustment(
                            adjustment_id=f"{eob_id}-item-{sequence}-adj-{category_code}",
                            charge_id=None,
                            payment_id=payment.payment_id if payment else None,
                            adjustment_date=payment_date,
                            adjustment_reason_code=category_code,
                            adjustment_reason_description=adjustment_desc,
                            adjustment_amount=amount_value,
                            adjustment_type=adjustment_type,
                            organization_id=None
                        ))
        
        # Find or create the target claim
        if claim_id:
            # Check if claims list exists, create it if it doesn't
            if not hasattr(content, "claims"):
                setattr(content, "claims", [])
            
            # Find the target claim
            target_claim = None
            for claim in content.claims:
                if claim.claim_id == claim_id:
                    target_claim = claim
                    break
            
            # If claim exists, update it with payment and adjustment info
            if target_claim:
                # Initialize lists if they don't exist
                if target_claim.payments is None:
                    target_claim.payments = []
                
                if target_claim.adjustments is None:
                    target_claim.adjustments = []
                
                # Add payment if it exists
                if payment:
                    target_claim.payments.append(payment)
                    # Update total payment amount
                    target_claim.total_payment_amount += payment_amount
                
                # Add adjustments
                for adjustment in adjustments:
                    target_claim.adjustments.append(adjustment)
                
                # Update claim status
                if resource.get("status") == "active":
                    target_claim.claim_status = "paid"
                
            # If claim doesn't exist, create a new one
            else:
                # Create a minimal claim object with payment info
                new_claim = Claim(
                    claim_id=claim_id,
                    patient_id=patient_id,
                    encounter_id=encounter_id,
                    claim_date=None,  # We don't have the original claim date
                    payer_id=payer_id,
                    total_charge_amount=Decimal('0.00'),  # We don't have the original charge amount
                    total_payment_amount=payment_amount if payment else Decimal('0.00'),
                    claim_status="paid" if resource.get("status") == "active" else resource.get("status"),
                    claim_type=None,  # We don't know the claim type
                    service_start_date=None,
                    service_end_date=None,
                    charges=None,
                    payments=[payment] if payment else None,
                    adjustments=adjustments if adjustments else None,
                    organization_id=None
                )
                
                content.claims.append(new_claim)