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

# src/pulsepipe/models/operational_content.py

from pydantic import BaseModel
from typing import Optional, List
from .billing import Claim, Charge, Payment, Adjustment
from .prior_authorization import PriorAuthorization
from .drg import DRG

class PulseOperationalContent(BaseModel):
    """
    Comprehensive container for administrative and financial healthcare data.
    
    This model serves as the core operational data structure in PulsePipe,
    capturing administrative transactions related to healthcare billing, 
    payment processing, claims submission, and prior authorizations.
    
    It standardizes data from various electronic data interchange (EDI) 
    formats, particularly X12 transactions such as:
    - 837 (Claims)
    - 835 (Payment/Remittance)
    - 278 (Prior Authorization)
    - 270/271 (Eligibility)
    - 276/277 (Claim Status)
    
    This structure supports analytics, revenue cycle management, and AI-driven
    insights into the business operations of healthcare organizations.
    """
    transaction_type: Optional[str] = None  # e.g., '837P', '835', '278'
    interchange_control_number: Optional[str] = None
    functional_group_control_number: Optional[str] = None
    organization_id: Optional[str] = None
    drgs: List[DRG] = []
    claims: List[Claim] = []
    charges: List[Charge] = []
    payments: List[Payment] = []
    adjustments: List[Adjustment] = []
    prior_authorizations: List[PriorAuthorization] = []
    
    # De-identification status
    deidentified: bool = False
    
    def summary(self) -> str:
        """
        Generate a human-friendly summary of the operational content.
        
        Returns:
            str: A formatted summary string with emoji indicators
        """
        summary_parts = []
        
        # Transaction info
        if self.transaction_type:
            trans_emoji = {
                "837": "📤",        # Claims submission
                "837P": "📤",
                "837I": "📤",
                "837D": "📤",
                "835": "💵",        # Payment/remittance
                "278": "🔐",        # Prior authorization
                "270": "❓",         # Eligibility inquiry
                "271": "✅",        # Eligibility response
                "276": "❓",         # Claim status inquiry
                "277": "📋",        # Claim status response
                "834": "👥",        # Enrollment
                "820": "💰",        # Premium payment
            }.get(self.transaction_type, "📊")
            
            summary_parts.append(f"{trans_emoji} Transaction: {self.transaction_type}")
        
        # Control numbers (only if they exist)
        control_info = []
        if self.interchange_control_number:
            control_info.append(f"ICN: {self.interchange_control_number}")
        if self.functional_group_control_number:
            control_info.append(f"GCN: {self.functional_group_control_number}")
        
        if control_info:
            summary_parts.append(f"🔢 {' | '.join(control_info)}")
        
        # Organization
        if self.organization_id:
            summary_parts.append(f"🏢 Org: {self.organization_id}")
        
        # Financial data summary
        entity_counts = {
            "drgs": ("🏥", len(self.drgs)),
            "claims": ("💼", len(self.claims)),
            "charges": ("🧾", len(self.charges)),
            "payments": ("💵", len(self.payments)),
            "adjustments": ("📝", len(self.adjustments)),
            "prior_authorizations": ("🔐", len(self.prior_authorizations))
        }
        
        # Add entity counts but only if they're non-zero
        for name, (emoji, count) in entity_counts.items():
            if count > 0:
                # Format name for display (snake_case to Title Case)
                display_name = " ".join(word.capitalize() for word in name.split("_"))
                summary_parts.append(f"{emoji} {count} {display_name}")
        
        # Financial totals (if we can calculate them)
        totals = []
        
        # Calculate total billed amount if available
        billed_amount = 0
        has_billed = False
        for claim in self.claims:
            if hasattr(claim, "total_billed") and claim.total_billed is not None:
                billed_amount += claim.total_billed
                has_billed = True
        
        if has_billed:
            totals.append(f"Billed: ${billed_amount:,.2f}")
        
        # Calculate total paid amount if available
        paid_amount = 0
        has_paid = False
        for payment in self.payments:
            if hasattr(payment, "amount") and payment.amount is not None:
                paid_amount += payment.amount
                has_paid = True
        
        if has_paid:
            totals.append(f"Paid: ${paid_amount:,.2f}")
        
        # Calculate DRG-based expected reimbursement if available
        drg_based_amount = 0
        has_drg_payment = False
        for drg in self.drgs:
            if hasattr(drg, "payment_amount") and drg.payment_amount is not None:
                drg_based_amount += drg.payment_amount
                has_drg_payment = True
        
        if has_drg_payment:
            totals.append(f"DRG Expected: ${drg_based_amount:,.2f}")
        
        # Add totals if available
        if totals:
            summary_parts.append(f"💰 {' | '.join(totals)}")
        
        # If no content, provide default message
        if not summary_parts:
            return "❌ No operational content found"
        
        return "✅ " + " | ".join(summary_parts)
