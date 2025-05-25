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

# tests/test_operational_content.py

import pytest
from decimal import Decimal
from datetime import datetime
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.models.billing import Claim, Payment, Charge, Adjustment
from pulsepipe.models.drg import DRG
from pulsepipe.models.prior_authorization import PriorAuthorization


class TestPulseOperationalContent:
    """Test suite for PulseOperationalContent model."""

    def test_basic_initialization(self):
        """Test basic model initialization with default values."""
        content = PulseOperationalContent()
        
        assert content.transaction_type is None
        assert content.interchange_control_number is None
        assert content.functional_group_control_number is None
        assert content.organization_id is None
        assert content.drgs == []
        assert content.claims == []
        assert content.charges == []
        assert content.payments == []
        assert content.adjustments == []
        assert content.prior_authorizations == []

    def test_initialization_with_all_fields(self):
        """Test initialization with all fields populated."""
        content = PulseOperationalContent(
            transaction_type="837P",
            interchange_control_number="123456789",
            functional_group_control_number="987654321",
            organization_id="ORG001",
            drgs=[],
            claims=[],
            charges=[],
            payments=[],
            adjustments=[],
            prior_authorizations=[]
        )
        
        assert content.transaction_type == "837P"
        assert content.interchange_control_number == "123456789"
        assert content.functional_group_control_number == "987654321"
        assert content.organization_id == "ORG001"

    def test_summary_empty_content(self):
        """Test summary method with completely empty content (line 161)."""
        content = PulseOperationalContent()
        summary = content.summary()
        assert summary == "❌ No operational content found"

    def test_summary_with_transaction_type_835(self):
        """Test summary with 835 (payment) transaction type."""
        content = PulseOperationalContent(transaction_type="835")
        summary = content.summary()
        assert "💵 Transaction: 835" in summary

    def test_summary_with_transaction_type_278(self):
        """Test summary with 278 (prior auth) transaction type."""
        content = PulseOperationalContent(transaction_type="278")
        summary = content.summary()
        assert "🔐 Transaction: 278" in summary

    def test_summary_with_unknown_transaction_type(self):
        """Test summary with unknown transaction type."""
        content = PulseOperationalContent(transaction_type="999")
        summary = content.summary()
        assert "📊 Transaction: 999" in summary

    def test_summary_with_control_numbers(self):
        """Test summary with control numbers."""
        content = PulseOperationalContent(
            interchange_control_number="ICN123",
            functional_group_control_number="GCN456"
        )
        summary = content.summary()
        assert "🔢 ICN: ICN123 | GCN: GCN456" in summary

    def test_summary_with_organization_id(self):
        """Test summary with organization ID."""
        content = PulseOperationalContent(organization_id="ORG001")
        summary = content.summary()
        assert "🏢 Org: ORG001" in summary

    def test_summary_with_entity_counts(self):
        """Test summary with non-zero entity counts."""
        drg = DRG(drg_code="123")
        claim = Claim(claim_id="C001", total_charge_amount=Decimal("100.00"))
        charge = Charge(charge_id="CH001", charge_code="99213", charge_amount=Decimal("50.00"))
        payment = Payment(payment_id="P001", payment_amount=Decimal("75.00"))
        adjustment = Adjustment(adjustment_id="A001", adjustment_amount=Decimal("25.00"))
        prior_auth = PriorAuthorization()
        
        content = PulseOperationalContent(
            drgs=[drg],
            claims=[claim],
            charges=[charge],
            payments=[payment],
            adjustments=[adjustment],
            prior_authorizations=[prior_auth]
        )
        
        summary = content.summary()
        assert "🏥 1 Drgs" in summary
        assert "💼 1 Claims" in summary
        assert "🧾 1 Charges" in summary
        assert "💵 1 Payments" in summary
        assert "📝 1 Adjustments" in summary
        assert "🔐 1 Prior Authorizations" in summary

    def test_summary_with_claims_total_billed(self):
        """Test summary calculation with claims that have total_billed attribute (lines 126-128, 131)."""
        # Test by directly modifying the claims list after creation to add objects with total_billed
        content = PulseOperationalContent()
        
        # Create simple objects with total_billed attribute
        class MockClaim:
            def __init__(self, total_billed):
                self.total_billed = total_billed
        
        # Directly set the claims list to bypass validation
        content.claims = [MockClaim(Decimal("150.00")), MockClaim(Decimal("250.00"))]
        
        summary = content.summary()
        assert "Billed: $400.00" in summary

    def test_summary_with_claims_without_total_billed(self):
        """Test summary with claims that don't have total_billed attribute."""
        claim = Claim(claim_id="C001", total_charge_amount=Decimal("100.00"))
        content = PulseOperationalContent(claims=[claim])
        summary = content.summary()
        # Should not include billed amount since hasattr(claim, "total_billed") is False
        assert "Billed:" not in summary

    def test_summary_with_claims_none_total_billed(self):
        """Test summary with claims that have None total_billed."""
        content = PulseOperationalContent()
        
        class MockClaimWithNone:
            def __init__(self):
                self.total_billed = None
        
        content.claims = [MockClaimWithNone()]
        summary = content.summary()
        # Should not include billed amount since total_billed is None
        assert "Billed:" not in summary

    def test_summary_with_payments_amount(self):
        """Test summary calculation with payments that have amount attribute (lines 137-139, 142)."""
        content = PulseOperationalContent()
        
        class MockPayment:
            def __init__(self, amount):
                self.amount = amount
        
        content.payments = [MockPayment(Decimal("75.00")), MockPayment(Decimal("125.00"))]
        summary = content.summary()
        assert "Paid: $200.00" in summary

    def test_summary_with_payments_without_amount(self):
        """Test summary with payments that don't have amount attribute."""
        payment = Payment(payment_id="P001", payment_amount=Decimal("100.00"))
        content = PulseOperationalContent(payments=[payment])
        summary = content.summary()
        # Should not include paid amount since hasattr(payment, "amount") is False
        assert "Paid:" not in summary

    def test_summary_with_payments_none_amount(self):
        """Test summary with payments that have None amount."""
        content = PulseOperationalContent()
        
        class MockPaymentWithNone:
            def __init__(self):
                self.amount = None
        
        content.payments = [MockPaymentWithNone()]
        summary = content.summary()
        # Should not include paid amount since amount is None
        assert "Paid:" not in summary

    def test_summary_with_drg_payment_amount(self):
        """Test summary calculation with DRGs that have payment_amount."""
        drg1 = DRG(drg_code="123", payment_amount=Decimal("1000.00"))
        drg2 = DRG(drg_code="456", payment_amount=Decimal("1500.00"))
        
        content = PulseOperationalContent(drgs=[drg1, drg2])
        summary = content.summary()
        assert "DRG Expected: $2,500.00" in summary

    def test_summary_with_drg_without_payment_amount(self):
        """Test summary with DRGs that don't have payment_amount."""
        drg = DRG(drg_code="123")
        content = PulseOperationalContent(drgs=[drg])
        summary = content.summary()
        # Should not include DRG expected amount since payment_amount is None
        assert "DRG Expected:" not in summary

    def test_summary_all_financial_totals(self):
        """Test summary with all types of financial totals."""
        content = PulseOperationalContent()
        
        class MockClaim:
            def __init__(self):
                self.total_billed = Decimal("300.00")
        
        class MockPayment:
            def __init__(self):
                self.amount = Decimal("200.00")
        
        drg = DRG(drg_code="123", payment_amount=Decimal("1000.00"))
        
        content.claims = [MockClaim()]
        content.payments = [MockPayment()]
        content.drgs = [drg]
        
        summary = content.summary()
        assert "💰 Billed: $300.00 | Paid: $200.00 | DRG Expected: $1,000.00" in summary

    def test_summary_comprehensive(self):
        """Test comprehensive summary with all fields populated."""
        drg = DRG(drg_code="123", payment_amount=Decimal("1000.00"))
        charge = Charge(charge_id="CH001", charge_code="99213", charge_amount=Decimal("50.00"))
        adjustment = Adjustment(adjustment_id="A001", adjustment_amount=Decimal("25.00"))
        prior_auth = PriorAuthorization()
        
        class MockClaim:
            def __init__(self):
                self.total_billed = Decimal("500.00")
        
        class MockPayment:
            def __init__(self):
                self.amount = Decimal("400.00")
        
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="ICN123",
            functional_group_control_number="GCN456",
            organization_id="ORG001"
        )
        
        # Set the lists directly to bypass validation
        content.drgs = [drg]
        content.claims = [MockClaim()]
        content.charges = [charge]
        content.payments = [MockPayment()]
        content.adjustments = [adjustment]
        content.prior_authorizations = [prior_auth]
        
        summary = content.summary()
        
        # Check all components are present
        assert summary.startswith("✅")
        assert "💵 Transaction: 835" in summary
        assert "🔢 ICN: ICN123 | GCN: GCN456" in summary
        assert "🏢 Org: ORG001" in summary
        assert "🏥 1 Drgs" in summary
        assert "💼 1 Claims" in summary
        assert "🧾 1 Charges" in summary
        assert "💵 1 Payments" in summary
        assert "📝 1 Adjustments" in summary
        assert "🔐 1 Prior Authorizations" in summary
        assert "💰 Billed: $500.00 | Paid: $400.00 | DRG Expected: $1,000.00" in summary

    def test_summary_name_formatting(self):
        """Test that entity names are properly formatted from snake_case to Title Case."""
        prior_auth = PriorAuthorization()
        content = PulseOperationalContent(prior_authorizations=[prior_auth])
        summary = content.summary()
        # "prior_authorizations" should become "Prior Authorizations"
        assert "🔐 1 Prior Authorizations" in summary

    def test_summary_multiple_drgs_zero_amount(self):
        """Test summary with multiple DRGs where some have zero payment_amount."""
        drg1 = DRG(drg_code="123", payment_amount=Decimal("0.00"))
        drg2 = DRG(drg_code="456", payment_amount=Decimal("1000.00"))
        
        content = PulseOperationalContent(drgs=[drg1, drg2])
        summary = content.summary()
        assert "DRG Expected: $1,000.00" in summary

    def test_summary_edge_case_all_none_amounts(self):
        """Test summary when all amounts are None."""
        claim = Claim(claim_id="C001", total_charge_amount=Decimal("100.00"))
        payment = Payment(payment_id="P001", payment_amount=Decimal("100.00"))
        drg = DRG(drg_code="123")
        
        content = PulseOperationalContent(
            claims=[claim],
            payments=[payment], 
            drgs=[drg]
        )
        
        summary = content.summary()
        # Should not include any financial totals
        assert "💰" not in summary
        assert "Billed:" not in summary
        assert "Paid:" not in summary
        assert "DRG Expected:" not in summary