import pytest
from unittest.mock import patch
from decimal import Decimal
from datetime import datetime
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.models.billing import Claim, Charge, Payment, Adjustment
from pulsepipe.models.prior_authorization import PriorAuthorization

class TestPulseOperationalContent:
    def test_init_empty(self):
        # Create with all optional fields
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None
        )
        
        assert content.transaction_type is None
        assert content.interchange_control_number is None
        assert content.functional_group_control_number is None
        assert content.organization_id is None
        assert content.claims == []
        assert content.charges == []
        assert content.payments == []
        assert content.adjustments == []
        assert content.prior_authorizations == []
    
    def test_init_with_values(self):
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG123"
        )
        
        assert content.transaction_type == "835"
        assert content.interchange_control_number == "12345"
        assert content.functional_group_control_number == "67890"
        assert content.organization_id == "ORG123"
    
    def test_summary_empty(self):
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None
        )
        summary = content.summary()
        
        assert summary == "❌ No operational content found"
    
    def test_summary_with_transaction_type(self):
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None
        )
        summary = content.summary()
        
        assert "💵 Transaction: 835" in summary
    
    def test_summary_with_control_numbers(self):
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id=None
        )
        summary = content.summary()
        
        assert "🔢 ICN: 12345 | GCN: 67890" in summary
    
    def test_summary_with_organization(self):
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id="ORG123"
        )
        summary = content.summary()
        
        assert "🏢 Org: ORG123" in summary
    
    def test_summary_with_claims(self):
        # Create a minimal valid Claim
        claim = Claim(
            claim_id="CLAIM123",
            patient_id=None,
            encounter_id=None,
            claim_date=None,
            payer_id=None,
            total_charge_amount=Decimal("100.00"),
            claim_status=None,
            claim_type=None,
            service_start_date=None,
            service_end_date=None,
            charges=None,
            payments=None,
            adjustments=None,
            organization_id=None
        )
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None,
            claims=[claim]
        )
        summary = content.summary()
        
        assert "💼 1 Claims" in summary
    
    def test_summary_with_charges(self):
        # Create a minimal valid Charge
        charge = Charge(
            charge_id="CHG123",
            encounter_id=None,
            patient_id=None,
            service_date=None,
            charge_code="CODE456",
            charge_description=None,
            charge_amount=Decimal("50.00"),
            quantity=None,
            performing_provider_id=None,
            ordering_provider_id=None,
            revenue_code=None,
            cpt_hcpcs_code=None,
            diagnosis_pointers=None,
            charge_status=None,
            organization_id=None
        )
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None,
            charges=[charge]
        )
        summary = content.summary()
        
        assert "🧾 1 Charges" in summary
    
    def test_summary_with_payments(self):
        # Create a minimal valid Payment
        payment = Payment(
            payment_id="PAY123",
            patient_id=None,
            encounter_id=None,
            charge_id=None,
            payer_id=None,
            payment_date=None,
            payment_amount=Decimal("75.00"),
            payment_type=None,
            check_number=None,
            remit_advice_code=None,
            remit_advice_description=None,
            organization_id=None
        )
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None,
            payments=[payment]
        )
        summary = content.summary()
        
        # Only check for the count of payments since the formatting 
        # of financial amounts might vary in the implementation
        assert "💵 1 Payments" in summary
    
    def test_summary_with_adjustments(self):
        # Create a minimal valid Adjustment
        adjustment = Adjustment(
            adjustment_id="ADJ123",
            charge_id=None,
            payment_id=None,
            adjustment_date=None,
            adjustment_reason_code=None,
            adjustment_reason_description=None,
            adjustment_amount=Decimal("25.00"),
            adjustment_type=None,
            organization_id=None
        )
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None,
            adjustments=[adjustment]
        )
        summary = content.summary()
        
        assert "📝 1 Adjustments" in summary
    
    def test_summary_with_prior_authorizations(self):
        # Create a minimal valid PriorAuthorization
        auth = PriorAuthorization(
            auth_id="AUTH123",
            patient_id=None,
            provider_id=None,
            requested_procedure=None,
            auth_type=None,
            review_status=None,
            service_dates=None,
            diagnosis_codes=None,
            organization_id=None
        )
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number=None,
            functional_group_control_number=None,
            organization_id=None,
            prior_authorizations=[auth]
        )
        summary = content.summary()
        
        assert "🔐 1 Prior Authorizations" in summary
    
    def test_summary_with_different_transaction_types(self):
        # Test different transaction type emojis
        transaction_types = {
            "837": "📤",
            "837P": "📤",
            "835": "💵",
            "278": "🔐",
            "270": "❓",
            "271": "✅",
            "unknown": "📊"  # Default for unknown types
        }
        
        for t_type, emoji in transaction_types.items():
            content = PulseOperationalContent(
                transaction_type=t_type,
                interchange_control_number=None,
                functional_group_control_number=None,
                organization_id=None
            )
            summary = content.summary()
            assert f"{emoji} Transaction: {t_type}" in summary