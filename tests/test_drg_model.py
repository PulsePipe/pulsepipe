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

# tests/test_drg_model.py

import unittest
from decimal import Decimal
import pytest
from pydantic import ValidationError

from pulsepipe.models.drg import DRG
from pulsepipe.models.operational_content import PulseOperationalContent

class TestDRGModel(unittest.TestCase):
    """Test cases for the DRG model."""
    
    def test_basic_initialization(self):
        """Test basic initialization with minimal fields."""
        drg = DRG(
            drg_code="470",
            drg_description="Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity without MCC"
        )
        
        self.assertEqual(drg.drg_code, "470")
        self.assertEqual(drg.drg_description, "Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity without MCC")
        self.assertIsNone(drg.drg_type)
        self.assertIsNone(drg.drg_version)
        self.assertIsNone(drg.relative_weight)
        self.assertIsNone(drg.severity_of_illness)
        self.assertIsNone(drg.risk_of_mortality)
        
    def test_full_initialization(self):
        """Test initialization with all fields."""
        drg = DRG(
            drg_code="292",
            drg_description="Heart Failure and Shock with CC",
            drg_type="MS-DRG",
            drg_version="38",
            relative_weight=Decimal("1.2329"),
            severity_of_illness=2,
            risk_of_mortality=3,
            average_length_of_stay=Decimal("4.2"),
            geometric_mean_length_of_stay=Decimal("3.8"),
            mdc_code="05",
            mdc_description="Diseases and Disorders of the Circulatory System",
            principal_diagnosis_code="I50.9",
            procedure_codes=["0JH602Z", "5A1221Z"],
            complication_flag=True,
            payment_amount=Decimal("12583.76"),
            patient_id="PAT12345",
            encounter_id="ENC67890"
        )
        
        self.assertEqual(drg.drg_code, "292")
        self.assertEqual(drg.drg_description, "Heart Failure and Shock with CC")
        self.assertEqual(drg.drg_type, "MS-DRG")
        self.assertEqual(drg.drg_version, "38")
        self.assertEqual(drg.relative_weight, Decimal("1.2329"))
        self.assertEqual(drg.severity_of_illness, 2)
        self.assertEqual(drg.risk_of_mortality, 3)
        self.assertEqual(drg.average_length_of_stay, Decimal("4.2"))
        self.assertEqual(drg.geometric_mean_length_of_stay, Decimal("3.8"))
        self.assertEqual(drg.mdc_code, "05")
        self.assertEqual(drg.mdc_description, "Diseases and Disorders of the Circulatory System")
        self.assertEqual(drg.principal_diagnosis_code, "I50.9")
        self.assertEqual(drg.procedure_codes, ["0JH602Z", "5A1221Z"])
        self.assertTrue(drg.complication_flag)
        self.assertEqual(drg.payment_amount, Decimal("12583.76"))
        self.assertEqual(drg.patient_id, "PAT12345")
        self.assertEqual(drg.encounter_id, "ENC67890")
        
    def test_validation_constraints(self):
        """Test validation constraints on fields."""
        # Test relative_weight >= 0
        with self.assertRaises(ValidationError):
            DRG(drg_code="470", relative_weight=Decimal("-1.5"))
            
        # Test severity_of_illness between 1 and 4
        with self.assertRaises(ValidationError):
            DRG(drg_code="470", severity_of_illness=0)
            
        with self.assertRaises(ValidationError):
            DRG(drg_code="470", severity_of_illness=5)
            
        # Test risk_of_mortality between 1 and 4
        with self.assertRaises(ValidationError):
            DRG(drg_code="470", risk_of_mortality=0)
            
        with self.assertRaises(ValidationError):
            DRG(drg_code="470", risk_of_mortality=5)
            
        # Test payment_amount >= 0
        with self.assertRaises(ValidationError):
            DRG(drg_code="470", payment_amount=Decimal("-100.00"))
    
    def test_integration_with_operational_content(self):
        """Test integration with PulseOperationalContent."""
        drg1 = DRG(
            drg_code="470",
            drg_description="Major Joint Replacement or Reattachment of Lower Extremity without MCC",
            drg_type="MS-DRG",
            payment_amount=Decimal("11838.65")
        )
        
        drg2 = DRG(
            drg_code="291",
            drg_description="Heart Failure and Shock with MCC",
            drg_type="MS-DRG",
            payment_amount=Decimal("8792.43")
        )
        
        operational_content = PulseOperationalContent(
            transaction_type="837I",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG100",
            drgs=[drg1, drg2]
        )
        
        # Test that DRGs are correctly stored
        self.assertEqual(len(operational_content.drgs), 2)
        self.assertEqual(operational_content.drgs[0].drg_code, "470")
        self.assertEqual(operational_content.drgs[1].drg_code, "291")
        
        # Test summary method
        summary = operational_content.summary()
        self.assertIn("🏥 2 Drgs", summary)
        self.assertIn("DRG Expected: $20,631.08", summary)
    
    def test_drg_type_validation(self):
        """Test drg_type field validation."""
        # Valid DRG types
        valid_types = ["MS-DRG", "AP-DRG", "APR-DRG"]
        
        for drg_type in valid_types:
            drg = DRG(
                drg_code="193", 
                drg_description="Simple Pneumonia and Pleurisy with MCC", 
                drg_type=drg_type
            )
            self.assertEqual(drg.drg_type, drg_type)

if __name__ == "__main__":
    unittest.main()
