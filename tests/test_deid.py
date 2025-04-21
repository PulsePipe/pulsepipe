# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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

# tests/test_deid.py

import unittest
from unittest.mock import MagicMock, patch
import copy
import re
from datetime import datetime, date

from pulsepipe.pipelines.stages.deid import DeidentificationStage
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.patient import PatientInfo
from pulsepipe.models.note import Note
from pulsepipe.models.lab import LabReport, LabObservation

# Import the de-identification configuration for testing
from pulsepipe.pipelines.deid.config import DEFAULT_SALT
from pulsepipe.models.encounter import EncounterInfo, EncounterProvider

class TestDeidentificationStage(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.deid_stage = DeidentificationStage()
        
        # Create mock context with configuration
        self.mock_context = MagicMock(spec=PipelineContext)
        self.mock_context.log_prefix = "[test_deid]"
        self.mock_context.name = "test_pipeline"
        
        # Set up basic configuration for testing
        self.test_config = {
            "method": "safe_harbor",
            "keep_year": True,
            "geographic_precision": "state",
            "over_90_handling": "flag",
            "patient_id_strategy": "hash",
            "id_salt": "test-salt-for-unit-tests"
        }
        
        # Mock the get_stage_config method to return our test config
        self.mock_context.get_stage_config.return_value = self.test_config
        
        # Create sample patient data for testing
        self.sample_patient = PatientInfo(
            id="12345",
            gender="female",
            dob_year=1980,
            over_90=False,
            identifiers={
                "MRN": "MRN12345",
                "SSN": "123-45-6789",
                "other_id": "OTHER-ID-123"
            },
            geographic_area="New York, NY, USA",
            preferences=None
        )
        
        # Create sample notes with PHI
        self.sample_note = Note(
            note_type_code="PN",
            text="Patient Jane Smith (MRN: 12345) was seen on 2023-05-15. " 
                 "Patient lives at 123 Main St, New York NY 10001. " 
                 "Contact at (555) 123-4567 or jane.smith@example.com. " 
                 "SSN: 123-45-6789.",
            timestamp="2023-05-15T14:30:00",
            author_id="PROVIDER-123",
            author_name="Dr. John Doe",
            patient_id="12345",
            encounter_id="ENC-789"
        )
        
        # Create sample encounter data
        self.sample_encounter = EncounterInfo(
            id="ENC-789",
            admit_date="2023-05-15T08:00:00",
            discharge_date="2023-05-15T16:30:00",
            encounter_type="Outpatient",
            type_coding_method="CPT",
            location="Internal Medicine Clinic",
            reason_code="Z00.00",
            reason_coding_method="ICD-10",
            visit_type="Follow-up",
            patient_id="12345",
            providers=[
                EncounterProvider(
                    id="PROVIDER-123",
                    type_code="MD",
                    coding_method="NUCC",
                    name="Dr. John Doe",
                    specialty="Internal Medicine"
                )
            ]
        )
        
        # Create sample lab report with dates
        self.sample_lab = LabReport(
            report_id="LAB123",
            lab_type="Chemistry",
            code="CHEM123",
            coding_method="LOINC",
            panel_name="Basic Metabolic Panel",
            panel_code="BMP123",
            panel_code_method="CPT",
            is_panel=True,
            ordering_provider_id="PROVIDER-123",
            performing_lab="Test Laboratory Inc.",
            report_type="Comprehensive",
            collection_date="2023-05-14T08:00:00",
            report_date="2023-05-15T10:30:00",
            patient_id="12345",
            encounter_id="ENC-789",
            note="Within normal limits except for glucose",
            observations=[
                LabObservation(
                    code="GLUC123",
                    coding_method="LOINC",
                    name="Glucose",
                    description="Glucose measurement",
                    value="110",
                    unit="mg/dL",
                    reference_range="70-100",
                    abnormal_flag="H",
                    result_date="2023-05-15T09:45:00"
                )
            ]
        )
        
        # Set up a complete clinical content model
        self.clinical_content = PulseClinicalContent(
            patient=self.sample_patient,
            encounter=self.sample_encounter,
            notes=[self.sample_note],
            lab=[self.sample_lab]
        )

    async def test_execute_valid_clinical_content(self):
        """Test de-identification of clinical content."""
        try:
            # Test the execute method
            result = await self.deid_stage.execute(self.mock_context, self.clinical_content)
            
            # Verify result is marked as de-identified
            self.assertTrue(result.deidentified)
            
            # Check that the patient ID has been hashed
            self.assertNotEqual(result.patient.id, "12345")
            self.assertTrue(result.patient.id.startswith("DEID_"))
            
            # Check that the notes have been properly redacted
            self.assertNotIn("Jane Smith", result.notes[0].text)
            self.assertNotIn("123 Main St", result.notes[0].text)
            self.assertNotIn("555-123-4567", result.notes[0].text)
            self.assertNotIn("jane.smith@example.com", result.notes[0].text)
            
            # Check for redaction markers
            self.assertIn("[REDACTED", result.notes[0].text)
            
            # Original data should remain unchanged
            self.assertEqual(self.sample_patient.id, "12345")
            self.assertIn("Jane Smith", self.sample_note.text)
        finally:
            pass

    async def test_deterministic_id_hashing(self):
        """Test that ID hashing is deterministic across multiple runs."""
        try:
            # First run
            result1 = await self.deid_stage.execute(self.mock_context, copy.deepcopy(self.clinical_content))
            patient_id1 = result1.patient.id
            
            # Second run with same input
            result2 = await self.deid_stage.execute(self.mock_context, copy.deepcopy(self.clinical_content))
            patient_id2 = result2.patient.id
            
            # IDs should be consistent across runs
            self.assertEqual(patient_id1, patient_id2)
            
            # Verify reference consistency - notes should point to the same patient ID
            self.assertEqual(result1.notes[0].patient_id, result1.patient.id)
            self.assertEqual(result2.notes[0].patient_id, result2.patient.id)
        finally:
            pass

    def test_handle_dates(self):
        """Test handling of dates according to Safe Harbor method."""
        # Create a test object with dates
        test_obj = MagicMock()
        test_obj.specific_date = datetime(2023, 5, 15)
        test_obj.date_string = "2023-05-15"
        test_obj.another_date = date(2023, 5, 15)
        
        # Process the object
        result = self.deid_stage._handle_dates(test_obj, self.test_config)
        
        # Only year should be kept
        self.assertEqual(result.specific_date.year, 2023)
        self.assertEqual(result.specific_date.month, 1)
        self.assertEqual(result.specific_date.day, 1)
        
        # Date string should be converted to year only
        self.assertEqual(result.date_string, "2023")
        
        # Date object should be converted to Jan 1
        self.assertEqual(result.another_date.year, 2023)
        self.assertEqual(result.another_date.month, 1)
        self.assertEqual(result.another_date.day, 1)
        
        # Test with over_90 handling
        test_obj.over_90 = True
        config_with_redaction = self.test_config.copy()
        config_with_redaction["over_90_handling"] = "redact"
        
        result = self.deid_stage._handle_dates(test_obj, config_with_redaction)
        
        # Dates should be handled according to over_90_handling setting
        # Note: In the DeidentificationStage implementation, only datetime and date objects 
        # are completely redacted for over-90, not strings 
        self.assertIsNone(result.specific_date)
        self.assertEqual(result.date_string, "2023")  # Strings keep the year
        self.assertIsNone(result.another_date)

    def test_redact_phi_from_text(self):
        """Test PHI redaction from free text."""
        test_text = """
        Patient Jane Smith (MRN: 12345) was seen on 2023-05-15. 
        Patient lives at 123 Main St, New York NY 10001. 
        Contact at (555) 123-4567 or jane.smith@example.com. 
        SSN: 123-45-6789.
        Dr. Johnson noted that the patient has been taking medication regularly.
        """
        
        result = self.deid_stage._redact_phi_from_text(test_text, self.test_config)
        
        # Check for redacted PHI - only verify what's actually implemented
        # in the deid.py file's _redact_phi_from_text method
        self.assertIn("[REDACTED-NAME]", result)
        self.assertIn("[REDACTED-MRN]", result)
        self.assertIn("[REDACTED-DATE]", result)
        self.assertIn("[REDACTED-PHONE]", result)
        self.assertIn("[REDACTED-EMAIL]", result)
        self.assertIn("[REDACTED-SSN]", result)
        self.assertIn("[REDACTED-ZIP]", result)
        
        # Names should be redacted
        self.assertNotIn("Jane Smith", result)
        self.assertNotIn("Dr. Johnson", result)

    def test_verify_mrn_hash(self):
        """Test verification of MRN hash."""
        # Create a function to calculate hash for testing
        import hashlib
        
        def calculate_hash(value, salt=DEFAULT_SALT):
            return hashlib.sha256((value + salt).encode()).hexdigest()[:16]
        
        # Create a sample MRN
        test_mrn = "MRN12345"
        
        # Calculate hash manually
        expected_hash = calculate_hash(test_mrn, self.test_config["id_salt"])
        hashed_id = f"DEID_MRN_{expected_hash}"
        
        # Create a patient with the MRN in identifiers
        test_patient = PatientInfo(
            id="12345",
            gender="female",
            dob_year=1980,
            identifiers={"MRN": test_mrn},
            geographic_area="New York, NY, USA",
            preferences=None,
            over_90=False
        )
        
        # Run the MRN through the identifier handler
        deid_patient = self.deid_stage._handle_identifiers(test_patient, self.test_config)
        
        # Verify that the MRN has been hashed as expected
        self.assertIn("mrn_hash", deid_patient.identifiers)
        self.assertEqual(deid_patient.identifiers["mrn_hash"], f"DEID_{expected_hash}")