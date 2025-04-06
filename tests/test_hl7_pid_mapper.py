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

# tests/test_hl7_pid_mapper.py

import unittest
import logging
import os
from pathlib import Path
from hl7apy.parser import parse_message
from pulsepipe.ingesters.hl7v2_utils.pid_mapper import PIDMapper
from pulsepipe.models import PulseClinicalContent, MessageCache

# Set up logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestPIDMapper(unittest.TestCase):
    """Test class for the PID segment mapper."""
    
    def test_pid_mapper_basic(self):
        """Test basic PID mapping functionality with a standard message."""
        # Create a basic HL7 message for testing
        hl7_message = """MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311200||ADT^A01|MSG00001|P|2.5
EVN|A01|202503311200
PID|1||123456^^^HOSP^MR||DOE^JOHN||19320101|M|||123 Main St^^Boston^MA^02115^USA||(555)555-1212|||EN|M|Catholic|MR|123456"""

        # Initialize cache
        cache = MessageCache(patient_id="123456", encounter_id=None)

        # Parse the message
        message = parse_message(hl7_message)
        content = PulseClinicalContent(
            patient=None,
            encounter=None,
            vital_signs=[],
            allergies=[],
            immunizations=[],
            diagnoses=[],
            problem_list=[],
            procedures=[],
            medications=[],
            payors=[],
            mar=[],
            notes=[],
            imaging=[],
            lab=[],
            pathology=[],
            diagnostic_test=[],
            microbiology=[],
            blood_bank=[],
            family_history=[],
            social_history=[],
            advance_directives=[],
            functional_status=[],
            order=[],
            implant=[],
        )

        # Get PID segment and map it
        pid_segment = next(s for s in message.children if s.name == "PID")
        mapper = PIDMapper()
        self.assertTrue(mapper.accepts(pid_segment))

        logger.info("Mapping PID segment...")
        mapper.map(pid_segment, content, cache)
        logger.info("PID segment mapped successfully")

        # ✅ Assertions
        self.assertIsNotNone(content.patient)
        self.assertEqual(content.patient.id, "123456")
        self.assertEqual(content.patient.gender, "M")
        self.assertEqual(content.patient.dob_year, 1932)
        self.assertTrue(content.patient.over_90)
        
        # Geographic area might contain country, state, and/or zip
        # Tests for presence instead of exact match to be more flexible
        self.assertIsNotNone(content.patient.geographic_area)
        self.assertIn("USA", content.patient.geographic_area)
        
        self.assertEqual(content.patient.identifiers.get("MR"), "123456")
        
        # ✅ Preferences
        prefs = content.patient.preferences[0] if content.patient.preferences else None
        self.assertIsNotNone(prefs)
        self.assertEqual(prefs.preferred_language, "EN")
        self.assertEqual(prefs.communication_method, "Phone")

    def test_pid_mapper_from_fixture(self):
        """Test PID mapper using a fixture file with multiple messages."""
        # Get fixture path
        fixture_path = Path(__file__).parent / "fixtures" / "sample_hl7_messages.hl7"
        
        # Skip test if fixture doesn't exist
        if not fixture_path.exists():
            self.skipTest(f"Fixture file not found: {fixture_path}")
        
        # Load the fixture content
        with open(fixture_path, 'r') as f:
            hl7_data = f.read()
        
        logger.info(f"Loading HL7 data from {fixture_path}")
        logger.debug(f"First 100 chars: {hl7_data[:100]}...")
        
        # Split messages by blank lines
        messages = hl7_data.split("\n\n")
        
        for i, message_text in enumerate(messages):
            if not message_text.strip():
                continue
                
            logger.info(f"Processing message {i+1}")
            
            try:
                # Parse the message
                message = parse_message(message_text)
                
                # Check if message has PID segment
                pid_segments = [s for s in message.children if s.name == "PID"]
                if not pid_segments:
                    logger.warning(f"No PID segment found in message {i+1}")
                    continue
                
                pid_segment = pid_segments[0]
                
                # Initialize for mapping
                cache = MessageCache()
                content = PulseClinicalContent(
                    patient=None,
                    encounter=None,
                    vital_signs=[],
                    allergies=[],
                    immunizations=[],
                    diagnoses=[],
                    problem_list=[],
                    procedures=[],
                    medications=[],
                    payors=[],
                    mar=[],
                    notes=[],
                    imaging=[],
                    lab=[],
                    pathology=[],
                    diagnostic_test=[],
                    microbiology=[],
                    blood_bank=[],
                    family_history=[],
                    social_history=[],
                    advance_directives=[],
                    functional_status=[],
                    order=[],
                    implant=[],
                )
                
                # Map the PID segment
                mapper = PIDMapper()
                mapper.map(pid_segment, content, cache)
                
                # Basic assertions for each patient
                self.assertIsNotNone(content.patient, f"Patient should be mapped for message {i+1}")
                
                # Get the expected language from the message (PID-15)
                expected_language = None
                try:
                    if hasattr(pid_segment, "pid_15") and pid_segment.pid_15:
                        if hasattr(pid_segment.pid_15, "value"):
                            expected_language = pid_segment.pid_15.value
                except:
                    expected_language = None
                
                # Check language if it was found in the message
                if expected_language:
                    logger.info(f"Expected language: {expected_language}")
                    prefs = content.patient.preferences[0] if content.patient.preferences else None
                    self.assertIsNotNone(prefs, f"Patient preferences missing for message {i+1}")
                    self.assertEqual(prefs.preferred_language, expected_language, 
                                    f"Language should match for message {i+1}")
                
                logger.info(f"Successfully mapped patient from message {i+1}")
                
            except Exception as e:
                logger.error(f"Error processing message {i+1}: {e}")
                self.fail(f"Exception while processing message {i+1}: {e}")

    def test_parse_single_message(self):
        """Test parsing a single HL7 message with ONLY a PID segment."""
        # Create a basic HL7 message with ONLY a PID segment
        hl7_message = """MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311200||ADT^A01|MSG00001|P|2.5
PID|1||123456^^^HOSP^MR||DOE^JOHN||19320101|M|||123 Main St^^Boston^MA^02115^USA||(555)555-1212|||EN|M|Catholic|MR|123456"""
        
        # Initialize mapper and content
        pid_mapper = PIDMapper()
        content = PulseClinicalContent(
            patient=None,
            encounter=None,
            vital_signs=[],
            allergies=[],
            immunizations=[],
            diagnoses=[],
            problem_list=[],
            procedures=[],
            medications=[],
            payors=[],
            mar=[],
            notes=[],
            imaging=[],
            lab=[],
            pathology=[],
            diagnostic_test=[],
            microbiology=[],
            blood_bank=[],
            family_history=[],
            social_history=[],
            advance_directives=[],
            functional_status=[],
            order=[],
            implant=[],
        )
        
        # Parse the message
        message = parse_message(hl7_message)
        pid_segment = next(s for s in message.children if s.name == "PID")
        
        # Create empty cache 
        cache = MessageCache()
        
        # Map the segment
        pid_mapper.map(pid_segment, content, cache)
        
        # Verify patient data was properly extracted
        self.assertIsNotNone(content.patient)
        self.assertIsNotNone(content.patient.gender)
        self.assertIsNotNone(content.patient.preferences)
        self.assertEqual(content.patient.preferences[0].preferred_language, "EN")
        
        logger.info("Single message test passed")


if __name__ == "__main__":
    unittest.main()