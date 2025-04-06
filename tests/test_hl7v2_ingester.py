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

# tests/test_hl7v2_ingester.py

import pytest
import logging
from pathlib import Path
from pulsepipe.ingesters.hl7v2_ingester import HL7v2Ingester

# Set up logging for tests
# Copy this code to the top of your test file (test_hl7v2_ingester.py)
# or create a debug_helper.py file and import it

import logging
import sys

# Configure root logger to show all messages
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Create console handler that prints to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create a formatter with detailed info
formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the root logger
root_logger.addHandler(console_handler)

# Set specific module loggers to DEBUG
for module in ['pulsepipe.ingesters.hl7v2_utils.pid_mapper', 
               'pulsepipe.ingesters.hl7v2_utils.obr_mapper',
               'pulsepipe.ingesters.hl7v2_utils.obx_mapper',
               'pulsepipe.ingesters.hl7v2_ingester']:
    module_logger = logging.getLogger(module)
    module_logger.setLevel(logging.DEBUG)

# Helper function for direct console output during tests
def debug_print(message):
    """Print debug message directly to console, even during pytest run"""
    print(f"\nDEBUG: {message}", flush=True)

class TestHL7v2Ingester:
    """Tests for the HL7v2 ingester class."""
    
    def test_empty_data(self):
        """Test that empty input raises appropriate error."""
        ingester = HL7v2Ingester()
        
        with pytest.raises(ValueError, match="Empty HL7v2 data received"):
            ingester.parse("")
            
        with pytest.raises(ValueError, match="Empty HL7v2 data received"):
            ingester.parse("   ")
            
        module_logger.info("Empty data test passed")
    
    def test_parse_single_message(self):
        """Test parsing a single simple HL7 message."""
        # Create a simple ADT message
        hl7_message = """MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311200||ADT^A01|MSG00001|P|2.4
PID|1||12345^^^HOSP^MR||SMITH^JOHN||19800101|M|||123 Main St^^Boston^MA^02115^USA||(555)555-1212|||EN"""
        
        # Parse with HL7v2 ingester
        ingester = HL7v2Ingester()
        content = ingester.parse(hl7_message)
        
        # Basic assertions
        assert content[0] is not None
        assert content[0].patient is not None
        assert content[0].patient.id == "12345"
        assert content[0].patient.gender == "M"
        
        # Check PID-15 language field
        preferences = content[0].patient.preferences[0] if content[0].patient.preferences else None
        assert preferences is not None
        assert preferences.preferred_language == "EN", "Language should be EN"
        
        module_logger.info(f"Successfully parsed patient: {content[0].patient.id}")
        module_logger.info("Single message test passed")
    
    def test_malformed_message(self):
        """Test handling of malformed HL7 messages."""
        # Create a malformed message with invalid format
        invalid_message = "This is not an HL7 message"
        
        # Parse with HL7v2 ingester - should raise ValueError
        ingester = HL7v2Ingester()
        
        with pytest.raises(ValueError):
            ingester.parse(invalid_message)
        
        # Now test a partially valid batch where only some messages are parseable
        partially_valid = """MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311200||ADT^A01|MSG00001|P|2.4
PID|1||123456^^^HOSP^MR||DOE^JOHN||19320101|M

This is an invalid segment that should be skipped

MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311215||ADT^A01|MSG00002|P|2.3
PID|1||234567^^^HOSP^MR||SMITH^JANE||19450215|F"""
        
        # This should parse without error, getting what it can from the valid parts
        content = ingester.parse(partially_valid)
        assert content[0] is not None
        print("\nHL7 Content: \n", content[0], "\n")
        assert content[0].patient is not None  # Should have extracted patient info from valid segments
        
        module_logger.info("Malformed message test passed")
    
    def test_vital_signs(self):
        """Test parsing vital signs from ORU messages."""
        # Create an HL7 vital signs message
        hl7_message = """MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311245||ORU^R01|VS12345|P|2.4
PID|1||456789^^^HOSP^MR||WILSON^MARY||19720315|F
OBR|1||VS5678|VS^VITAL SIGNS^L|||202503311240
OBX|1|NM|BP^BLOOD PRESSURE^L||120/80|mmHg|90-120/60-80|N|||F
OBX|2|NM|TEMP^TEMPERATURE^L||37.0|C|36.5-37.5|N|||F
OBX|3|NM|HR^HEART RATE^L||72|bpm|60-100|N|||F
OBX|4|NM|RR^RESPIRATORY RATE^L||16|/min|12-20|N|||F
OBX|5|NM|O2SAT^OXYGEN SATURATION^L||98|%|95-100|N|||F"""
        
        # Parse with HL7v2 ingester
        ingester = HL7v2Ingester()
        content = ingester.parse(hl7_message)
        
        # Assertions for vital signs
        assert content[0] is not None
        assert content[0].patient is not None
        assert len(content[0].vital_signs) > 0, "Vital signs should be parsed"
        
        # Log vital signs details
        module_logger.info(f"Parsed {len(content[0].vital_signs)} vital signs")
        for vs in content[0].vital_signs:
            module_logger.info(f"Vital sign: {vs.display}, value: {vs.value}, unit: {vs.unit}")
        
        # Check for specific vital signs
        assert any('BP' in vs.display or 'BLOOD PRESSURE' in vs.display for vs in content[0].vital_signs), \
            "Blood pressure vital sign should be present"
            
        assert any('TEMP' in vs.display or 'TEMPERATURE' in vs.display for vs in content[0].vital_signs), \
            "Temperature vital sign should be present"
            
        assert any('HR' in vs.display or 'HEART RATE' in vs.display for vs in content[0].vital_signs), \
            "Heart rate vital sign should be present"
        
        module_logger.info("Vital signs test passed")
    
    def test_parse_multiple_messages(self):
        """Test parsing multiple HL7 messages from fixture file."""
        # Load fixture file
        fixture_path = Path(__file__).parent / "fixtures" / "sample_hl7_messages.hl7"
        
        if not fixture_path.exists():
            pytest.skip(f"Fixture file not found: {fixture_path}")
        
        with open(fixture_path, 'r') as f:
            hl7_data = f.read()
        
        module_logger.info(f"Loaded fixture file: {fixture_path}")
        
        # Parse with HL7v2 ingester
        ingester = HL7v2Ingester()
        content = ingester.parse(hl7_data)
        
        # Basic assertions - should have merged content from all messages
        assert content[0] is not None
        assert content[0].patient is not None
        
        # Log summary of parsed data
        module_logger.info(content[0].summary())
        
        # Patient should be from one of the messages
        assert content[0].patient.id in ["123456", "234567", "345678", "456789"]
        
        # We should have lab data and vital signs from the ORU messages
        module_logger.info(f"Lab reports: {len(content[0].lab)}")
        module_logger.info(f"Vital signs: {len(content[0].vital_signs)}")
        print("!!!!Lab Result\n", content[2])
        # Verify lab results are present
        assert len(content[2].lab) > 0, "Should have lab results from the CBC in ORU message"

        # Find CBC lab results 
        has_cbc_results = False
        for lab_report in content[2].lab:
            for observation in lab_report.observations:
                observation_name = str(observation.name).upper() if observation.name else ""
                observation_code = str(observation.code).upper() if observation.code else ""
                if any(term in observation_name or term in observation_code 
                    for term in ["CBC", "WBC", "RBC", "HGB", "HEMOGLOBIN"]):
                    print(f"Found CBC-related result: {observation.name} ({observation.code})")
                    has_cbc_results = True
                    break

        assert has_cbc_results, "CBC lab results should be present"
        
        # Check vital signs
        if len(content[3].vital_signs) > 0:
            # At least some vital signs were mapped
            vital_names = [vs.display for vs in content[3].vital_signs]
            module_logger.info(f"Vital sign names: {vital_names}")
        
        module_logger.info("Multiple message test passed")
