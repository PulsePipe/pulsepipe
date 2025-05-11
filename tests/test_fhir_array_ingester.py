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

# tests/test_fhir_array_ingester.py

import os
import json
import pytest
from pulsepipe.ingesters.fhir_ingester import FHIRIngester
from pulsepipe.models.clinical_content import PulseClinicalContent

class TestFHIRArrayIngester:
    """Test the FHIR ingester with array input."""
    
    @pytest.fixture
    def fhir_ingester(self):
        """Set up a FHIR ingester instance."""
        return FHIRIngester()
    
    @pytest.fixture
    def sample_array_data(self):
        """Load sample FHIR bundles array from file."""
        fixture_path = os.path.join("tests", "fixtures", "simple_patient_bundle_array.json")
        if not os.path.exists(fixture_path):
            # For CI environments where directory structure might be different
            fixture_path = os.path.join("fixtures", "simple_patient_bundle_array.json")
        
        with open(fixture_path, "r") as f:
            return f.read()
    
    def test_array_ingestion(self, fhir_ingester, sample_array_data):
        """Test ingesting an array of FHIR bundles."""
        # Process the test data
        result = fhir_ingester.parse(sample_array_data)
        
        # Verify we got a list of results
        assert isinstance(result, list), "Expected a list of results"
        
        # Verify we have the correct number of results (2 bundles in the test file)
        assert len(result) == 2, f"Expected 2 results, got {len(result)}"
        
        # Verify each result is a PulseClinicalContent object
        for item in result:
            assert isinstance(item, PulseClinicalContent), f"Expected PulseClinicalContent, got {type(item)}"
        
        # Verify patient information in each result
        patient1 = result[0].patient
        patient2 = result[1].patient
        
        assert patient1 is not None, "First bundle should have patient data"
        assert patient1.id == "patient-001", f"Incorrect patient ID: {patient1.id}"
        assert patient1.gender == "male", f"Incorrect gender: {patient1.gender}"
        
        assert patient2 is not None, "Second bundle should have patient data"
        assert patient2.id == "patient-002", f"Incorrect patient ID: {patient2.id}"
        assert patient2.gender == "female", f"Incorrect gender: {patient2.gender}"
        
        # Verify other data elements are populated
        for i, item in enumerate(result):
            # Check if vital signs were processed
            assert len(item.vital_signs) > 0, f"Bundle {i+1} should have vital signs"
            
            # Check if allergies were processed
            assert len(item.allergies) > 0, f"Bundle {i+1} should have allergies"
            
            # Check if immunizations were processed
            assert len(item.immunizations) > 0, f"Bundle {i+1} should have immunizations"
            
            # Check if diagnostic reports were processed
            assert len(item.lab) > 0 or len(item.diagnostic_test) > 0, f"Bundle {i+1} should have diagnostic reports"
    
    def test_single_item_in_array(self, fhir_ingester):
        """Test processing an array with a single FHIR bundle."""
        # Create a single-item array
        single_bundle = json.loads("""
        [
            {
                "resourceType": "Bundle",
                "type": "collection",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Patient",
                            "id": "single-test",
                            "name": [{ "family": "Test", "given": ["Single"] }],
                            "gender": "male",
                            "birthDate": "1980-01-01"
                        }
                    }
                ]
            }
        ]
        """)
        
        # Convert to JSON string and process
        result = fhir_ingester.parse(json.dumps(single_bundle))
        
        # Verify we got a list with one item
        assert isinstance(result, list), "Expected a list of results"
        assert len(result) == 1, f"Expected 1 result, got {len(result)}"
        
        # Verify the patient data
        patient = result[0].patient
        assert patient is not None, "Should have patient data"
        assert patient.id == "single-test", f"Incorrect patient ID: {patient.id}"
        assert patient.gender == "male", f"Incorrect gender: {patient.gender}"
