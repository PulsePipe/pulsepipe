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

"""Unit tests for the pipeline context functionality."""

import os
import json
import uuid
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.models.clinical_content import PulseClinicalContent


class TestPipelineContext(unittest.TestCase):
    """Tests for the PipelineContext class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "ingester": {
                "type": "fhir",
                "enabled": True
            },
            "chunker": {
                "type": "clinical",
                "enabled": True
            },
            "embedding": {
                "type": "clinical",
                "enabled": False
            },
            "vectorstore": {
                "engine": "qdrant",
                "host": "localhost"
            }
        }
        self.context = PipelineContext(
            name="test_pipeline",
            config=self.config,
            output_path="/tmp/output.json",
            summary=True,
            verbose=True
        )
    
    def test_initialization(self):
        """Test that context initializes correctly."""
        self.assertEqual(self.context.name, "test_pipeline")
        self.assertEqual(self.context.config, self.config)
        self.assertEqual(self.context.output_path, "/tmp/output.json")
        self.assertEqual(self.context.summary, True)
        self.assertEqual(self.context.verbose, True)
        self.assertIsNotNone(self.context.pipeline_id)
        self.assertIsNotNone(self.context.start_time)
        self.assertEqual(self.context.errors, [])
        self.assertEqual(self.context.warnings, [])
        self.assertEqual(self.context.executed_stages, [])
    
    def test_stage_tracking(self):
        """Test that stage execution is tracked correctly."""
        # Start and end a stage
        self.context.start_stage("ingestion")
        self.assertIn("ingestion", self.context.stage_timings)
        self.assertIsNotNone(self.context.stage_timings["ingestion"]["start"])
        self.assertIsNone(self.context.stage_timings["ingestion"]["end"])
        
        # End the stage
        mock_result = [{"patient": "data"}]
        self.context.end_stage("ingestion", mock_result)
        self.assertIsNotNone(self.context.stage_timings["ingestion"]["end"])
        self.assertIsNotNone(self.context.stage_timings["ingestion"]["duration"])
        self.assertEqual(self.context.ingested_data, mock_result)
        self.assertIn("ingestion", self.context.executed_stages)
    
    def test_error_and_warning_tracking(self):
        """Test error and warning recording."""
        self.context.add_error("ingestion", "Failed to parse data", {"file": "test.hl7"})
        self.context.add_warning("chunking", "No chunker specified", {"defaulting": "to clinical"})
        
        self.assertEqual(len(self.context.errors), 1)
        self.assertEqual(len(self.context.warnings), 1)
        self.assertEqual(self.context.errors[0]["stage"], "ingestion")
        self.assertEqual(self.context.warnings[0]["stage"], "chunking")
        self.assertEqual(self.context.errors[0]["details"]["file"], "test.hl7")
    
    def test_get_stage_config(self):
        """Test retrieving configuration for different stages."""
        # Direct configuration
        ingester_config = self.context.get_stage_config("ingester")
        self.assertEqual(ingester_config["type"], "fhir")
        
        # Alternate name
        ingestion_config = self.context.get_stage_config("ingestion")
        self.assertEqual(ingestion_config["type"], "fhir")
        
        # Missing configuration
        deid_config = self.context.get_stage_config("deid")
        self.assertEqual(deid_config, {})
        
        # Vectorstore configuration
        vectorstore_config = self.context.get_stage_config("vectorstore")
        self.assertEqual(vectorstore_config["engine"], "qdrant")
    
    def test_is_stage_enabled(self):
        """Test checking if stages are enabled."""
        # Explicitly enabled
        self.assertTrue(self.context.is_stage_enabled("ingestion"))
        
        # Explicitly disabled
        self.assertFalse(self.context.is_stage_enabled("embedding"))
        
        # Implicitly enabled by config presence
        self.assertTrue(self.context.is_stage_enabled("chunking"))
        
        # Not in config
        self.assertFalse(self.context.is_stage_enabled("deid"))
    
    def test_get_output_path_for_stage(self):
        """Test generating output paths for stages."""
        ingestion_path = self.context.get_output_path_for_stage("ingestion")
        self.assertEqual(ingestion_path, "/tmp/output_ingestion.json")
        
        suffixed_path = self.context.get_output_path_for_stage("chunking", "clinical")
        self.assertEqual(suffixed_path, "/tmp/output_chunking_clinical.json")
        
        # No output path configured
        context_no_output = PipelineContext("test", self.config)
        self.assertIsNone(context_no_output.get_output_path_for_stage("ingestion"))
    
    @patch("os.makedirs")
    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    def test_export_results(self, mock_open, mock_makedirs):
        """Test exporting results to file."""
        # Test exporting a list of dictionaries
        data = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        self.context.export_results(data, "ingestion", "json")
        
        # Check proper directory creation
        mock_makedirs.assert_called_once_with(os.path.dirname(os.path.abspath("/tmp/output_ingestion.json")), exist_ok=True)
        
        # Check file opened with correct path
        mock_open.assert_called_once_with("/tmp/output_ingestion.json", "w", encoding='utf-8')
        
        # Test exporting Pydantic model
        from pulsepipe.models.patient import PatientInfo, PatientPreferences
        from pulsepipe.models.encounter import EncounterInfo, EncounterProvider
        
        clinical_content = PulseClinicalContent(
            patient=PatientInfo(
                id="123",
                dob_year=1980,
                gender="male",
                geographic_area="Los Angeles",
                preferences=[PatientPreferences(
                    preferred_language="English", 
                    communication_method="Phone", 
                    requires_interpreter=False, 
                    preferred_contact_time="Morning", 
                    notes="Test notes"
                )]
            ),
            encounter=EncounterInfo(
                id="E1",
                admit_date="2023-01-01",
                discharge_date="2023-01-05",
                encounter_type="Outpatient",
                type_coding_method="ICD-10",
                location="Main Hospital",
                reason_code="Z00.00",
                reason_coding_method="ICD-10",
                providers=[],
                visit_type="Follow-up",
                patient_id="123"
            )
        )
        mock_open.reset_mock()
        mock_makedirs.reset_mock()
        
        # Use mock_json instead of trying to patch the model_dump_json method
        with patch('json.dump') as mock_json:
            self.context.export_results(clinical_content, "patient", "json")
            
        mock_open.assert_called_once_with("/tmp/output_patient.json", "w", encoding='utf-8')
        
        # No output configured
        context_no_output = PipelineContext("test", self.config)
        context_no_output.export_results(data)  # Should not raise an exception
    
    def test_get_summary(self):
        """Test generating execution summary."""
        # Set up stage timing data
        self.context.start_stage("ingestion")
        self.context.end_stage("ingestion", [{"data": "example"}])
        
        self.context.start_stage("chunking")
        self.context.end_stage("chunking", [{"chunk1": "data"}, {"chunk2": "data"}])
        
        # Add some errors and warnings
        self.context.add_error("ingestion", "Test error")
        self.context.add_warning("chunking", "Test warning")
        
        # Get summary
        summary = self.context.get_summary()
        
        # Verify summary data
        self.assertEqual(summary["name"], "test_pipeline")
        self.assertEqual(summary["executed_stages"], ["ingestion", "chunking"])
        self.assertEqual(summary["error_count"], 1)
        self.assertEqual(summary["warning_count"], 1)
        self.assertEqual(summary["result_counts"]["ingested"], 1)
        self.assertEqual(summary["result_counts"]["chunked"], 2)
        self.assertIn("stage_timings", summary)
        self.assertIn("ingestion", summary["stage_timings"])
        self.assertIn("chunking", summary["stage_timings"])


if __name__ == "__main__":
    unittest.main()