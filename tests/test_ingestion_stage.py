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

# tests/test_ingestion_stage.py

"""Unit tests for the IngestionStage pipeline stage."""

import unittest
import tempfile
import os
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages.ingestion import IngestionStage
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.errors import ConfigurationError, AdapterError, IngesterError, IngestionEngineError


class TestIngestionStage:
    """Test suite for IngestionStage."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment before each test."""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        
        # Create patchers for the factory methods to avoid actual file system access
        self.adapter_patcher = patch("pulsepipe.pipelines.stages.ingestion.create_adapter")
        self.ingester_patcher = patch("pulsepipe.pipelines.stages.ingestion.create_ingester")
        self.engine_patcher = patch("pulsepipe.pipelines.stages.ingestion.IngestionEngine")
        
        # Start the patchers
        self.mock_create_adapter = self.adapter_patcher.start()
        self.mock_create_ingester = self.ingester_patcher.start()
        self.mock_engine_class = self.engine_patcher.start()
        
        # Set up the mock objects
        self.mock_adapter = MagicMock()
        self.mock_ingester = MagicMock()
        self.mock_engine = MagicMock()
        self.mock_engine.run = AsyncMock(return_value=[])
        self.mock_engine.processing_errors = []
        
        # Configure the mocks to return our mock objects
        self.mock_create_adapter.return_value = self.mock_adapter
        self.mock_create_ingester.return_value = self.mock_ingester
        self.mock_engine_class.return_value = self.mock_engine
        
        # Create context with basic config using the temporary directory
        self.context = PipelineContext(
            name="test_pipeline",
            config={
                "adapter": {
                    "type": "file_watcher",
                    "watch_path": self.temp_dir,
                    "extensions": [".json", ".xml"]
                },
                "ingester": {
                    "type": "fhir",
                    "resource_types": ["Patient", "Observation"]
                }
            }
        )
        
        # Create a fresh instance of IngestionStage for each test
        self.ingestion_stage = IngestionStage()
        
    def teardown(self):
        """Clean up after tests."""
        # Stop all the patchers
        self.adapter_patcher.stop()
        self.ingester_patcher.stop()
        self.engine_patcher.stop()
        
        # Remove temporary directory
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_execute_missing_adapter_config(self):
        """Test execution with missing adapter configuration."""
        # Remove adapter config
        self.context.config.pop("adapter")
        
        # Should raise ConfigurationError
        with pytest.raises(ConfigurationError) as cm:
            await self.ingestion_stage.execute(self.context)
            
        # Verify error message
        assert "Missing adapter configuration" in str(cm.value)

    @pytest.mark.asyncio
    async def test_execute_missing_ingester_config(self):
        """Test execution with missing ingester configuration."""
        # Remove ingester config
        self.context.config.pop("ingester")
        
        # Should raise ConfigurationError
        with pytest.raises(ConfigurationError) as cm:
            await self.ingestion_stage.execute(self.context)
            
        # Verify error message
        assert "Missing ingester configuration" in str(cm.value)

    @pytest.mark.asyncio
    async def test_execute_successful_ingestion(self):
        """Test successful ingestion execution."""
        # Sample ingestion results
        sample_results = [
            {"id": "patient1", "resource_type": "Patient", "name": "Test Patient"},
            {"id": "obs1", "resource_type": "Observation", "code": "8480-6", "value": "120/80"}
        ]
        
        # Configure the mock engine to return sample data
        self.mock_engine.run = AsyncMock(return_value=sample_results)
        
        # Execute the stage
        result = await self.ingestion_stage.execute(self.context)
        
        # Verify results
        assert result == sample_results
        assert len(result) == 2
        
        # Verify mocks were called correctly
        self.mock_create_adapter.assert_called_once()
        self.mock_create_ingester.assert_called_once()
        self.mock_engine_class.assert_called_once_with(self.mock_adapter, self.mock_ingester)
        self.mock_engine.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_with_single_scan_mode(self):
        """Test execution with single scan mode enabled."""
        # Update config to include single_scan
        self.context.config["single_scan"] = True
        
        # Configure the mock engine to return a sample result
        self.mock_engine.run = AsyncMock(return_value=[{"id": "test"}])
        
        # Execute the stage
        await self.ingestion_stage.execute(self.context)
        
        # Verify adapter was created with single_scan=True
        self.mock_create_adapter.assert_called_once_with(
            self.context.config["adapter"], 
            single_scan=True
        )

    @pytest.mark.asyncio
    async def test_execute_with_non_continuous_file_watcher(self):
        """Test execution with non-continuous file watcher."""
        # Update config for non-continuous mode
        self.context.config["adapter"]["continuous"] = False
        self.context.config["timeout"] = 15.0
        
        # Configure mock engine to return a result
        self.mock_engine.run = AsyncMock(return_value=[{"id": "test"}])
        
        # Execute the stage
        await self.ingestion_stage.execute(self.context)
        
        # Verify engine.run was called with the timeout
        self.mock_engine.run.assert_called_once_with(timeout=15.0)

    @pytest.mark.asyncio
    async def test_execute_with_continuous_file_watcher(self):
        """Test execution with continuous file watcher."""
        # Ensure continuous mode is enabled
        self.context.config["adapter"]["continuous"] = True
        
        # Configure the mock engine to return a result
        self.mock_engine.run = AsyncMock(return_value=[{"id": "test"}])
        
        # Execute the stage
        await self.ingestion_stage.execute(self.context)
        
        # Verify engine.run was called with no timeout (None)
        self.mock_engine.run.assert_called_once_with(timeout=None)

    @pytest.mark.asyncio
    async def test_execute_with_empty_result_continuous(self):
        """Test execution with empty result in continuous mode."""
        # Ensure continuous mode is enabled
        self.context.config["adapter"]["continuous"] = True
        
        # Configure the mock engine to return an empty result
        self.mock_engine.run = AsyncMock(return_value=[])  # Empty result
        
        # Execute the stage
        result = await self.ingestion_stage.execute(self.context)
        
        # In continuous mode, empty result should return None
        assert result is None

    @pytest.mark.asyncio
    async def test_execute_with_empty_result_non_continuous(self):
        """Test execution with empty result in non-continuous mode."""
        # Set non-continuous mode
        self.context.config["adapter"]["continuous"] = False
        
        # Configure the mock engine to return an empty result
        self.mock_engine.run = AsyncMock(return_value=[])  # Empty result
        
        # Execute the stage
        result = await self.ingestion_stage.execute(self.context)
        
        # In non-continuous mode, empty result should still return None
        assert result is None

    @pytest.mark.asyncio
    async def test_execute_with_processing_errors(self):
        """Test execution with processing errors."""
        # Sample processing errors
        sample_errors = [
            {"message": "Error 1", "file": "test1.json", "error_type": "parse_error"},
            {"message": "Error 2", "file": "test2.json", "error_type": "validation_error"}
        ]
        
        # Configure the mock engine
        self.mock_engine.run = AsyncMock(return_value=[{"id": "success"}])  # Some successful results
        self.mock_engine.processing_errors = sample_errors
        
        # Mock context's add_error method
        self.context.add_error = MagicMock()
        
        # Execute the stage
        result = await self.ingestion_stage.execute(self.context)
        
        # Verify add_error was called for each error
        assert self.context.add_error.call_count == 2
        
        # Verify the result still contains the successful items
        assert len(result) == 1
        assert result[0]["id"] == "success"

    @pytest.mark.asyncio
    async def test_execute_with_adapter_error(self):
        """Test handling of adapter errors."""
        # Create adapter error
        adapter_error = AdapterError("Test adapter error")
        
        # Configure the mock to raise the error
        self.mock_create_adapter.side_effect = adapter_error
        
        # Should propagate the adapter error
        with pytest.raises(AdapterError) as cm:
            await self.ingestion_stage.execute(self.context)
            
        # Verify it's the same error
        assert cm.value == adapter_error

    @pytest.mark.asyncio
    async def test_execute_with_ingester_error(self):
        """Test handling of ingester errors."""
        # Create ingester error
        ingester_error = IngesterError("Test ingester error")
        
        # Configure the create_adapter mock to succeed
        self.mock_create_adapter.side_effect = None
        self.mock_create_adapter.return_value = self.mock_adapter
        
        # Configure the create_ingester mock to raise the error
        self.mock_create_ingester.side_effect = ingester_error
        
        # Should propagate the ingester error
        with pytest.raises(IngesterError) as cm:
            await self.ingestion_stage.execute(self.context)
            
        # Verify it's the same error
        assert cm.value == ingester_error

    @pytest.mark.asyncio
    async def test_execute_with_unexpected_error(self):
        """Test handling of unexpected errors."""
        # Create unexpected error
        unexpected_error = ValueError("Unexpected test error")
        
        # Configure the mocks to work normally until engine.run
        self.mock_create_adapter.side_effect = None
        self.mock_create_adapter.return_value = self.mock_adapter
        self.mock_create_ingester.side_effect = None
        self.mock_create_ingester.return_value = self.mock_ingester
        
        # Configure engine.run to raise the unexpected error
        self.mock_engine.run = AsyncMock(side_effect=unexpected_error)
        
        # Should wrap the unexpected error in an IngestionEngineError
        with pytest.raises(IngestionEngineError) as cm:
            await self.ingestion_stage.execute(self.context)
            
        # Verify error details
        assert "Unexpected error in ingestion engine" in str(cm.value)
        assert cm.value.cause == unexpected_error

    @pytest.mark.asyncio
    async def test_execute_with_single_item_result(self):
        """Test execution with a single item result (not a list)."""
        # Single item result (not a list)
        single_result = {"id": "patient1", "resource_type": "Patient", "name": "Test Patient"}
        
        # Configure the mock engine to return a single item
        self.mock_engine.run = AsyncMock(return_value=single_result)  # Single item, not a list
        
        # Execute the stage
        result = await self.ingestion_stage.execute(self.context)
        
        # Verify the result is passed through correctly
        assert result == single_result
        assert result["id"] == "patient1"

if __name__ == "__main__":
    unittest.main()