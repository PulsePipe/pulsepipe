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

"""Unit tests for the chunking pipeline stage."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from pulsepipe.pipelines.stages.chunking import ChunkingStage
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.utils.errors import ChunkerError, ConfigurationError


class TestChunkingStage:
    """Tests for the ChunkingStage class."""
    
    @pytest.fixture
    def stage(self):
        """Create a ChunkingStage instance."""
        return ChunkingStage()
    
    @pytest.fixture
    def context(self):
        """Create a test pipeline context."""
        config = {
            "chunker": {
                "type": "clinical",
                "include_metadata": True
            }
        }
        ctx = PipelineContext(
            name="test_chunking",
            config=config,
            output_path="/tmp/test_output.json"
        )
        ctx.log_prefix = "[test_chunking]"  # Simplify for testing
        return ctx
    
    @pytest.fixture
    def clinical_content(self):
        """Create a mock clinical content object."""
        from pulsepipe.models.patient import PatientInfo, PatientPreferences
        from pulsepipe.models.encounter import EncounterInfo
        
        content = PulseClinicalContent(
            patient=PatientInfo(
                id="P123",
                dob_year=1980,
                gender="male",
                geographic_area="New York",
                preferences=[PatientPreferences(
                    preferred_language="English", 
                    communication_method="Phone", 
                    requires_interpreter=False, 
                    preferred_contact_time="Morning", 
                    notes="Test notes"
                )]
            ),
            encounter=EncounterInfo(
                id="E123",
                admit_date="2023-01-01",
                discharge_date="2023-01-05",
                encounter_type="Outpatient",
                type_coding_method="ICD-10",
                location="Main Hospital",
                reason_code="Z00.00",
                reason_coding_method="ICD-10",
                providers=[],
                visit_type="Follow-up",
                patient_id="P123"
            )
        )
        return content
    
    @pytest.fixture
    def operational_content(self):
        """Create a mock operational content object."""
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ABC123"
        )
        return content
    
    def test_initialization(self, stage):
        """Test that the stage initializes correctly."""
        assert stage.name == "chunking"
        assert stage.clinical_chunker is not None
        assert stage.operational_chunker is not None
    
    @pytest.mark.asyncio
    async def test_execute_with_clinical_content(self, stage, context, clinical_content):
        """Test executing the stage with clinical content."""
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Patient info for John Doe", "metadata": {"patient_id": "P123"}},
            {"id": "chunk2", "text": "Clinical observations for John Doe", "metadata": {"patient_id": "P123"}}
        ]
        
        # Need to mock the _chunk_item method instead of the chunker's chunk method
        with patch.object(stage, '_chunk_item', return_value=mock_chunks):
            # Execute the stage
            result = await stage.execute(context, clinical_content)
            
            # Verify results
            assert result == mock_chunks
            assert len(result) == 2
            # Not testing the internal chunker call since we're mocking _chunk_item
    
    @pytest.mark.asyncio
    async def test_execute_with_operational_content(self, stage, context, operational_content):
        """Test executing the stage with operational content."""
        # Update config to use operational chunker
        context.config["chunker"]["type"] = "operational"
        
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Payment info for claim 12345", "metadata": {"transaction_type": "835"}},
            {"id": "chunk2", "text": "Provider details for payment", "metadata": {"transaction_type": "835"}}
        ]
        
        # Need to mock the _chunk_item method instead of the chunker's chunk method
        with patch.object(stage, '_chunk_item', return_value=mock_chunks):
            # Execute the stage
            result = await stage.execute(context, operational_content)
            
            # Verify results
            assert result == mock_chunks
            assert len(result) == 2
            # Not testing the internal chunker call since we're mocking _chunk_item
    
    @pytest.mark.asyncio
    async def test_execute_with_auto_detection(self, stage, context, clinical_content):
        """Test executing the stage with auto chunker detection."""
        # Set chunker type to auto
        context.config["chunker"]["type"] = "auto"
        
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Patient info", "metadata": {"patient_id": "P123"}},
            {"id": "chunk2", "text": "Clinical data", "metadata": {"patient_id": "P123"}}
        ]
        
        with patch.object(stage.clinical_chunker.__class__, 'chunk', return_value=mock_chunks):
            # Execute the stage
            result = await stage.execute(context, clinical_content)
            
            # Verify results
            assert result == mock_chunks
            assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_execute_with_batch_input(self, stage, context, clinical_content, operational_content):
        """Test executing the stage with a batch of items."""
        # Set chunker type to auto
        context.config["chunker"]["type"] = "auto"
        
        # Create a batch of items
        batch = [clinical_content, operational_content]
        
        # Mock the chunkers to return predetermined chunks
        clinical_chunks = [
            {"id": "chunk1", "text": "Patient info", "metadata": {"patient_id": "P123"}}
        ]
        
        operational_chunks = [
            {"id": "chunk2", "text": "Payment info", "metadata": {"transaction_type": "835"}}
        ]
        
        # Use side_effect to return different chunks for different items
        def mock_chunk_item(item, chunker_type, include_metadata):
            if isinstance(item, PulseClinicalContent):
                return clinical_chunks
            elif isinstance(item, PulseOperationalContent):
                return operational_chunks
            return None
        
        with patch.object(stage, '_chunk_item', side_effect=mock_chunk_item):
            # Execute the stage
            result = await stage.execute(context, batch)
            
            # Verify results
            assert len(result) == 2
            assert result[0] == clinical_chunks[0]
            assert result[1] == operational_chunks[0]
            assert stage._chunk_item.call_count == 2
    
    @pytest.mark.asyncio
    async def test_execute_with_context_data(self, stage, context, clinical_content):
        """Test executing the stage using data from context."""
        # Set ingested data in the context
        context.ingested_data = clinical_content
        
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Patient info from context", "metadata": {"patient_id": "P123"}}
        ]
        
        with patch.object(stage, '_chunk_item', return_value=mock_chunks):
            # Execute the stage without input data
            result = await stage.execute(context)
            
            # Verify results
            assert result == mock_chunks
            assert len(result) == 1
            stage._chunk_item.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_with_no_data(self, stage, context):
        """Test executing the stage with no data available."""
        # Context with no ingested data
        context.ingested_data = None
        
        # Execute the stage and check for error
        with pytest.raises(ChunkerError) as excinfo:
            await stage.execute(context)
        
        assert "No input data available for chunking" in str(excinfo.value)
    
    @pytest.mark.asyncio
    async def test_execute_with_export_json(self, stage, context, clinical_content):
        """Test executing the stage with JSON export."""
        # Set export format
        context.config["chunker"]["export_chunks_to"] = "json"
        
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Patient info", "metadata": {"patient_id": "P123"}}
        ]
        
        with patch.object(stage, '_chunk_item', return_value=mock_chunks), \
             patch('builtins.open', new_callable=MagicMock), \
             patch('json.dump') as mock_json_dump:
            # Execute the stage
            result = await stage.execute(context, clinical_content)
            
            # Verify results
            assert result == mock_chunks
            mock_json_dump.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_with_export_jsonl(self, stage, context, clinical_content):
        """Test executing the stage with JSONL export."""
        # Set export format
        context.config["chunker"]["export_chunks_to"] = "jsonl"
        
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Patient info", "metadata": {"patient_id": "P123"}},
            {"id": "chunk2", "text": "Clinical data", "metadata": {"patient_id": "P123"}}
        ]
        
        # Use a real temporary file for testing
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Mock context to return our temp file path
            with patch.object(context, 'get_output_path_for_stage', return_value=temp_path), \
                 patch.object(stage, '_chunk_item', return_value=mock_chunks):
                # Execute the stage
                result = await stage.execute(context, clinical_content)
                
                # Verify results
                assert result == mock_chunks
                assert os.path.exists(temp_path)
                
                # Verify file contents
                with open(temp_path, 'r') as f:
                    lines = f.readlines()
                    assert len(lines) == 2
                    # Verify first chunk
                    chunk1 = json.loads(lines[0])
                    assert chunk1["id"] == "chunk1"
                    # Verify second chunk
                    chunk2 = json.loads(lines[1])
                    assert chunk2["id"] == "chunk2"
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_execute_with_export_error(self, stage, context, clinical_content):
        """Test executing the stage with export error."""
        # Set export format
        context.config["chunker"]["export_chunks_to"] = "json"
        
        # Mock the chunker to return predetermined chunks
        mock_chunks = [
            {"id": "chunk1", "text": "Patient info", "metadata": {"patient_id": "P123"}}
        ]
        
        with patch.object(stage, '_chunk_item', return_value=mock_chunks), \
             patch('builtins.open', side_effect=IOError("Permission denied")):
            # Execute the stage - should not raise but record error
            result = await stage.execute(context, clinical_content)
            
            # Verify results
            assert result == mock_chunks
            assert len(context.errors) == 1
            assert "Failed to write chunks" in context.errors[0]["message"]
    
    @pytest.mark.asyncio
    async def test_execute_chunking_error(self, stage, context, clinical_content):
        """Test executing the stage with a chunking error."""
        # Mock chunker to raise an exception
        with patch.object(stage, '_chunk_item', side_effect=Exception("Chunking failed")):
            # Execute the stage and check for error
            with pytest.raises(ChunkerError) as excinfo:
                await stage.execute(context, clinical_content)
            
            assert "Error during chunking" in str(excinfo.value)
    
    def test_chunk_item_clinical(self, stage, clinical_content):
        """Test chunking a clinical item."""
        # Mock the clinical chunker
        mock_chunks = [{"id": "chunk1", "text": "Clinical data"}]
        
        with patch.object(stage.clinical_chunker.__class__, 'chunk', return_value=mock_chunks):
            # Chunk the item
            result = stage._chunk_item(clinical_content, "clinical", True)
            
            # Verify results
            assert result == mock_chunks
    
    def test_chunk_item_operational(self, stage, operational_content):
        """Test chunking an operational item."""
        # Mock the operational chunker
        mock_chunks = [{"id": "chunk1", "text": "Operational data"}]
        
        with patch.object(stage.operational_chunker.__class__, 'chunk', return_value=mock_chunks):
            # Chunk the item
            result = stage._chunk_item(operational_content, "operational", True)
            
            # Verify results
            assert result == mock_chunks
    
    def test_chunk_item_auto(self, stage, clinical_content, operational_content):
        """Test auto-detection of chunker type."""
        # Mock both chunkers
        clinical_chunks = [{"id": "chunk1", "text": "Clinical data"}]
        operational_chunks = [{"id": "chunk2", "text": "Operational data"}]
        
        with patch.object(stage.clinical_chunker.__class__, 'chunk', return_value=clinical_chunks), \
             patch.object(stage.operational_chunker.__class__, 'chunk', return_value=operational_chunks):
            # Chunk clinical item with auto
            result1 = stage._chunk_item(clinical_content, "auto", True)
            assert result1 == clinical_chunks
            
            # Chunk operational item with auto
            result2 = stage._chunk_item(operational_content, "auto", True)
            assert result2 == operational_chunks
    
    def test_chunk_item_unknown_type(self, stage, clinical_content):
        """Test chunking with unknown chunker type."""
        # Chunk with invalid type
        result = stage._chunk_item(clinical_content, "invalid_type", True)
        
        # Verify no results returned
        assert result is None
    
    def test_chunk_item_error(self, stage, clinical_content):
        """Test error handling during chunking."""
        # Mock chunker to raise an exception
        with patch.object(stage.clinical_chunker.__class__, 'chunk', side_effect=Exception("Chunking failed")):
            # Chunk with error
            result = stage._chunk_item(clinical_content, "clinical", True)
            
            # Verify no results returned due to error
            assert result is None
    
    def test_chunk_item_unknown_input_type(self, stage):
        """Test chunking with unknown input type."""
        # Create an input of unknown type
        unknown_input = {"this": "is not a canonical model"}
        
        # Chunk with auto detection
        result = stage._chunk_item(unknown_input, "auto", True)
        
        # Verify no results returned
        assert result is None