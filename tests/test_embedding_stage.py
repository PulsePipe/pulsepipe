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

# tests/test_embedding_stage.py

"""Unit tests for the EmbeddingStage pipeline stage."""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pytest
import sys
import numpy as np

# Mock sentence_transformers module before importing the module that uses it
sys.modules['sentence_transformers'] = MagicMock()
sys.modules['sentence_transformers.SentenceTransformer'] = MagicMock()

from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages.embedding import EmbeddingStage
from pulsepipe.utils.errors import EmbedderError
from pulsepipe.pipelines.embedders.clinical_embedder import ClinicalEmbedder
from pulsepipe.pipelines.embedders.operational_embedder import OperationalEmbedder


class TestEmbeddingStage:
    """Test suite for EmbeddingStage."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment before each test."""
        self.context = PipelineContext(
            name="test_pipeline",
            config={}  # Empty config will be updated in specific tests
        )
        self.sample_chunks = [
            {"id": "chunk1", "text": "This is a test chunk for embedding", "content": "This is a test chunk for embedding", "metadata": {"source": "test"}},
            {"id": "chunk2", "text": "Another test chunk with different content", "content": "Another test chunk with different content", "metadata": {"source": "test"}},
        ]
        # Use a NumPy array for the embedding to ensure it can be serialized to JSON
        self.mock_embedding = np.array([0.1, 0.2, 0.3, 0.4, 0.5]).tolist()
        
        # Create mock embedders 
        self.mock_clinical_embedder = MagicMock(spec=ClinicalEmbedder)
        self.mock_clinical_embedder.name = "MockClinicalEmbedder"
        self.mock_clinical_embedder.embed_chunk.side_effect = lambda chunk: {**chunk, "embedding": self.mock_embedding}
        
        self.mock_operational_embedder = MagicMock(spec=OperationalEmbedder)
        self.mock_operational_embedder.name = "MockOperationalEmbedder"
        self.mock_operational_embedder.embed_chunk.side_effect = lambda chunk: {**chunk, "embedding": self.mock_embedding}
        
        # Create embedder classes that return our mock instances
        self.mock_clinical_embedder_class = MagicMock(return_value=self.mock_clinical_embedder)
        self.mock_operational_embedder_class = MagicMock(return_value=self.mock_operational_embedder)
        
        # Patch the EMBEDDER_REGISTRY to use our mock classes
        self.registry_patcher = patch(
            "pulsepipe.pipelines.stages.embedding.EMBEDDER_REGISTRY", 
            {"clinical": self.mock_clinical_embedder_class, "operational": self.mock_operational_embedder_class}
        )
        self.mock_registry = self.registry_patcher.start()
        
        # Create a fresh EmbeddingStage instance for each test
        self.embedding_stage = EmbeddingStage()
        
    def teardown(self):
        """Clean up after tests."""
        self.registry_patcher.stop()

    @pytest.mark.asyncio
    async def test_execute_without_config(self):
        """Test execution with default configuration."""
        # Execute the embedding stage with default config (uses clinical embedder)
        result = await self.embedding_stage.execute(self.context, self.sample_chunks)
        
        # Verify the results
        assert len(result) == 2
        assert "embedding" in result[0]
        assert result[0]["embedding"] == self.mock_embedding
        assert result[0]["id"] == "chunk1"
        assert result[1]["id"] == "chunk2"
        
        # Verify the clinical embedder was called twice (for our two chunks)
        assert self.mock_clinical_embedder.embed_chunk.call_count == 2
        
        # Verify operational embedder was not called
        assert self.mock_operational_embedder.embed_chunk.call_count == 0
            
    @pytest.mark.asyncio
    async def test_execute_with_explicit_config(self):
        """Test execution with explicit configuration."""
        # Set up a config with specific embedder settings
        config = {"type": "operational", "model_name": "test-model"}
        self.context.config = {"embedding": config}
        
        # Execute the embedding stage (should use operational embedder)
        result = await self.embedding_stage.execute(self.context, self.sample_chunks)
        
        # Verify the results
        assert len(result) == 2
        assert "embedding" in result[0]
        assert result[0]["embedding"] == self.mock_embedding
        
        # Verify the operational embedder was called twice (for our two chunks)
        assert self.mock_operational_embedder.embed_chunk.call_count == 2
        
        # Verify clinical embedder was not called
        assert self.mock_clinical_embedder.embed_chunk.call_count == 0
    
    @pytest.mark.asyncio
    async def test_execute_with_no_input_data(self):
        """Test execution with no input data."""
        # Set context with no chunked data
        self.context.chunked_data = None
        
        # Test that it raises an error when no input data is provided
        with pytest.raises(EmbedderError):
            await self.embedding_stage.execute(self.context)
    
    @pytest.mark.asyncio
    async def test_execute_with_data_from_context(self):
        """Test execution getting data from context."""
        # Set sample chunks in context
        self.context.chunked_data = self.sample_chunks
        
        # Execute the embedding stage without passing input data (uses data from context)
        result = await self.embedding_stage.execute(self.context)
        
        # Verify the results
        assert len(result) == 2
        assert "embedding" in result[0]
        assert result[0]["embedding"] == self.mock_embedding
        
        # Verify the clinical embedder was called twice (for our two chunks)
        assert self.mock_clinical_embedder.embed_chunk.call_count == 2
    
    @pytest.mark.asyncio
    async def test_execute_with_empty_chunks(self):
        """Test execution with empty chunks list."""
        # Execute with empty chunks list
        result = await self.embedding_stage.execute(self.context, [])
        
        # Should return empty list without error
        assert result == []
        
        # Verify that no embedders were called
        assert self.mock_clinical_embedder.embed_chunk.call_count == 0
        assert self.mock_operational_embedder.embed_chunk.call_count == 0
    
    @pytest.mark.asyncio
    async def test_export_embeddings_to_jsonl(self):
        """Test exporting embeddings to JSONL file."""
        # Create temp dir for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up config with export option
            config = {
                "type": "clinical",
                "model_name": "test-model",
                "export_embeddings_to": "jsonl"
            }
            self.context.config = {"embedding": config}
            
            # Set output path in context
            output_path = os.path.join(temp_dir, "test_embeddings.jsonl")
            self.context.get_output_path_for_stage = MagicMock(return_value=output_path)
            
            # Mock the ClinicalEmbedder
            with patch("pulsepipe.pipelines.embedders.clinical_embedder.ClinicalEmbedder") as mock_embedder_class:
                # Configure the mock
                mock_embedder = MagicMock()
                mock_embedder.name = "MockEmbedder"
                mock_embedder.embed_chunk.side_effect = lambda chunk: {**chunk, "embedding": self.mock_embedding}
                mock_embedder_class.return_value = mock_embedder
                
                # Execute the embedding stage
                result = await self.embedding_stage.execute(self.context, self.sample_chunks)
                
                # Verify file was created
                assert os.path.exists(output_path)
                
                # Verify file contents
                with open(output_path, "r") as f:
                    lines = f.readlines()
                    assert len(lines) == 2  # Two chunks
                    
                    # Parse and verify content
                    for i, line in enumerate(lines):
                        chunk = json.loads(line)
                        assert chunk["id"] == f"chunk{i+1}"
                        assert chunk["embedding"] == self.mock_embedding
    
    @pytest.mark.asyncio
    async def test_export_embeddings_to_json(self):
        """Test exporting embeddings to JSON file."""
        # Create temp dir for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up config with export option
            config = {
                "type": "clinical",
                "model_name": "test-model",
                "export_embeddings_to": "json"
            }
            self.context.config = {"embedding": config}
            
            # Set output path in context
            output_path = os.path.join(temp_dir, "test_embeddings.json")
            self.context.get_output_path_for_stage = MagicMock(return_value=output_path)
            
            # Mock the ClinicalEmbedder
            with patch("pulsepipe.pipelines.embedders.clinical_embedder.ClinicalEmbedder") as mock_embedder_class:
                # Configure the mock
                mock_embedder = MagicMock()
                mock_embedder.name = "MockEmbedder"
                mock_embedder.embed_chunk.side_effect = lambda chunk: {**chunk, "embedding": self.mock_embedding}
                mock_embedder_class.return_value = mock_embedder
                
                # Execute the embedding stage
                result = await self.embedding_stage.execute(self.context, self.sample_chunks)
                
                # Verify file was created
                assert os.path.exists(output_path)
                
                # Verify file contents
                with open(output_path, "r") as f:
                    chunks = json.load(f)
                    assert len(chunks) == 2  # Two chunks
                    assert chunks[0]["id"] == "chunk1"
                    assert chunks[0]["embedding"] == self.mock_embedding
                    assert chunks[1]["id"] == "chunk2"
                    assert chunks[1]["embedding"] == self.mock_embedding
    
    @pytest.mark.asyncio
    async def test_unsupported_export_format(self):
        """Test unsupported export format."""
        # Set up config with unsupported export format
        config = {
            "type": "clinical",
            "model_name": "test-model",
            "export_embeddings_to": "csv"  # Unsupported format
        }
        self.context.config = {"embedding": config}
        self.context.add_warning = MagicMock()
        
        # Mock the ClinicalEmbedder
        with patch("pulsepipe.pipelines.embedders.clinical_embedder.ClinicalEmbedder") as mock_embedder_class:
            # Configure the mock
            mock_embedder = MagicMock()
            mock_embedder.name = "MockEmbedder"
            mock_embedder.embed_chunk.side_effect = lambda chunk: {**chunk, "embedding": self.mock_embedding}
            mock_embedder_class.return_value = mock_embedder
            
            # Execute the embedding stage
            result = await self.embedding_stage.execute(self.context, self.sample_chunks)
            
            # Verify warning was added
            self.context.add_warning.assert_called_once()
            call_args = self.context.add_warning.call_args[0]
            assert call_args[0] == "embedding"
            assert "Unsupported export format" in call_args[1]
    
    @pytest.mark.asyncio
    async def test_execute_with_exception_in_embedder_class(self):
        """Test handling when the embedder class itself raises an exception."""
        # Set up a specific error message for testing
        error_message = "Embedder class initialization error"
        
        # Mock the EMBEDDER_REGISTRY to raise an exception
        with patch("pulsepipe.pipelines.stages.embedding.EMBEDDER_REGISTRY", 
                 {"clinical": MagicMock(side_effect=Exception(error_message))}):
            
            # Execute the stage and expect an EmbedderError
            with pytest.raises(EmbedderError) as error_context:
                await self.embedding_stage.execute(self.context, self.sample_chunks)
                
            # Verify the error details
            assert error_message in str(error_context.value)