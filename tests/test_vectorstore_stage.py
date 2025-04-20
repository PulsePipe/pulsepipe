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

"""Unit tests for the VectorStoreStage pipeline stage."""

import json
import os
import unittest
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages.vectorstore import VectorStoreStage
from pulsepipe.utils.errors import VectorStoreError, ConfigurationError
from pulsepipe.pipelines.vectorstore import VectorStoreConnectionError


class TestVectorStoreStage:
    """Test suite for VectorStoreStage."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment before each test."""
        self.vectorstore_stage = VectorStoreStage()
        self.context = PipelineContext(
            name="test_pipeline",
            config={}  # Empty config will be updated in specific tests
        )
        self.sample_chunks = [
            {
                "id": "chunk1", 
                "type": "clinical",
                "text": "This is a test chunk for embedding", 
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
                "metadata": {"source": "test"}
            },
            {
                "id": "chunk2", 
                "type": "clinical",
                "text": "Another test chunk with different content", 
                "embedding": [0.5, 0.4, 0.3, 0.2, 0.1],
                "metadata": {"source": "test"}
            },
            {
                "id": "chunk3", 
                "type": "operational",
                "text": "An operational chunk", 
                "embedding": [0.2, 0.4, 0.6, 0.8, 1.0],
                "metadata": {"source": "test"}
            }
        ]
        
        # Create a patch for the registry to avoid actual connections
        patcher = patch.object(
            self.vectorstore_stage, 
            'vectorstore_registry', 
            {
                "weaviate": MagicMock(),
                "qdrant": MagicMock()
            }
        )
        self.mock_registry = patcher.start()
        self.addCleanup(patcher.stop)
    
    def addCleanup(self, func, *args, **kwargs):
        """Add cleanup function for pytest compatibility."""
        # This is a no-op function to make unittest-style code work with pytest
        pass

    @pytest.mark.asyncio
    async def test_execute_without_config(self):
        """Test execution with missing configuration."""
        # No config set in context
        with pytest.raises(ConfigurationError):
            await self.vectorstore_stage.execute(self.context, self.sample_chunks)

    @pytest.mark.asyncio
    async def test_execute_with_disabled_config(self):
        """Test execution with disabled configuration."""
        # Set config with vectorstore disabled
        self.context.config = {
            "vectorstore": {
                "enabled": False,
                "engine": "qdrant",
                "host": "http://localhost"
            }
        }
        
        # Should return a skipped status without error
        result = await self.vectorstore_stage.execute(self.context, self.sample_chunks)
        assert result["status"] == "skipped"
        assert result["reason"] == "disabled in configuration"

    @pytest.mark.asyncio
    async def test_execute_with_no_input_data(self):
        """Test execution with no input data."""
        # Set config but no data
        self.context.config = {
            "vectorstore": {
                "engine": "qdrant",
                "host": "http://localhost"
            }
        }
        self.context.embedded_data = None
        
        # Should raise an error
        with pytest.raises(VectorStoreError):
            await self.vectorstore_stage.execute(self.context)

    @pytest.mark.asyncio
    async def test_execute_with_data_from_context(self):
        """Test execution getting data from context."""
        # Set config and data in context
        self.context.config = {
            "vectorstore": {
                "engine": "qdrant",
                "host": "http://localhost"
            }
        }
        self.context.embedded_data = self.sample_chunks
        
        # Mock the vector store for the upload
        mock_store = MagicMock()
        mock_store.upsert = MagicMock(return_value=True)
        
        # Mock the _upload_chunks method to avoid actual uploads
        with patch.object(self.vectorstore_stage, '_upload_chunks', AsyncMock()) as mock_upload:
            # Configure the mock to return a successful result
            mock_upload.return_value = {
                "total_uploaded": 3,
                "total_chunks": 3,
                "collections": ["clinical", "operational"],
                "details": {}
            }
            
            # Execute the stage
            result = await self.vectorstore_stage.execute(self.context)
            
            # Verify results
            assert result["total_uploaded"] == 3
            assert result["total_chunks"] == 3
            assert "clinical" in result["collections"]
            assert "operational" in result["collections"]

    @pytest.mark.asyncio
    async def test_unsupported_vector_engine(self):
        """Test with unsupported vector engine."""
        # Set config with invalid engine
        self.context.config = {
            "vectorstore": {
                "engine": "nonexistent_engine",
                "host": "http://localhost"
            }
        }
        
        # Make sure the registry doesn't have the engine we're testing
        self.vectorstore_stage.vectorstore_registry = {"weaviate": MagicMock(), "qdrant": MagicMock()}
        
        # Should raise a VectorStoreError with appropriate details
        with pytest.raises(VectorStoreError) as cm:
            await self.vectorstore_stage.execute(self.context, self.sample_chunks)
            
        # Verify error details
        assert "Unsupported vector store engine" in str(cm.value)
        assert cm.value.details["engine"] == "nonexistent_engine"

    @pytest.mark.asyncio
    async def test_connection_error(self):
        """Test handling of connection errors."""
        # Set config
        self.context.config = {
            "vectorstore": {
                "engine": "qdrant",
                "host": "http://nonexistent-host"
            }
        }
        
        # Create a mock class that raises a connection error when instantiated
        mock_qdrant_instance = MagicMock()
        mock_qdrant_class = MagicMock()
        mock_qdrant_class.return_value = mock_qdrant_instance
        
        # Set up the _upload_chunks method to raise a connection error
        with patch.object(self.vectorstore_stage, 'vectorstore_registry', {"qdrant": mock_qdrant_class}):
            with patch.object(self.vectorstore_stage, '_upload_chunks', AsyncMock()) as mock_upload:
                mock_upload.side_effect = VectorStoreConnectionError(
                    "Failed to connect", 
                    host="http://nonexistent-host", 
                    port=6333
                )
                
                # Should raise a vector store error
                with pytest.raises(VectorStoreError) as cm:
                    await self.vectorstore_stage.execute(self.context, self.sample_chunks)
                    
                # Verify error details
                assert "Failed to connect" in str(cm.value)
                assert cm.value.details["engine"] == "qdrant"

    @pytest.mark.asyncio
    async def test_chunk_grouping_by_type(self):
        """Test that chunks are correctly grouped by type."""
        # Set config
        self.context.config = {
            "vectorstore": {
                "engine": "qdrant",
                "host": "http://localhost"
            }
        }
        
        # Instead of mocking the QdrantVectorStore class, let's directly test the _upload_chunks method
        mock_vectorstore = MagicMock()
        mock_vectorstore.upsert = MagicMock()
        
        # Call the _upload_chunks method directly
        result = await self.vectorstore_stage._upload_chunks(
            vectorstore=mock_vectorstore,
            chunks=self.sample_chunks,
            namespace_prefix="test",
            context=self.context
        )
        
        # Verify results
        assert result["total_uploaded"] == 3
        assert set(result["collections"]) == {"clinical", "operational"}
        
        # Verify the upsert calls - should be called once for each chunk type
        assert mock_vectorstore.upsert.call_count == 2
        
        # Check that the clinical chunks were grouped together
        clinical_call = None
        operational_call = None
        for call in mock_vectorstore.upsert.call_args_list:
            namespace = call[0][0]
            chunks = call[0][1]
            if "clinical" in namespace:
                clinical_call = (namespace, chunks)
            elif "operational" in namespace:
                operational_call = (namespace, chunks)
        
        # Verify clinical call
        assert clinical_call is not None
        assert len(clinical_call[1]) == 2  # Two clinical chunks
        
        # Verify operational call
        assert operational_call is not None
        assert len(operational_call[1]) == 1  # One operational chunk

    @pytest.mark.asyncio
    async def test_chunk_id_generation(self):
        """Test that chunk IDs are generated if missing."""
        # Create chunks without IDs
        chunks_without_ids = [
            {
                "type": "clinical",
                "text": "This chunk has no ID",
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
            },
            {
                "type": "clinical",
                "text": "This chunk also has no ID",
                "embedding": [0.5, 0.4, 0.3, 0.2, 0.1]
            }
        ]
        
        # Create mock for upsert
        mock_vectorstore = MagicMock()
        mock_vectorstore.upsert = MagicMock()
        
        # Call the _upload_chunks method directly
        await self.vectorstore_stage._upload_chunks(
            vectorstore=mock_vectorstore,
            chunks=chunks_without_ids,
            namespace_prefix="test",
            context=self.context
        )
        
        # Get the chunks that were passed to upsert
        args = mock_vectorstore.upsert.call_args
        chunks_passed = args[0][1]
        
        # Verify that IDs were added
        for chunk in chunks_passed:
            assert "id" in chunk
            assert isinstance(chunk["id"], str)
            assert len(chunk["id"]) > 0

    @pytest.mark.asyncio
    async def test_upload_error_handling(self):
        """Test handling of upload errors."""
        # Configure the stage
        self.context.config = {
            "vectorstore": {
                "engine": "qdrant",
                "host": "http://localhost"
            }
        }
        
        # Create mock for upsert that fails
        mock_vectorstore = MagicMock()
        mock_vectorstore.upsert = MagicMock(side_effect=Exception("Upload failed"))
        
        # Call the _upload_chunks method directly
        result = await self.vectorstore_stage._upload_chunks(
            vectorstore=mock_vectorstore,
            chunks=self.sample_chunks,
            namespace_prefix="test",
            context=self.context
        )
        
        # Verify results
        assert result["total_uploaded"] == 0  # No successful uploads
        
        # Check that failure details were recorded
        for chunk_type in result["details"]:
            assert not result["details"][chunk_type]["success"]
            assert "error" in result["details"][chunk_type]
            assert result["details"][chunk_type]["count"] == 0