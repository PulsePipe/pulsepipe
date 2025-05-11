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

# tests/test_embedder_operational.py

import pytest
from unittest.mock import patch, MagicMock
import numpy as np
from pulsepipe.pipelines.embedders.operational_embedder import OperationalEmbedder
from pulsepipe.utils.log_factory import LogFactory

# Initialize logging for tests
logger = LogFactory.get_logger(__name__)
logger.info("📁 Initializing Operational Embedder Tests")

# Mock for SentenceTransformer to avoid actual model loading
@pytest.fixture
def mock_sentence_transformer(monkeypatch):
    mock_model = MagicMock()
    mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]])
    mock_model.get_sentence_embedding_dimension.return_value = 4
    
    mock_st = MagicMock()
    mock_st.return_value = mock_model
    
    with patch('sentence_transformers.SentenceTransformer', mock_st):
        yield mock_st

class TestOperationalEmbedder:
    """Tests for the OperationalEmbedder class."""
    
    def test_initialization(self, mock_sentence_transformer):
        """Test that the embedder initializes correctly."""
        embedder = OperationalEmbedder()
        
        assert embedder.name == "OperationalEmbedder"
        assert embedder.model_name == "all-MiniLM-L6-v2"  # Default model
        assert embedder.normalize is True
        assert embedder.dimension == 4  # Based on our mock
        
        # Test with custom config
        custom_config = {
            "model_name": "custom/model",
            "normalize": False
        }
        embedder = OperationalEmbedder(config=custom_config)
        assert embedder.model_name == "custom/model"
        assert embedder.normalize is False


    @pytest.mark.asyncio
    async def test_embed_method(self, mock_sentence_transformer):
        """Test the embed method with various inputs."""
        embedder = OperationalEmbedder()
        
        # Configure mock for empty list
        mock_model = mock_sentence_transformer.return_value
        mock_model.encode.return_value = np.array([])
        
        # Test with empty content
        chunk = {"content": ""}
        result = embedder.embed_chunk(chunk)
        # Instead of checking equality, check that embedding was generated
        assert "embedding" in result
        assert "embedding_model" in result
        assert "embedding_dim" in result
        
        # Configure mock for single text
        mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3, 0.4]])
        
        # Test with single text
        texts = ["Claim ID: 12345, Amount: $500, Provider: XYZ Hospital"]
        result = await embedder.embed(texts)
        assert len(result) == 1
        assert len(result[0]) == 4  # 4-dimensional vectors
        
        # Configure mock for multiple texts
        mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]])
        
        # Test with multiple texts
        texts = [
            "Claim ID: 12345, Amount: $500, Provider: XYZ Hospital",
            "Prior Authorization: 67890, Status: Approved"
        ]
        result = await embedder.embed(texts)
        assert len(result) == 2
        assert all(len(vec) == 4 for vec in result)


    def test_embed_chunk_with_different_content_types(self, mock_sentence_transformer):
        """Test embedding chunks with different content types."""
        embedder = OperationalEmbedder()
        
        # Test with string content
        chunk = {"content": "Claim ID: 12345, Amount: $500"}
        result = embedder.embed_chunk(chunk)
        assert "embedding" in result
        assert "embedding_model" in result
        assert "embedding_dim" in result
        assert result["embedding_dim"] == 4
        
        # Test with dictionary content
        chunk = {"content": {"claim_id": "12345", "amount": 500}}
        result = embedder.embed_chunk(chunk)
        assert "embedding" in result
        
        # Test with list of dictionaries (common for operational data)
        chunk = {"content": [
            {"claim_id": "12345", "amount": 500},
            {"claim_id": "67890", "amount": 1000}
        ]}
        result = embedder.embed_chunk(chunk)
        assert "embedding" in result
        
        # Test with missing content
        chunk = {"metadata": {"source": "claims_system"}}
        result = embedder.embed_chunk(chunk)
        assert "embedding" not in result
    

    def test_dimension_property(self, mock_sentence_transformer):
        """Test the dimension property."""
        embedder = OperationalEmbedder()
        assert embedder.dimension == 4  # Based on our mock

 
    def test_handle_complex_operational_data(self, mock_sentence_transformer):
        """Test handling of complex operational data structures."""
        embedder = OperationalEmbedder()
        
        # Test with a typical claims chunk
        claim_chunk = {
            "type": "claims",
            "content": [
                {
                    "claim_id": "CL12345",
                    "patient_id": "PT67890",
                    "total_amount": 1250.50,
                    "status": "paid",
                    "service_date": "2025-01-15"
                }
            ],
            "metadata": {
                "source": "X12_835",
                "version": "5010"
            }
        }
        
        result = embedder.embed_chunk(claim_chunk)
        assert "embedding" in result
        assert result["embedding_model"] == "all-MiniLM-L6-v2"
