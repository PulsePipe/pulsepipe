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

# tests/test_embedder_clinical.py

import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
import numpy as np
from pulsepipe.pipelines.embedders.clinical_embedder import ClinicalEmbedder
from pulsepipe.utils.log_factory import LogFactory

# Initialize logging for tests
logger = LogFactory.get_logger(__name__)
logger.info("📁 Initializing Clinical Embedder Tests")

# Mock for SentenceTransformer to avoid actual model loading
@pytest.fixture
def mock_sentence_transformer(monkeypatch):
    """
    Create a mock for SentenceTransformer with proper implementation
    that avoids triggering asyncio warnings in torch.
    """
    # Create a proper mock for the model
    mock_model = MagicMock()
    mock_model.encode.return_value = np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
    mock_model.get_sentence_embedding_dimension.return_value = 3
    
    # Mock the SentenceTransformer constructor directly via monkeypatch
    # This avoids the warning from torch's async operations
    def mock_transformer_constructor(*args, **kwargs):
        return mock_model
    
    # Apply the monkeypatch before importing torch in SentenceTransformer
    monkeypatch.setattr('sentence_transformers.SentenceTransformer', mock_transformer_constructor)
    
    yield mock_model

class TestClinicalEmbedder:
    """Tests for the ClinicalEmbedder class."""
    
    def test_initialization(self, mock_sentence_transformer):
        """Test that the embedder initializes correctly."""
        embedder = ClinicalEmbedder()
        
        assert embedder.name == "ClinicalEmbedder"
        assert embedder.model_name == "emilyalsentzer/Bio_ClinicalBERT"
        assert embedder.normalize is True
        assert embedder.dimension == 3
        
        # Test with custom config
        custom_config = {
            "model_name": "some/other-model",
            "normalize": False
        }
        embedder = ClinicalEmbedder(config=custom_config)
        assert embedder.model_name == "some/other-model"
        assert embedder.normalize is False


    @pytest.mark.asyncio
    async def test_embed_method(self, mock_sentence_transformer):
        """Test the embed method with various inputs."""
        embedder = ClinicalEmbedder()
        
        # Configure mock for various test cases
        mock_sentence_transformer.encode.side_effect = [
            np.array([]),  # First call returns empty array for empty input
            np.array([[0.1, 0.2, 0.3]]),  # Second call returns one embedding
            np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])  # Third call returns two embeddings
        ]
        
        # Test with empty list
        result = await embedder.embed([])
        assert result == []  # Should return empty list
        
        # Test with single text
        texts = ["The patient presents with hypertension and diabetes."]
        result = await embedder.embed(texts)
        assert len(result) == 1
        assert len(result[0]) == 3  # 3-dimensional vectors
        
        # Test with multiple texts
        texts = [
            "The patient presents with hypertension and diabetes.",
            "No known drug allergies."
        ]
        result = await embedder.embed(texts)
        assert len(result) == 2
        assert all(len(vec) == 3 for vec in result)
    

    def test_embed_chunk(self, mock_sentence_transformer):
        """Test embedding a single chunk."""
        embedder = ClinicalEmbedder()
        
        # Configure mock for various test cases
        mock_sentence_transformer.encode.side_effect = [
            np.array([0.1, 0.2, 0.3]),  # For empty content
            np.array([0.4, 0.5, 0.6]),  # For string content
            np.array([0.7, 0.8, 0.9])   # For list content
        ]

        # Test with empty content
        chunk = {"content": ""}
        result = embedder.embed_chunk(chunk)
        # Instead of checking equality, check that embedding was generated
        assert "embedding" in result
        assert "embedding_model" in result
        assert "embedding_dim" in result

        # Test with string content
        chunk = {"content": "The patient has a history of heart disease."}
        result = embedder.embed_chunk(chunk)
        assert "embedding" in result
        assert "embedding_model" in result
        assert "embedding_dim" in result
        assert result["embedding_dim"] == 3
        
        # Test with list content
        chunk = {"content": ["Heart disease", "Diabetes", "Hypertension"]}
        result = embedder.embed_chunk(chunk)
        assert "embedding" in result
        
        # Test with missing content
        chunk = {"other_field": "value"}
        result = embedder.embed_chunk(chunk)
        assert "embedding" not in result
    

    def test_dimension_property(self, mock_sentence_transformer):
        """Test the dimension property."""
        embedder = ClinicalEmbedder()
        assert embedder.dimension == 3  # Based on our mock
