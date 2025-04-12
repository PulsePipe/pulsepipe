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

# tests/test_vectorstore_weaviate.py

import pytest
from unittest.mock import Mock, MagicMock
from pulsepipe.pipelines.vectorstore.weaviate_store import WeaviateVectorStore

def test_weaviate_upsert_and_query(monkeypatch):
    # Mock the Weaviate client
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_data = MagicMock()
    
    # Setup the mock chain
    mock_client.collections.get.return_value = mock_collection
    mock_collection.data = mock_data
    
    # Mock graphql response for query
    mock_response = {
        "data": {
            "Get": {
                "test_namespace": [
                    {
                        "patient_id": "123",
                        "text": "blood pressure normal",
                        "_additional": {"vector": [0.1, 0.2, 0.3]}
                    }
                ]
            }
        }
    }
    mock_client.graphql_raw_query.return_value = mock_response
    
    # Replace the actual connection with our mock
    monkeypatch.setattr("pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate", 
                       lambda host, port: mock_client)
    
    # Test the vector store with mocked client
    store = WeaviateVectorStore()
    dummy_vectors = [{
        "embedding": [0.1, 0.2, 0.3],
        "metadata": {"patient_id": "123", "text": "blood pressure normal"}
    }]
    namespace = "test_namespace"
    
    # Test upsert
    store.upsert(namespace, dummy_vectors)
    mock_collection.data.insert.assert_called_once()
    
    # Test query
    results = store.query(namespace, [0.1, 0.2, 0.3], top_k=1)
    mock_client.graphql_raw_query.assert_called_once()
    
    # Verify results
    assert len(results) == 1
    assert results[0]["patient_id"] == "123"
    assert results[0]["text"] == "blood pressure normal"