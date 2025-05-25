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

# tests/test_vectorstore_weaviate_store.py

import pytest
from unittest.mock import Mock, MagicMock, patch
from pulsepipe.pipelines.vectorstore.weaviate_store import (
    WeaviateVectorStore, 
    connect_to_local_weaviate, 
    connect_to_wcs_weaviate,
    weaviate_create_object
)
from pulsepipe.pipelines.vectorstore.base_vectorstore import VectorStoreConnectionError


def test_connect_to_local_weaviate_success():
    with patch('weaviate.WeaviateClient') as mock_weaviate_client, \
         patch('weaviate.connect.ConnectionParams') as mock_connection_params:
        
        mock_client = MagicMock()
        mock_weaviate_client.return_value = mock_client
        
        result = connect_to_local_weaviate("localhost", 8080)
        
        assert result == mock_client
        mock_client.connect.assert_called_once()


def test_connect_to_local_weaviate_failure():
    with patch('weaviate.WeaviateClient') as mock_weaviate_client:
        mock_weaviate_client.side_effect = Exception("Connection failed")
        
        with pytest.raises(VectorStoreConnectionError) as exc_info:
            connect_to_local_weaviate("localhost", 8080)
        
        assert "Weaviate" in str(exc_info.value)
        assert "localhost" in str(exc_info.value)
        assert "8080" in str(exc_info.value)


def test_connect_to_local_weaviate_custom_host_port():
    with patch('weaviate.WeaviateClient') as mock_weaviate_client, \
         patch('weaviate.connect.ConnectionParams') as mock_connection_params:
        
        mock_client = MagicMock()
        mock_weaviate_client.return_value = mock_client
        mock_params = MagicMock()
        mock_connection_params.from_params.return_value = mock_params
        
        connect_to_local_weaviate("example.com", 9090)
        
        mock_connection_params.from_params.assert_called_once_with(
            http_host="example.com",
            http_port=9090,
            grpc_host="example.com",
            grpc_port=50051,
            http_secure=False,
            grpc_secure=False,
        )


def test_connect_to_wcs_weaviate_success():
    with patch('weaviate.connect_to_wcs') as mock_connect_to_wcs, \
         patch('weaviate.auth.AuthApiKey') as mock_auth_api_key:
        
        mock_client = MagicMock()
        mock_auth = MagicMock()
        mock_connect_to_wcs.return_value = mock_client
        mock_auth_api_key.return_value = mock_auth
        
        result = connect_to_wcs_weaviate("https://cluster.weaviate.cloud", "api-key-123")
        
        assert result == mock_client
        mock_auth_api_key.assert_called_once_with("api-key-123")
        mock_connect_to_wcs.assert_called_once_with(
            cluster_url="https://cluster.weaviate.cloud",
            auth_credentials=mock_auth
        )


def test_connect_to_wcs_weaviate_failure():
    with patch('weaviate.connect_to_wcs') as mock_connect_to_wcs:
        mock_connect_to_wcs.side_effect = Exception("WCS connection failed")
        
        with pytest.raises(VectorStoreConnectionError) as exc_info:
            connect_to_wcs_weaviate("https://cluster.weaviate.cloud", "api-key-123")
        
        assert "Weaviate" in str(exc_info.value)
        from urllib.parse import urlparse
        parsed_url = urlparse("https://cluster.weaviate.cloud")
        assert parsed_url.hostname in str(exc_info.value)
        assert "443" in str(exc_info.value)


def test_weaviate_create_object():
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_data = MagicMock()
    
    mock_client.collections.get.return_value = mock_collection
    mock_collection.data = mock_data
    
    vector = {
        "embedding": [0.1, 0.2, 0.3],
        "metadata": {"patient_id": "123", "text": "test data"}
    }
    
    weaviate_create_object(mock_client, "test_namespace", vector)
    
    mock_client.collections.get.assert_called_once_with("test_namespace")
    mock_data.insert.assert_called_once_with(
        properties={"patient_id": "123", "text": "test data"},
        vector=[0.1, 0.2, 0.3]
    )


def test_weaviate_vector_store_init_success():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect:
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        
        store = WeaviateVectorStore()
        
        assert store.client == mock_client
        mock_connect.assert_called_once_with(host="localhost", port=8080)


def test_weaviate_vector_store_init_failure():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect:
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(VectorStoreConnectionError) as exc_info:
            WeaviateVectorStore()
        
        assert "Weaviate" in str(exc_info.value)
        assert "localhost" in str(exc_info.value)
        assert "8080" in str(exc_info.value)


def test_weaviate_vector_store_upsert():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect, \
         patch('pulsepipe.pipelines.vectorstore.weaviate_store.weaviate_create_object') as mock_create:
        
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        
        store = WeaviateVectorStore()
        
        vectors = [
            {
                "embedding": [0.1, 0.2, 0.3],
                "metadata": {"patient_id": "123", "text": "blood pressure normal"}
            },
            {
                "embedding": [0.4, 0.5, 0.6],
                "metadata": {"patient_id": "456", "text": "glucose high"}
            }
        ]
        
        store.upsert("test_namespace", vectors)
        
        assert mock_create.call_count == 2
        mock_create.assert_any_call(mock_client, "test_namespace", vectors[0])
        mock_create.assert_any_call(mock_client, "test_namespace", vectors[1])


def test_weaviate_vector_store_query_success():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect:
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        
        # Mock successful GraphQL response
        mock_response = {
            "data": {
                "Get": {
                    "test_namespace": [
                        {
                            "patient_id": "123",
                            "text": "blood pressure normal"
                        },
                        {
                            "patient_id": "456", 
                            "text": "glucose high"
                        }
                    ]
                }
            }
        }
        mock_client.graphql_raw_query.return_value = mock_response
        
        store = WeaviateVectorStore()
        results = store.query("test_namespace", [0.1, 0.2, 0.3], top_k=2)
        
        assert len(results) == 2
        assert results[0]["patient_id"] == "123"
        assert results[0]["text"] == "blood pressure normal"
        assert results[1]["patient_id"] == "456"
        assert results[1]["text"] == "glucose high"
        
        # Verify GraphQL query was called
        mock_client.graphql_raw_query.assert_called_once()
        query_arg = mock_client.graphql_raw_query.call_args[0][0]
        assert "test_namespace" in query_arg
        assert "[0.1, 0.2, 0.3]" in query_arg
        assert "limit: 2" in query_arg


def test_weaviate_vector_store_query_no_data():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect:
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        
        # Mock response with no data
        mock_response = {"errors": ["No data found"]}
        mock_client.graphql_raw_query.return_value = mock_response
        
        store = WeaviateVectorStore()
        results = store.query("test_namespace", [0.1, 0.2, 0.3])
        
        assert results == []


def test_weaviate_vector_store_query_missing_namespace():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect:
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        
        # Mock response with missing namespace
        mock_response = {
            "data": {
                "Get": {
                    "other_namespace": []
                }
            }
        }
        mock_client.graphql_raw_query.return_value = mock_response
        
        store = WeaviateVectorStore()
        results = store.query("test_namespace", [0.1, 0.2, 0.3])
        
        assert results == []


def test_weaviate_vector_store_query_default_top_k():
    with patch('pulsepipe.pipelines.vectorstore.weaviate_store.connect_to_local_weaviate') as mock_connect:
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        
        mock_response = {
            "data": {
                "Get": {
                    "test_namespace": []
                }
            }
        }
        mock_client.graphql_raw_query.return_value = mock_response
        
        store = WeaviateVectorStore()
        store.query("test_namespace", [0.1, 0.2, 0.3])
        
        # Verify default top_k=5 is used
        query_arg = mock_client.graphql_raw_query.call_args[0][0]
        assert "limit: 5" in query_arg


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