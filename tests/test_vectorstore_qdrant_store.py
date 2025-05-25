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

# tests/test_vectorstore_qdrant_store.py

import pytest
from unittest.mock import Mock, MagicMock, patch
from pulsepipe.pipelines.vectorstore.qdrant_store import QdrantVectorStore
from pulsepipe.pipelines.vectorstore.base_vectorstore import VectorStoreConnectionError


@pytest.fixture
def mock_qdrant_client():
    with patch('pulsepipe.pipelines.vectorstore.qdrant_store.QdrantClient') as mock_client_class, \
         patch('pulsepipe.pipelines.vectorstore.qdrant_store.requests') as mock_requests:
        
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock successful connection check
        mock_response = MagicMock()
        mock_response.ok = True
        mock_requests.get.return_value = mock_response
        
        yield mock_client


def test_qdrant_init_success(mock_qdrant_client):
    store = QdrantVectorStore("http://localhost:6333")
    assert store.url == "http://localhost:6333"
    assert store.client is not None


def test_qdrant_init_connection_failure():
    with patch('pulsepipe.pipelines.vectorstore.qdrant_store.requests') as mock_requests:
        mock_requests.get.side_effect = Exception("Connection failed")
        
        with pytest.raises(VectorStoreConnectionError) as exc_info:
            QdrantVectorStore("http://localhost:6333")
        
        assert "Qdrant" in str(exc_info.value)
        assert "localhost" in str(exc_info.value)
        assert "6333" in str(exc_info.value)


def test_qdrant_init_bad_response():
    with patch('pulsepipe.pipelines.vectorstore.qdrant_store.requests') as mock_requests:
        mock_response = MagicMock()
        mock_response.ok = False
        mock_requests.get.return_value = mock_response
        
        with pytest.raises(VectorStoreConnectionError):
            QdrantVectorStore("http://localhost:6333")


def test_extract_host(mock_qdrant_client):
    store = QdrantVectorStore("http://example.com:6333")
    assert store._extract_host() == "example.com"
    
    store = QdrantVectorStore("http://localhost:6333")
    assert store._extract_host() == "localhost"
    
    store = QdrantVectorStore("https://test-cluster.weaviate.io")
    assert store._extract_host() == "test-cluster.weaviate.io"


def test_extract_port(mock_qdrant_client):
    store = QdrantVectorStore("http://localhost:6333")
    assert store._extract_port() == 6333
    
    store = QdrantVectorStore("http://localhost:8080")
    assert store._extract_port() == 8080
    
    # Test default port when none specified
    store = QdrantVectorStore("http://localhost")
    assert store._extract_port() == 6333


def test_ensure_collection_exists(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock collection exists
    mock_qdrant_client.get_collection.return_value = MagicMock()
    
    store.ensure_collection("test_collection", 384)
    
    mock_qdrant_client.get_collection.assert_called_once_with("test_collection")
    mock_qdrant_client.recreate_collection.assert_not_called()


def test_ensure_collection_creates_new(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock collection doesn't exist
    mock_qdrant_client.get_collection.side_effect = Exception("Collection not found")
    
    with patch('qdrant_client.models.VectorParams') as mock_vector_params:
        mock_vector_config = MagicMock()
        mock_vector_params.return_value = mock_vector_config
        
        store.ensure_collection("new_collection", 512)
        
        mock_vector_params.assert_called_once_with(size=512, distance="Cosine")
        mock_qdrant_client.recreate_collection.assert_called_once_with(
            collection_name="new_collection",
            vectors_config=mock_vector_config
        )


def test_upsert_vectors(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock collection exists
    mock_qdrant_client.get_collection.return_value = MagicMock()
    
    dummy_vectors = [
        {
            "id": "vector_1",
            "embedding": [0.1, 0.2, 0.3],
            "metadata": {"patient_id": "456", "note": "glucose high"}
        },
        {
            "embedding": [0.4, 0.5, 0.6],
            "metadata": {"patient_id": "789", "note": "blood pressure normal"}
        }
    ]
    
    store.upsert("test_namespace", dummy_vectors)
    
    # Verify ensure_collection was called with correct vector size
    mock_qdrant_client.get_collection.assert_called_with("test_namespace")
    
    # Verify upsert was called with correct points structure
    expected_points = [
        {
            "id": "vector_1",
            "vector": [0.1, 0.2, 0.3],
            "payload": {"patient_id": "456", "note": "glucose high"}
        },
        {
            "id": 1,  # Auto-assigned ID
            "vector": [0.4, 0.5, 0.6],
            "payload": {"patient_id": "789", "note": "blood pressure normal"}
        }
    ]
    
    mock_qdrant_client.upsert.assert_called_once_with(
        collection_name="test_namespace",
        points=expected_points
    )


def test_query_new_api_format(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock search result with new API format (has .points attribute)
    mock_point = MagicMock()
    mock_point.payload = {"patient_id": "456", "note": "glucose high"}
    
    mock_search_result = MagicMock()
    mock_search_result.points = [mock_point]
    
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.2, 0.3], top_k=5)
    
    mock_qdrant_client.query_points.assert_called_once_with(
        collection_name="test_namespace",
        query=[0.1, 0.2, 0.3],
        limit=5
    )
    
    assert len(results) == 1
    assert results[0] == {"patient_id": "456", "note": "glucose high"}


def test_query_dict_result_format(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock search result with dict format
    mock_result_item = MagicMock()
    mock_result_item.payload = {"patient_id": "789", "text": "normal values"}
    
    mock_search_result = {
        "result": [mock_result_item]
    }
    
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.4, 0.5, 0.6], top_k=3)
    
    assert len(results) == 1
    assert results[0] == {"patient_id": "789", "text": "normal values"}


def test_query_list_format(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock search result as direct list
    mock_item = MagicMock()
    mock_item.payload = {"patient_id": "123", "diagnosis": "diabetes"}
    
    mock_search_result = [mock_item]
    
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.7, 0.8, 0.9])
    
    assert len(results) == 1
    assert results[0] == {"patient_id": "123", "diagnosis": "diabetes"}


def test_query_tuple_format(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Mock search result with tuple format (id, payload, score)
    mock_search_result = [
        ("id_1", {"patient_id": "111", "status": "active"}, 0.95),
        ("id_2", {"patient_id": "222", "status": "inactive"}, 0.85)
    ]
    
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    
    # Results should contain the original tuples since they have no .payload attribute
    assert len(results) == 2
    assert results[0] == ("id_1", {"patient_id": "111", "status": "active"}, 0.95)
    assert results[1] == ("id_2", {"patient_id": "222", "status": "inactive"}, 0.85)


def test_query_fallback_formats(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Test with unexpected tuple format - as a list, tuples are returned as-is
    mock_search_result = [("just_id",)]
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    assert len(results) == 1
    assert results[0] == ("just_id",)
    
    # Test with dict items - as a list, dicts are returned as-is
    mock_search_result = [{"patient_id": "direct_dict"}]
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    assert len(results) == 1
    assert results[0] == {"patient_id": "direct_dict"}
    
    # Test with string items - as a list, strings are returned as-is
    mock_search_result = ["some_string_result"]
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    assert len(results) == 1
    assert results[0] == "some_string_result"


def test_query_iterable_fallback_formats(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Test with custom iterable that's not a list but has __iter__
    class CustomIterable:
        def __init__(self, items):
            self.items = items
        def __iter__(self):
            return iter(self.items)
    
    # Test with tuple format in custom iterable
    mock_search_result = CustomIterable([
        ("id_1", {"patient_id": "111", "status": "active"}, 0.95)
    ])
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    assert len(results) == 1
    assert results[0] == {"patient_id": "111", "status": "active"}  # Second element extracted
    
    # Test with unexpected tuple format in custom iterable
    mock_search_result = CustomIterable([("just_id",)])
    mock_qdrant_client.query_points.return_value = mock_search_result
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    assert len(results) == 1
    assert results[0] == {"item": ("just_id",)}


def test_query_empty_result(mock_qdrant_client):
    store = QdrantVectorStore()
    
    # Test with completely unexpected format that's not iterable
    mock_qdrant_client.query_points.return_value = 42  # Non-iterable number
    
    results = store.query("test_namespace", [0.1, 0.1, 0.1])
    
    assert results == []