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

# src/pulsepipe/pipelines/vectorstore/qdrant_store.py

from typing import List, Dict, Any
from .base_vectorstore import VectorStore, VectorStoreConnectionError
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
import requests


class QdrantVectorStore(VectorStore):
    def __init__(self, url: str = "http://localhost:6333"):
        self.client = QdrantClient(url=url)
        try:
            ready = requests.get(f"{url}/collections", timeout=3)
            if not ready.ok:
                raise VectorStoreConnectionError("Qdrant", url, 6333)
        except Exception:
            raise VectorStoreConnectionError("Qdrant", url, 6333)


    def ensure_collection(self, name: str, vector_size: int = 3):
        try:
            self.client.get_collection(name)
        except Exception:
            from qdrant_client.models import VectorParams
            self.client.recreate_collection(
                collection_name=name,
                vectors_config=VectorParams(size=vector_size, distance="Cosine")
            )


    def upsert(self, namespace: str, vectors: List[Dict[str, Any]]) -> None:
        self.ensure_collection(namespace, vector_size=len(vectors[0]["embedding"]))
        points = [
            {
                "id": v.get("id", i),
                "vector": v["embedding"],
                "payload": v["metadata"]
            }
            for i, v in enumerate(vectors)
        ]
        self.client.upsert(collection_name=namespace, points=points)


    def query(self, namespace: str, query_vector: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        search_result = self.client.query_points(
            collection_name=namespace,
            query=query_vector,
            limit=top_k
        )
        
        # Handle the response based on its structure
        if hasattr(search_result, 'points'):
            # New API format
            return [point.payload for point in search_result.points]
        elif isinstance(search_result, dict) and 'result' in search_result:
            # Older API format
            return [r.payload for r in search_result['result']]
        elif isinstance(search_result, list):
            # Direct list format
            return [r.payload if hasattr(r, 'payload') else r for r in search_result]
        elif hasattr(search_result, '__iter__'):
            # It's some kind of iterable but not what we expected
            # Try to extract payload if available, otherwise return the items
            result = []
            for item in search_result:
                if isinstance(item, tuple):
                    # If it's a tuple, the payload might be one of the elements
                    # Common format is (id, payload, score)
                    if len(item) >= 2 and isinstance(item[1], dict):
                        result.append(item[1])  # Assume second element is payload
                    else:
                        # Return the whole tuple as a dict
                        result.append({"item": item})
                elif hasattr(item, 'payload'):
                    result.append(item.payload)
                elif isinstance(item, dict):
                    result.append(item)
                else:
                    # Fallback: convert to string
                    result.append({"value": str(item)})
            return result
        
        # Fallback for unexpected formats
        return []
