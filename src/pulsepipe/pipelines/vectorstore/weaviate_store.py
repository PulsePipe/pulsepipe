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

# src/pulsepipe/pipelines/vectorstore/weaviate_store.py

from typing import List, Dict, Any
from .base_vectorstore import VectorStore, VectorStoreConnectionError

def connect_to_local_weaviate(host: str = "localhost", port: int = 8080):
    import weaviate
    from weaviate.connect import ConnectionParams
    from weaviate.config import AdditionalConfig
    try:
        client = weaviate.WeaviateClient(
            connection_params=ConnectionParams.from_params(
                http_host=host,
                http_port=port,
                grpc_host=host,
                grpc_port=50051,
                http_secure=False,
                grpc_secure=False,
            ),
            skip_init_checks=True,
            additional_config=AdditionalConfig(skip_version_check=True),
        )
        client.connect()
        return client
    except Exception as e:
        raise VectorStoreConnectionError("Weaviate", host, port) from e


def connect_to_wcs_weaviate(cluster_url: str, auth_api_key: str):
    import weaviate
    try:
        return weaviate.connect_to_wcs(
            cluster_url=cluster_url,
            auth_credentials=weaviate.AuthApiKey(auth_api_key)
        )
    except Exception as e:
        raise VectorStoreConnectionError("Weaviate", cluster_url, 443) from e


# Patch for Weaviate v4 client data object creation

def weaviate_create_object(client, namespace: str, vector: Dict[str, Any]) -> None:
    collection = client.collections.get(namespace)
    collection.data.insert(
        properties=vector["metadata"],
        vector=vector["embedding"]
    )


class WeaviateVectorStore(VectorStore):
    def __init__(self, url: str = "http://localhost:8080"):
        try:
            self.client = connect_to_local_weaviate(host="localhost", port=8080)
        except Exception:
            raise VectorStoreConnectionError("Weaviate", "localhost", 8080)

    def upsert(self, namespace: str, vectors: List[Dict[str, Any]]) -> None:
        for vector in vectors:
            weaviate_create_object(self.client, namespace, vector)

    def query(self, namespace: str, query_vector: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        graphql_query = f"""
        {{
          Get {{
            {namespace}(
              nearVector: {{ vector: {query_vector} }},
              limit: {top_k}
            ) {{
              patient_id
              text
            }}
          }}
        }}
        """
        response = self.client.graphql_raw_query(graphql_query)
        if "data" in response and "Get" in response["data"] and namespace in response["data"]["Get"]:
            return response["data"]["Get"][namespace]
        return []
