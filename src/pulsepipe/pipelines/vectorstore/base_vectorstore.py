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

# src/pulsepipe/pipelines/vectorstore/vectorstore.py

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class VectorStore(ABC):
    @abstractmethod
    def upsert(self, namespace: str, vectors: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def query(self, namespace: str, query_vector: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        pass


class VectorStoreConnectionError(Exception):
    def __init__(self, engine: str, host: str, port: int):
        message = f"❌ Unable to connect to {engine} vector store at {host}:{port} 🚫🛜"
        super().__init__(message)
