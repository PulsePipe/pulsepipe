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

# tests/test_vectorstore_qdrant.py

import pytest
from pulsepipe.pipelines.vectorstore.qdrant_store import QdrantVectorStore

def test_qdrant_upsert_and_query():
    store = QdrantVectorStore()

    dummy_vectors = [{
        "id": 1,
        "embedding": [0.1, 0.2, 0.3],
        "metadata": {"patient_id": "456", "note": "glucose high"}
    }]
    namespace = "test_qdrant"

    store.upsert(namespace, dummy_vectors)
    results = store.query(namespace, [0.1, 0.2, 0.3], top_k=1)

    assert isinstance(results, list)
