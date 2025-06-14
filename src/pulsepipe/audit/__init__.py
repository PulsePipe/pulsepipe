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

# src/pulsepipe/audit/__init__.py

from .audit_logger import (
    AuditLogger,
    AuditEvent,
    AuditLevel,
    EventType
)
from .error_classifier import (
    ErrorClassifier,
    ErrorAnalysis,
    ClassifiedError
)
from .audit_reporter import (
    AuditReporter,
    AuditReport,
    ProcessingSummary
)
from .ingestion_tracker import (
    IngestionTracker,
    IngestionRecord,
    IngestionBatchMetrics,
    IngestionSummary,
    IngestionOutcome,
    IngestionStage
)
from .chunking_tracker import (
    ChunkingTracker,
    ChunkingRecord,
    ChunkingBatchMetrics,
    ChunkingSummary,
    ChunkingOutcome,
    ChunkingStage
)
from .deid_tracker import (
    DeidTracker,
    DeidRecord,
    DeidBatchMetrics,
    DeidSummary,
    DeidOutcome,
    DeidStage
)
from .embedding_tracker import (
    EmbeddingTracker,
    EmbeddingRecord,
    EmbeddingBatchMetrics,
    EmbeddingSummary,
    EmbeddingOutcome,
    EmbeddingStage
)
from .vector_db_tracker import (
    VectorDbTracker,
    VectorDbRecord,
    VectorDbBatchMetrics,
    VectorDbSummary,
    VectorDbOutcome,
    VectorDbStage
)