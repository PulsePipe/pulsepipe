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

# src/pulsepipe/persistence/__init__.py

"""Persistence layer for data intelligence and audit tracking."""

# Async persistence interface
from .factory import (
    create_persistence_provider,
    get_async_tracking_repository,
    validate_persistence_config
)
from .base import (
    BasePersistenceProvider,
    BaseTrackingRepository,
    PipelineRunSummary,
    IngestionStat,
    QualityMetric
)
from .sqlite_provider import SQLitePersistenceProvider
from .mongodb_provider import MongoDBPersistenceProvider
from .postgresql_provider import PostgreSQLPersistenceProvider
from .sqlserver_provider import SQLServerPersistenceProvider

# Enums (database-agnostic)
from .models import (
    ProcessingStatus,
    ErrorCategory
)

__all__ = [
    # Async persistence interface
    "create_persistence_provider",
    "get_async_tracking_repository", 
    "validate_persistence_config",
    "BasePersistenceProvider",
    "BaseTrackingRepository",
    "SQLitePersistenceProvider",
    "MongoDBPersistenceProvider",
    "PostgreSQLPersistenceProvider",
    "SQLServerPersistenceProvider",
    "PipelineRunSummary",
    "IngestionStat",
    "QualityMetric",
    
    # Enums
    "ProcessingStatus",
    "ErrorCategory",
]