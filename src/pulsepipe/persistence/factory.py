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

# src/pulsepipe/persistence/factory.py

import sqlite3
from pathlib import Path
from typing import Optional

from .models import init_data_intelligence_db, DataIntelligenceSchema
from .tracking_repository import TrackingRepository

def get_shared_sqlite_connection(config: dict) -> sqlite3.Connection:
    db_path = config.get("persistence", {}).get("sqlite", {}).get(
        "db_path", ".pulsepipe/state/ingestion.sqlite3"
    )
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    # Convert Path to string to avoid "expected str, bytes or os.PathLike, not Connection" error on Windows
    return sqlite3.connect(str(db_file))


def get_tracking_repository(config: dict, connection: Optional[sqlite3.Connection] = None) -> TrackingRepository:
    """
    Get a tracking repository instance with initialized schema.
    
    Args:
        config: Configuration dictionary
        connection: Optional existing connection, creates new one if None
        
    Returns:
        TrackingRepository instance ready for use
    """
    if connection is None:
        connection = get_shared_sqlite_connection(config)
    
    # Initialize the data intelligence schema
    init_data_intelligence_db(connection)
    
    return TrackingRepository(connection)


def get_data_intelligence_schema(config: dict, connection: Optional[sqlite3.Connection] = None) -> DataIntelligenceSchema:
    """
    Get a data intelligence schema manager.
    
    Args:
        config: Configuration dictionary
        connection: Optional existing connection, creates new one if None
        
    Returns:
        DataIntelligenceSchema instance
    """
    if connection is None:
        connection = get_shared_sqlite_connection(config)
    
    return init_data_intelligence_db(connection)
