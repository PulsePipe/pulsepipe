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

# src/pulsepipe/persistence/models.py

"""
Database models for data intelligence and audit tracking.

Defines the database schema for tracking ingestion statistics,
audit trails, quality metrics, and performance data.
"""

import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
import json

from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


class ProcessingStatus(str, Enum):
    """Status values for processing records."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"
    SKIPPED = "skipped"


class ErrorCategory(str, Enum):
    """Categories for error classification."""
    SCHEMA_ERROR = "schema_error"
    VALIDATION_ERROR = "validation_error"
    PARSE_ERROR = "parse_error"
    TRANSFORMATION_ERROR = "transformation_error"
    SYSTEM_ERROR = "system_error"
    DATA_QUALITY_ERROR = "data_quality_error"
    NETWORK_ERROR = "network_error"
    PERMISSION_ERROR = "permission_error"


class DataIntelligenceSchema:
    """
    Database schema manager for data intelligence tracking.
    
    Handles creation and management of tables for tracking ingestion,
    audit trails, quality metrics, and performance data.
    """
    
    def __init__(self, connection: sqlite3.Connection):
        """
        Initialize schema manager.
        
        Args:
            connection: SQLite database connection
        """
        self.conn = connection
        self.conn.execute("PRAGMA foreign_keys = ON")
        
    def create_tables(self) -> None:
        """Create all data intelligence tracking tables."""
        logger.info("Creating data intelligence tracking tables")
        
        # Create tables in dependency order
        self._create_pipeline_runs_table()
        self._create_ingestion_stats_table()
        self._create_failed_records_table()
        self._create_audit_events_table()
        self._create_quality_metrics_table()
        self._create_performance_metrics_table()
        self._create_system_metrics_table()
        
        self.conn.commit()
        logger.info("Data intelligence tracking tables created successfully")
    
    def _create_pipeline_runs_table(self) -> None:
        """Create pipeline_runs table for tracking pipeline executions."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'running',
                total_records INTEGER DEFAULT 0,
                successful_records INTEGER DEFAULT 0,
                failed_records INTEGER DEFAULT 0,
                skipped_records INTEGER DEFAULT 0,
                config_snapshot TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for common queries
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name ON pipeline_runs(name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status)")
    
    def _create_ingestion_stats_table(self) -> None:
        """Create ingestion_stats table for tracking ingestion success/failure."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                file_path TEXT,
                record_id TEXT,
                record_type TEXT,
                status TEXT NOT NULL,
                error_category TEXT,
                error_message TEXT,
                error_details TEXT,
                processing_time_ms INTEGER,
                record_size_bytes INTEGER,
                data_source TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
            )
        """)
        
        # Create indexes for analytics queries
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_pipeline_run ON ingestion_stats(pipeline_run_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_status ON ingestion_stats(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_stage ON ingestion_stats(stage_name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_error_category ON ingestion_stats(error_category)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_timestamp ON ingestion_stats(timestamp)")
    
    def _create_failed_records_table(self) -> None:
        """Create failed_records table for storing complete failed record data."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS failed_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingestion_stat_id INTEGER NOT NULL,
                original_data TEXT NOT NULL,
                normalized_data TEXT,
                failure_reason TEXT NOT NULL,
                stack_trace TEXT,
                retry_count INTEGER DEFAULT 0,
                last_retry_at TIMESTAMP,
                resolved_at TIMESTAMP,
                resolution_notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ingestion_stat_id) REFERENCES ingestion_stats(id)
            )
        """)
        
        # Create indexes for failure analysis
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_failed_records_ingestion_stat ON failed_records(ingestion_stat_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_failed_records_retry_count ON failed_records(retry_count)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_failed_records_resolved ON failed_records(resolved_at)")
    
    def _create_audit_events_table(self) -> None:
        """Create audit_events table for detailed audit trail."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                record_id TEXT,
                event_level TEXT NOT NULL DEFAULT 'INFO',
                message TEXT NOT NULL,
                details TEXT,
                user_context TEXT,
                system_context TEXT,
                correlation_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
            )
        """)
        
        # Create indexes for audit queries
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_pipeline_run ON audit_events(pipeline_run_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_type ON audit_events(event_type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_level ON audit_events(event_level)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON audit_events(timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_correlation ON audit_events(correlation_id)")
    
    def _create_quality_metrics_table(self) -> None:
        """Create quality_metrics table for data quality scoring."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quality_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                record_id TEXT,
                record_type TEXT,
                completeness_score REAL,
                consistency_score REAL,
                validity_score REAL,
                accuracy_score REAL,
                overall_score REAL,
                missing_fields TEXT,
                invalid_fields TEXT,
                outlier_fields TEXT,
                quality_issues TEXT,
                metrics_details TEXT,
                sampled BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
            )
        """)
        
        # Create indexes for quality analysis
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_pipeline_run ON quality_metrics(pipeline_run_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_record_type ON quality_metrics(record_type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_overall_score ON quality_metrics(overall_score)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_sampled ON quality_metrics(sampled)")
    
    def _create_performance_metrics_table(self) -> None:
        """Create performance_metrics table for tracking performance data."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP NOT NULL,
                duration_ms INTEGER NOT NULL,
                records_processed INTEGER DEFAULT 0,
                records_per_second REAL,
                memory_usage_mb REAL,
                cpu_usage_percent REAL,
                disk_io_bytes INTEGER,
                network_io_bytes INTEGER,
                bottleneck_indicator TEXT,
                optimization_suggestions TEXT,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
            )
        """)
        
        # Create indexes for performance analysis
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_pipeline_run ON performance_metrics(pipeline_run_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_stage ON performance_metrics(stage_name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_duration ON performance_metrics(duration_ms)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_rps ON performance_metrics(records_per_second)")
    
    def _create_system_metrics_table(self) -> None:
        """Create system_metrics table for environmental system information."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS system_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                hostname TEXT,
                os_name TEXT,
                os_version TEXT,
                python_version TEXT,
                cpu_model TEXT,
                cpu_cores INTEGER,
                cpu_threads INTEGER,
                memory_total_gb REAL,
                memory_available_gb REAL,
                disk_total_gb REAL,
                disk_free_gb REAL,
                gpu_available BOOLEAN DEFAULT FALSE,
                gpu_model TEXT,
                gpu_memory_gb REAL,
                network_interfaces TEXT,
                environment_variables TEXT,
                package_versions TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
            )
        """)
        
        # Create indexes for system analysis
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_pipeline_run ON system_metrics(pipeline_run_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_hostname ON system_metrics(hostname)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_os ON system_metrics(os_name)")
    
    def drop_tables(self) -> None:
        """Drop all data intelligence tracking tables (for testing/cleanup)."""
        logger.warning("Dropping all data intelligence tracking tables")
        
        tables = [
            "system_metrics",
            "performance_metrics", 
            "quality_metrics",
            "audit_events",
            "failed_records",
            "ingestion_stats",
            "pipeline_runs"
        ]
        
        for table in tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        
        self.conn.commit()
        logger.info("All data intelligence tracking tables dropped")
    
    def get_table_info(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get information about all tracking tables.
        
        Returns:
            Dictionary mapping table names to their column information
        """
        tables = [
            "pipeline_runs",
            "ingestion_stats", 
            "failed_records",
            "audit_events",
            "quality_metrics",
            "performance_metrics",
            "system_metrics"
        ]
        
        table_info = {}
        for table in tables:
            cursor = self.conn.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            table_info[table] = [
                {
                    "name": col[1],
                    "type": col[2], 
                    "not_null": bool(col[3]),
                    "default": col[4],
                    "primary_key": bool(col[5])
                }
                for col in columns
            ]
        
        return table_info
    
    def validate_schema(self) -> bool:
        """
        Validate that all required tables exist and have correct structure.
        
        Returns:
            True if schema is valid, False otherwise
        """
        required_tables = [
            "pipeline_runs",
            "ingestion_stats",
            "failed_records", 
            "audit_events",
            "quality_metrics",
            "performance_metrics",
            "system_metrics"
        ]
        
        try:
            # Check if all tables exist
            cursor = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            existing_tables = {row[0] for row in cursor.fetchall()}
            
            missing_tables = set(required_tables) - existing_tables
            if missing_tables:
                logger.error(f"Missing required tables: {missing_tables}")
                return False
            
            # Validate basic structure of key tables
            # Check pipeline_runs has required columns
            cursor = self.conn.execute("PRAGMA table_info(pipeline_runs)")
            pipeline_columns = {col[1] for col in cursor.fetchall()}
            required_pipeline_columns = {
                "id", "name", "started_at", "status", 
                "total_records", "successful_records", "failed_records"
            }
            
            if not required_pipeline_columns.issubset(pipeline_columns):
                missing_cols = required_pipeline_columns - pipeline_columns
                logger.error(f"pipeline_runs missing columns: {missing_cols}")
                return False
            
            logger.info("Database schema validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False


def init_data_intelligence_db(connection: sqlite3.Connection) -> DataIntelligenceSchema:
    """
    Initialize data intelligence database schema.
    
    Args:
        connection: SQLite database connection
        
    Returns:
        DataIntelligenceSchema instance
    """
    schema = DataIntelligenceSchema(connection)
    
    # Check if tables already exist and are valid
    if not schema.validate_schema():
        logger.info("Creating data intelligence tracking schema")
        schema.create_tables()
    else:
        logger.info("Data intelligence tracking schema already exists and is valid")
    
    return schema