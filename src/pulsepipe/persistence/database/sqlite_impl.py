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

# src/pulsepipe/persistence/database/sqlite_impl.py

"""
SQLite implementation of database connection and SQL dialect.

Provides SQLite-specific implementations while maintaining backward compatibility.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager

from .connection import DatabaseConnection, DatabaseResult
from .dialect import DatabaseDialect
from .exceptions import (
    ConnectionError,
    QueryError,
    TransactionError,
    wrap_database_error
)


class SQLiteConnection(DatabaseConnection):
    """
    SQLite implementation of DatabaseConnection.
    
    Wraps sqlite3.Connection to provide the common database interface.
    """
    
    def __init__(self, db_path: str, timeout: Optional[float] = None):
        """
        Initialize SQLite connection.
        
        Args:
            db_path: Path to SQLite database file
            timeout: Connection timeout in seconds
        """
        self.db_path = db_path
        self.timeout = timeout or 30.0
        self._connection: Optional[sqlite3.Connection] = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish SQLite connection."""
        try:
            # Ensure parent directory exists
            db_file = Path(self.db_path)
            db_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Create connection with row factory for dict-like access
            self._connection = sqlite3.connect(
                str(db_file),
                timeout=self.timeout,
                check_same_thread=False
            )
            self._connection.row_factory = sqlite3.Row
            
            # Enable foreign key constraints
            self._connection.execute("PRAGMA foreign_keys = ON")
            
        except sqlite3.Error as e:
            raise ConnectionError(
                f"Failed to connect to SQLite database: {self.db_path}",
                {"db_path": self.db_path, "timeout": self.timeout},
                e
            )
    
    def execute(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> DatabaseResult:
        """Execute a single SQLite query."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            if params is None:
                cursor = self._connection.execute(query)
            else:
                cursor = self._connection.execute(query, params)
            
            # Convert rows to dictionaries
            rows = [dict(row) for row in cursor.fetchall()]
            
            return DatabaseResult(
                rows=rows,
                lastrowid=cursor.lastrowid,
                rowcount=cursor.rowcount
            )
            
        except sqlite3.Error as e:
            raise QueryError(
                f"SQLite query execution failed: {query[:100]}...",
                {"query": query, "params": params},
                e
            )
    
    def executemany(self, query: str, params_list: List[Union[Tuple, Dict]]) -> DatabaseResult:
        """Execute a query multiple times with different parameters."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            cursor = self._connection.executemany(query, params_list)
            
            return DatabaseResult(
                rows=[],  # executemany doesn't return rows
                lastrowid=cursor.lastrowid,
                rowcount=cursor.rowcount
            )
            
        except sqlite3.Error as e:
            raise QueryError(
                f"SQLite executemany failed: {query[:100]}...",
                {"query": query, "params_count": len(params_list)},
                e
            )
    
    def commit(self) -> None:
        """Commit the current transaction."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            self._connection.commit()
        except sqlite3.Error as e:
            raise TransactionError("Failed to commit transaction", original_error=e)
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            self._connection.rollback()
        except sqlite3.Error as e:
            raise TransactionError("Failed to rollback transaction", original_error=e)
    
    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
            except sqlite3.Error as e:
                raise ConnectionError("Failed to close database connection", original_error=e)
    
    def is_connected(self) -> bool:
        """Check if the connection is still active."""
        if not self._connection:
            return False
        
        try:
            # Try a simple query to test connection
            self._connection.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection."""
        return {
            "database_type": "sqlite",
            "db_path": self.db_path,
            "timeout": self.timeout,
            "is_connected": self.is_connected()
        }
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise
    
    def get_raw_connection(self) -> sqlite3.Connection:
        """
        Get the underlying sqlite3.Connection for advanced operations.
        
        Returns:
            Raw sqlite3.Connection object
        """
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        return self._connection
    
    def init_schema(self) -> None:
        """
        Initialize database schema for tracking and audit data.
        
        Creates all necessary tables for pipeline tracking, ingestion statistics,
        audit events, quality metrics, and performance data.
        """
        schema_sql = [
            # Pipeline runs table
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'running',
                total_records INTEGER DEFAULT 0,
                successful_records INTEGER DEFAULT 0,
                failed_records INTEGER DEFAULT 0,
                skipped_records INTEGER DEFAULT 0,
                error_message TEXT,
                config_snapshot TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Ingestion statistics table
            """
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
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
            )
            """,
            
            # Failed records table
            """
            CREATE TABLE IF NOT EXISTS failed_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingestion_stat_id INTEGER NOT NULL,
                original_data TEXT,
                normalized_data TEXT,
                failure_reason TEXT,
                stack_trace TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ingestion_stat_id) REFERENCES ingestion_stats(id) ON DELETE CASCADE
            )
            """,
            
            # Audit events table
            """
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                stage_name TEXT,
                record_id TEXT,
                event_level TEXT DEFAULT 'INFO',
                message TEXT,
                details TEXT,
                correlation_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
            )
            """,
            
            # Quality metrics table
            """
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
                sampled BOOLEAN DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
            )
            """,
            
            # Performance metrics table
            """
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP NOT NULL,
                duration_ms INTEGER NOT NULL,
                records_processed INTEGER,
                records_per_second REAL,
                memory_usage_mb REAL,
                cpu_usage_percent REAL,
                bottleneck_indicator TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
            )
            """,
            
            # System metrics table
            """
            CREATE TABLE IF NOT EXISTS system_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                cpu_usage_percent REAL,
                memory_usage_mb REAL,
                memory_usage_percent REAL,
                disk_usage_mb REAL,
                disk_io_read_mb REAL,
                disk_io_write_mb REAL,
                network_io_sent_mb REAL,
                network_io_received_mb REAL,
                active_threads INTEGER,
                process_count INTEGER,
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
            )
            """
        ]
        
        # Create indexes for better performance
        index_sql = [
            "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status)",
            "CREATE INDEX IF NOT EXISTS idx_ingestion_stats_pipeline_run_id ON ingestion_stats(pipeline_run_id)",
            "CREATE INDEX IF NOT EXISTS idx_ingestion_stats_status ON ingestion_stats(status)",
            "CREATE INDEX IF NOT EXISTS idx_ingestion_stats_timestamp ON ingestion_stats(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_audit_events_pipeline_run_id ON audit_events(pipeline_run_id)",
            "CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON audit_events(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_quality_metrics_pipeline_run_id ON quality_metrics(pipeline_run_id)",
            "CREATE INDEX IF NOT EXISTS idx_performance_metrics_pipeline_run_id ON performance_metrics(pipeline_run_id)",
            "CREATE INDEX IF NOT EXISTS idx_system_metrics_pipeline_run_id ON system_metrics(pipeline_run_id)"
        ]
        
        try:
            # Execute schema creation
            for sql in schema_sql:
                self._connection.execute(sql)
            
            # Create indexes
            for sql in index_sql:
                self._connection.execute(sql)
            
            # Commit changes
            self._connection.commit()
            
        except sqlite3.Error as e:
            self._connection.rollback()
            raise QueryError(
                "Failed to initialize database schema",
                {"db_path": self.db_path},
                e
            )


class SQLiteDialect(DatabaseDialect):
    """
    SQLite implementation of SQL dialect.
    
    Provides SQLite-specific SQL generation and data handling.
    """
    
    def get_pipeline_run_insert(self) -> str:
        """Get SQL for inserting a pipeline run record."""
        return """
            INSERT INTO pipeline_runs (
                id, name, started_at, status, config_snapshot
            ) VALUES (?, ?, ?, ?, ?)
        """
    
    def get_pipeline_run_update(self) -> str:
        """Get SQL for updating a pipeline run record."""
        return """
            UPDATE pipeline_runs 
            SET completed_at = ?, status = ?, error_message = ?, updated_at = ?
            WHERE id = ?
        """
    
    def get_pipeline_run_select(self) -> str:
        """Get SQL for selecting a pipeline run by ID."""
        return """
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records, 
                   skipped_records, error_message
            FROM pipeline_runs 
            WHERE id = ?
        """
    
    def get_pipeline_runs_list(self) -> str:
        """Get SQL for listing recent pipeline runs."""
        return """
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records,
                   skipped_records, error_message
            FROM pipeline_runs 
            ORDER BY started_at DESC 
            LIMIT ?
        """
    
    def get_pipeline_run_count_update(self) -> str:
        """Get SQL for updating pipeline run counts incrementally."""
        return """
            UPDATE pipeline_runs 
            SET total_records = total_records + ?, 
                successful_records = successful_records + ?, 
                failed_records = failed_records + ?, 
                skipped_records = skipped_records + ?, 
                updated_at = ?
            WHERE id = ?
        """
    
    def get_recent_pipeline_runs(self, limit: int = 10) -> str:
        """Get SQL for recent pipeline runs."""
        return f"""
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records,
                   skipped_records, error_message
            FROM pipeline_runs 
            ORDER BY started_at DESC 
            LIMIT {limit}
        """
    
    def get_ingestion_stat_insert(self) -> str:
        """Get SQL for inserting an ingestion statistic."""
        return """
            INSERT INTO ingestion_stats (
                pipeline_run_id, stage_name, file_path, record_id, record_type,
                status, error_category, error_message, error_details,
                processing_time_ms, record_size_bytes, data_source, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_failed_record_insert(self) -> str:
        """Get SQL for inserting a failed record."""
        return """
            INSERT INTO failed_records (
                ingestion_stat_id, original_data, normalized_data,
                failure_reason, stack_trace
            ) VALUES (?, ?, ?, ?, ?)
        """
    
    def get_audit_event_insert(self) -> str:
        """Get SQL for inserting an audit event."""
        return """
            INSERT INTO audit_events (
                pipeline_run_id, event_type, stage_name, record_id,
                event_level, message, details, correlation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_quality_metric_insert(self) -> str:
        """Get SQL for inserting a quality metric."""
        return """
            INSERT INTO quality_metrics (
                pipeline_run_id, record_id, record_type,
                completeness_score, consistency_score, validity_score,
                accuracy_score, overall_score, missing_fields,
                invalid_fields, outlier_fields, quality_issues,
                metrics_details, sampled, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_performance_metric_insert(self) -> str:
        """Get SQL for inserting a performance metric."""
        return """
            INSERT INTO performance_metrics (
                pipeline_run_id, stage_name, started_at, completed_at,
                duration_ms, records_processed, records_per_second,
                memory_usage_mb, cpu_usage_percent, bottleneck_indicator
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_chunking_stat_insert(self) -> str:
        """Get SQL for inserting a chunking statistic."""
        return """
            INSERT INTO chunking_stats (
                pipeline_run_id, stage_name, source_id, record_id, chunk_type,
                status, error_category, error_message, error_details,
                processing_time_ms, chunk_count, total_chars, avg_chunk_size,
                overlap_chars, chunker_type, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_deid_stat_insert(self) -> str:
        """Get SQL for inserting a de-identification statistic."""
        return """
            INSERT INTO deid_stats (
                pipeline_run_id, stage_name, source_id, record_id, content_type,
                status, error_category, error_message, error_details,
                processing_time_ms, phi_entities_detected, phi_entities_removed,
                confidence_scores, deid_method, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_embedding_stat_insert(self) -> str:
        """Get SQL for inserting an embedding statistic."""
        return """
            INSERT INTO embedding_stats (
                pipeline_run_id, stage_name, source_id, record_id, content_type,
                status, error_category, error_message, error_details,
                processing_time_ms, chunk_count, embedding_dimensions, model_name,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_vector_db_stat_insert(self) -> str:
        """Get SQL for inserting a vector database statistic."""
        return """
            INSERT INTO vector_db_stats (
                pipeline_run_id, stage_name, source_id, record_id, content_type,
                status, error_category, error_message, error_details,
                processing_time_ms, vector_count, index_name, collection_name,
                vector_store_type, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> Tuple[str, List[Any]]:
        """Get SQL for ingestion summary with optional filters."""
        where_conditions = []
        params = []
        
        if pipeline_run_id:
            where_conditions.append("pipeline_run_id = ?")
            params.append(pipeline_run_id)
        
        if start_date:
            where_conditions.append("timestamp >= ?")
            params.append(self.format_datetime(start_date))
        
        if end_date:
            where_conditions.append("timestamp <= ?")
            params.append(self.format_datetime(end_date))
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        sql = f"""
            SELECT 
                status,
                error_category,
                COUNT(*) as count,
                AVG(processing_time_ms) as avg_processing_time,
                SUM(record_size_bytes) as total_bytes
            FROM ingestion_stats 
            {where_clause}
            GROUP BY status, error_category
        """
        
        return sql, params
    
    def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Tuple[str, List[Any]]:
        """Get SQL for quality summary with optional filters."""
        where_clause = "WHERE pipeline_run_id = ?" if pipeline_run_id else ""
        params = [pipeline_run_id] if pipeline_run_id else []
        
        sql = f"""
            SELECT 
                COUNT(*) as total_records,
                AVG(completeness_score) as avg_completeness,
                AVG(consistency_score) as avg_consistency,
                AVG(validity_score) as avg_validity,
                AVG(accuracy_score) as avg_accuracy,
                AVG(overall_score) as avg_overall_score,
                MIN(overall_score) as min_score,
                MAX(overall_score) as max_score
            FROM quality_metrics 
            {where_clause}
        """
        
        return sql, params
    
    def get_cleanup(self, cutoff_date: datetime) -> List[Tuple[str, List[Any]]]:
        """Get SQL statements for cleaning up old data."""
        # First get the pipeline run IDs to delete
        get_runs_sql = "SELECT id FROM pipeline_runs WHERE started_at < ?"
        
        # Delete statements in dependency order
        cleanup_statements = [
            # Delete failed_records first (they reference ingestion_stats)
            ("""
                DELETE FROM failed_records 
                WHERE ingestion_stat_id IN (
                    SELECT id FROM ingestion_stats 
                    WHERE pipeline_run_id IN (
                        SELECT id FROM pipeline_runs WHERE started_at < ?
                    )
                )
            """, [cutoff_date]),
            
            # Delete other related data
            ("DELETE FROM system_metrics WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < ?)", [cutoff_date]),
            ("DELETE FROM performance_metrics WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < ?)", [cutoff_date]),
            ("DELETE FROM quality_metrics WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < ?)", [cutoff_date]),
            ("DELETE FROM audit_events WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < ?)", [cutoff_date]),
            ("DELETE FROM ingestion_stats WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < ?)", [cutoff_date]),
            
            # Delete pipeline runs last
            ("DELETE FROM pipeline_runs WHERE started_at < ?", [cutoff_date])
        ]
        
        return cleanup_statements
    
    def format_datetime(self, dt: datetime) -> str:
        """Format datetime for SQLite storage."""
        return dt.isoformat()
    
    def parse_datetime(self, dt_str: str) -> datetime:
        """Parse datetime from SQLite storage format."""
        return datetime.fromisoformat(dt_str)
    
    def get_auto_increment_syntax(self) -> str:
        """Get SQLite auto-increment syntax."""
        return "INTEGER PRIMARY KEY AUTOINCREMENT"
    
    def get_json_column_type(self) -> str:
        """Get SQLite JSON column type."""
        return "TEXT"
    
    def serialize_json(self, data: Any) -> str:
        """Serialize data to JSON for SQLite storage."""
        return json.dumps(data) if data is not None else None
    
    def deserialize_json(self, json_str: Optional[str]) -> Any:
        """Deserialize JSON from SQLite storage."""
        return json.loads(json_str) if json_str else None
    
    def escape_identifier(self, identifier: str) -> str:
        """Escape SQLite identifier."""
        return f'"{identifier}"'
    
    def get_limit_syntax(self, limit: int, offset: Optional[int] = None) -> str:
        """Get SQLite LIMIT/OFFSET syntax."""
        if offset is not None:
            return f"LIMIT {limit} OFFSET {offset}"
        return f"LIMIT {limit}"
    
    def supports_feature(self, feature: str) -> bool:
        """Check if SQLite supports a specific feature."""
        sqlite_features = {
            "transactions",
            "basic_sql",
            "foreign_keys",
            "json_extract",
            "full_text_search"
        }
        return feature in sqlite_features
    
    # Bookmark Store SQL Methods
    
    def get_bookmark_table_create(self) -> str:
        """Get SQL for creating bookmarks table."""
        return """
            CREATE TABLE IF NOT EXISTS bookmarks (
                path TEXT PRIMARY KEY,
                status TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_bookmark_check(self) -> str:
        """Get SQL for checking if a bookmark exists."""
        return "SELECT 1 FROM bookmarks WHERE path = ?"
    
    def get_bookmark_insert(self) -> str:
        """Get SQL for inserting a bookmark."""
        return "INSERT OR IGNORE INTO bookmarks (path, status) VALUES (?, ?)"
    
    def get_bookmark_list(self) -> str:
        """Get SQL for listing all bookmarks."""
        return "SELECT path FROM bookmarks ORDER BY path"
    
    def get_bookmark_clear(self) -> str:
        """Get SQL for clearing all bookmarks."""
        return "DELETE FROM bookmarks"