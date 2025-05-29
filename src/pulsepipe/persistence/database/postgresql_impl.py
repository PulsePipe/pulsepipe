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

# src/pulsepipe/persistence/database/postgresql_impl.py

"""
PostgreSQL implementation of database connection and SQL dialect.

Provides PostgreSQL-specific implementations with connection pooling support.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
    from psycopg2 import sql
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

from .connection import DatabaseConnection, DatabaseResult
from .dialect import SQLDialect
from .exceptions import (
    ConnectionError,
    QueryError,
    TransactionError,
    ConfigurationError,
    wrap_database_error
)


class PostgreSQLConnection(DatabaseConnection):
    """
    PostgreSQL implementation of DatabaseConnection.
    
    Uses psycopg2 with connection pooling for improved performance.
    """
    
    def __init__(self, host: str, port: int, database: str, username: str, 
                 password: str, pool_size: int = 5, max_overflow: int = 10):
        """
        Initialize PostgreSQL connection.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            pool_size: Base connection pool size
            max_overflow: Maximum overflow connections
        """
        if not PSYCOPG2_AVAILABLE:
            raise ConfigurationError(
                "PostgreSQL support requires psycopg2. Install with: pip install psycopg2-binary"
            )
        
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._connection: Optional[psycopg2.extensions.connection] = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish PostgreSQL connection pool."""
        try:
            # Create connection pool
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size + self.max_overflow,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            
            # Get a connection from the pool for immediate use
            self._connection = self._pool.getconn()
            
        except psycopg2.Error as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL database: {self.host}:{self.port}/{self.database}",
                {
                    "host": self.host,
                    "port": self.port,
                    "database": self.database,
                    "username": self.username
                },
                e
            )
    
    def execute(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> DatabaseResult:
        """Execute a single PostgreSQL query."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, params)
                
                # Get results if this was a SELECT query
                try:
                    rows = [dict(row) for row in cursor.fetchall()]
                except psycopg2.ProgrammingError:
                    # No results to fetch (INSERT, UPDATE, DELETE)
                    rows = []
                
                return DatabaseResult(
                    rows=rows,
                    lastrowid=cursor.lastrowid if hasattr(cursor, 'lastrowid') else None,
                    rowcount=cursor.rowcount
                )
                
        except psycopg2.Error as e:
            raise wrap_database_error(
                e,
                f"PostgreSQL query execution failed: {query[:100]}...",
                {"query": query, "params": params}
            )
    
    def executemany(self, query: str, params_list: List[Union[Tuple, Dict]]) -> DatabaseResult:
        """Execute a query multiple times with different parameters."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            with self._connection.cursor() as cursor:
                cursor.executemany(query, params_list)
                
                return DatabaseResult(
                    rows=[],  # executemany doesn't return rows
                    lastrowid=cursor.lastrowid if hasattr(cursor, 'lastrowid') else None,
                    rowcount=cursor.rowcount
                )
                
        except psycopg2.Error as e:
            raise wrap_database_error(
                e,
                f"PostgreSQL executemany failed: {query[:100]}...",
                {"query": query, "params_count": len(params_list)}
            )
    
    def commit(self) -> None:
        """Commit the current transaction."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            self._connection.commit()
        except psycopg2.Error as e:
            raise TransactionError("Failed to commit transaction", original_error=e)
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        
        try:
            self._connection.rollback()
        except psycopg2.Error as e:
            raise TransactionError("Failed to rollback transaction", original_error=e)
    
    def close(self) -> None:
        """Close the database connection and pool."""
        if self._connection and self._pool:
            try:
                self._pool.putconn(self._connection)
                self._connection = None
            except psycopg2.Error as e:
                raise ConnectionError("Failed to return connection to pool", original_error=e)
        
        if self._pool:
            try:
                self._pool.closeall()
                self._pool = None
            except psycopg2.Error as e:
                raise ConnectionError("Failed to close connection pool", original_error=e)
    
    def is_connected(self) -> bool:
        """Check if the connection is still active."""
        if not self._connection:
            return False
        
        try:
            # Try a simple query to test connection
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except psycopg2.Error:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection."""
        return {
            "database_type": "postgresql",
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
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
    
    def get_raw_connection(self):
        """
        Get the underlying psycopg2 connection for advanced operations.
        
        Returns:
            Raw psycopg2 connection object
        """
        if not self._connection:
            raise ConnectionError("Database connection is not established")
        return self._connection


class PostgreSQLDialect(SQLDialect):
    """
    PostgreSQL implementation of SQL dialect.
    
    Provides PostgreSQL-specific SQL generation and data handling.
    """
    
    def get_pipeline_run_insert_sql(self) -> str:
        """Get SQL for inserting a pipeline run record."""
        return """
            INSERT INTO pipeline_runs (
                id, name, started_at, status, config_snapshot
            ) VALUES (%s, %s, %s, %s, %s)
        """
    
    def get_pipeline_run_update_sql(self) -> str:
        """Get SQL for updating a pipeline run record."""
        return """
            UPDATE pipeline_runs 
            SET completed_at = %s, status = %s, error_message = %s, updated_at = %s
            WHERE id = %s
        """
    
    def get_pipeline_run_select_sql(self) -> str:
        """Get SQL for selecting a pipeline run by ID."""
        return """
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records, 
                   skipped_records, error_message
            FROM pipeline_runs 
            WHERE id = %s
        """
    
    def get_pipeline_runs_list_sql(self) -> str:
        """Get SQL for listing recent pipeline runs."""
        return """
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records,
                   skipped_records, error_message
            FROM pipeline_runs 
            ORDER BY started_at DESC 
            LIMIT %s
        """
    
    def get_ingestion_stat_insert_sql(self) -> str:
        """Get SQL for inserting an ingestion statistic."""
        return """
            INSERT INTO ingestion_stats (
                pipeline_run_id, stage_name, file_path, record_id, record_type,
                status, error_category, error_message, error_details,
                processing_time_ms, record_size_bytes, data_source, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
    
    def get_failed_record_insert_sql(self) -> str:
        """Get SQL for inserting a failed record."""
        return """
            INSERT INTO failed_records (
                ingestion_stat_id, original_data, normalized_data,
                failure_reason, stack_trace
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """
    
    def get_audit_event_insert_sql(self) -> str:
        """Get SQL for inserting an audit event."""
        return """
            INSERT INTO audit_events (
                pipeline_run_id, event_type, stage_name, record_id,
                event_level, message, details, correlation_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
    
    def get_quality_metric_insert_sql(self) -> str:
        """Get SQL for inserting a quality metric."""
        return """
            INSERT INTO quality_metrics (
                pipeline_run_id, record_id, record_type,
                completeness_score, consistency_score, validity_score,
                accuracy_score, overall_score, missing_fields,
                invalid_fields, outlier_fields, quality_issues,
                metrics_details, sampled, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
    
    def get_performance_metric_insert_sql(self) -> str:
        """Get SQL for inserting a performance metric."""
        return """
            INSERT INTO performance_metrics (
                pipeline_run_id, stage_name, started_at, completed_at,
                duration_ms, records_processed, records_per_second,
                memory_usage_mb, cpu_usage_percent, bottleneck_indicator
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
    
    def get_ingestion_summary_sql(self, pipeline_run_id: Optional[str] = None,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> Tuple[str, List[Any]]:
        """Get SQL for ingestion summary with optional filters."""
        where_conditions = []
        params = []
        
        if pipeline_run_id:
            where_conditions.append("pipeline_run_id = %s")
            params.append(pipeline_run_id)
        
        if start_date:
            where_conditions.append("timestamp >= %s")
            params.append(start_date)
        
        if end_date:
            where_conditions.append("timestamp <= %s")
            params.append(end_date)
        
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
    
    def get_quality_summary_sql(self, pipeline_run_id: Optional[str] = None) -> Tuple[str, List[Any]]:
        """Get SQL for quality summary with optional filters."""
        where_clause = "WHERE pipeline_run_id = %s" if pipeline_run_id else ""
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
    
    def get_cleanup_sql(self, cutoff_date: datetime) -> List[Tuple[str, List[Any]]]:
        """Get SQL statements for cleaning up old data."""
        # Delete statements in dependency order
        cleanup_statements = [
            # Delete failed_records first (they reference ingestion_stats)
            ("""
                DELETE FROM failed_records 
                WHERE ingestion_stat_id IN (
                    SELECT id FROM ingestion_stats 
                    WHERE pipeline_run_id IN (
                        SELECT id FROM pipeline_runs WHERE started_at < %s
                    )
                )
            """, [cutoff_date]),
            
            # Delete other related data
            ("DELETE FROM system_metrics WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < %s)", [cutoff_date]),
            ("DELETE FROM performance_metrics WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < %s)", [cutoff_date]),
            ("DELETE FROM quality_metrics WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < %s)", [cutoff_date]),
            ("DELETE FROM audit_events WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < %s)", [cutoff_date]),
            ("DELETE FROM ingestion_stats WHERE pipeline_run_id IN (SELECT id FROM pipeline_runs WHERE started_at < %s)", [cutoff_date]),
            
            # Delete pipeline runs last
            ("DELETE FROM pipeline_runs WHERE started_at < %s", [cutoff_date])
        ]
        
        return cleanup_statements
    
    def format_datetime(self, dt: datetime) -> str:
        """Format datetime for PostgreSQL storage."""
        return dt.isoformat()
    
    def parse_datetime(self, dt_str: str) -> datetime:
        """Parse datetime from PostgreSQL storage format."""
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    
    def get_auto_increment_syntax(self) -> str:
        """Get PostgreSQL auto-increment syntax."""
        return "SERIAL PRIMARY KEY"
    
    def get_json_column_type(self) -> str:
        """Get PostgreSQL JSON column type."""
        return "JSONB"
    
    def serialize_json(self, data: Any) -> str:
        """Serialize data to JSON for PostgreSQL storage."""
        return json.dumps(data) if data is not None else None
    
    def deserialize_json(self, json_str: Optional[str]) -> Any:
        """Deserialize JSON from PostgreSQL storage."""
        if isinstance(json_str, dict):
            # Already deserialized by psycopg2
            return json_str
        return json.loads(json_str) if json_str else None
    
    def escape_identifier(self, identifier: str) -> str:
        """Escape PostgreSQL identifier."""
        return f'"{identifier}"'
    
    def get_limit_syntax(self, limit: int, offset: Optional[int] = None) -> str:
        """Get PostgreSQL LIMIT/OFFSET syntax."""
        if offset is not None:
            return f"LIMIT {limit} OFFSET {offset}"
        return f"LIMIT {limit}"
    
    def supports_feature(self, feature: str) -> bool:
        """Check if PostgreSQL supports a specific feature."""
        postgresql_features = {
            "transactions",
            "basic_sql",
            "foreign_keys",
            "json_extract",
            "full_text_search",
            "advanced_indexing",
            "connection_pooling",
            "jsonb",
            "arrays",
            "window_functions",
            "materialized_views"
        }
        return feature in postgresql_features