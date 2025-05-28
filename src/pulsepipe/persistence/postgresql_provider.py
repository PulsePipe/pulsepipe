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

# src/pulsepipe/persistence/postgresql_provider.py

"""
PostgreSQL persistence provider implementation.

Provides PostgreSQL-based persistence for healthcare data tracking and analytics
with async support, JSONB storage, and enterprise-grade features.
"""

import asyncpg
import json
import ssl
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from pulsepipe.utils.log_factory import LogFactory
from .base import (
    BasePersistenceProvider, 
    PipelineRunSummary, 
    IngestionStat, 
    QualityMetric
)
from .models import ProcessingStatus, ErrorCategory

logger = LogFactory.get_logger(__name__)


class PostgreSQLPersistenceProvider(BasePersistenceProvider):
    """
    PostgreSQL implementation of the persistence provider.
    
    Provides enterprise-grade persistence with JSONB support, transactions,
    and advanced PostgreSQL features for healthcare data compliance.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL persistence provider.
        
        Args:
            config: PostgreSQL configuration including connection details
        """
        self.config = config
        self.connection: Optional[asyncpg.Connection] = None
        self.pool: Optional[asyncpg.Pool] = None
        
        # Extract configuration
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 5432)
        self.database = config.get("database", "pulsepipe_intelligence")
        self.username = config.get("username", "pulsepipe")
        self.password = config.get("password")
        self.schema = config.get("schema", "public")
        self.ssl_mode = config.get("ssl_mode", "prefer")
        self.ssl_cert = config.get("ssl_cert")
        self.ssl_key = config.get("ssl_key")
        self.ssl_ca = config.get("ssl_ca")
        self.connection_timeout = config.get("connection_timeout", 10.0)
        self.command_timeout = config.get("command_timeout", 60.0)
        self.pool_min_size = config.get("pool_min_size", 5)
        self.pool_max_size = config.get("pool_max_size", 20)
        self.use_pool = config.get("use_pool", True)
    
    async def connect(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            # Build connection parameters
            conn_params = {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.username,
                "password": self.password,
                "timeout": self.connection_timeout,
                "command_timeout": self.command_timeout
            }
            
            # Configure SSL
            if self.ssl_mode != "disable":
                ssl_context = ssl.create_default_context()
                
                if self.ssl_mode == "require":
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                elif self.ssl_mode == "verify-ca":
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_REQUIRED
                elif self.ssl_mode == "verify-full":
                    ssl_context.check_hostname = True
                    ssl_context.verify_mode = ssl.CERT_REQUIRED
                
                # Load certificates if provided
                if self.ssl_cert and self.ssl_key:
                    ssl_context.load_cert_chain(self.ssl_cert, self.ssl_key)
                if self.ssl_ca:
                    ssl_context.load_verify_locations(self.ssl_ca)
                
                conn_params["ssl"] = ssl_context
            
            # Create connection pool or single connection
            if self.use_pool:
                self.pool = await asyncpg.create_pool(
                    min_size=self.pool_min_size,
                    max_size=self.pool_max_size,
                    **conn_params
                )
                # Test the pool
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                logger.info(f"Connected to PostgreSQL pool: {self.host}:{self.port}/{self.database}")
            else:
                self.connection = await asyncpg.connect(**conn_params)
                await self.connection.execute("SELECT 1")
                logger.info(f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}")
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close connection to PostgreSQL database."""
        try:
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()
                self.pool = None
            if self.connection:
                await self.connection.close()
                self.connection = None
            logger.info("Disconnected from PostgreSQL")
        except Exception as e:
            logger.warning(f"Error during PostgreSQL disconnect: {e}")
    
    def _get_connection(self):
        """Get a database connection from pool or direct connection."""
        if self.pool:
            return self.pool.acquire()
        elif self.connection:
            # For direct connections, create a dummy context manager
            class DirectConnectionContextManager:
                def __init__(self, connection):
                    self.connection = connection
                
                async def __aenter__(self):
                    return self.connection
                
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass
            
            return DirectConnectionContextManager(self.connection)
        else:
            raise RuntimeError("No database connection available")
    
    async def initialize_schema(self) -> None:
        """Initialize PostgreSQL database schema."""
        if not (self.pool or self.connection):
            raise RuntimeError("Database not connected")
        
        logger.info("Initializing PostgreSQL schema")
        
        async with self._get_connection() as conn:
            # Set schema search path
            await conn.execute(f"SET search_path TO {self.schema}")
            
            # Create tables
            await self._create_pipeline_runs_table(conn)
            await self._create_ingestion_stats_table(conn)
            await self._create_failed_records_table(conn)
            await self._create_audit_events_table(conn)
            await self._create_quality_metrics_table(conn)
            await self._create_performance_metrics_table(conn)
            await self._create_system_metrics_table(conn)
            
            # Create indexes
            await self._create_indexes(conn)
        
        logger.info("PostgreSQL schema initialization complete")
    
    async def _create_pipeline_runs_table(self, conn: asyncpg.Connection) -> None:
        """Create pipeline_runs table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.pipeline_runs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'running',
                total_records INTEGER DEFAULT 0,
                successful_records INTEGER DEFAULT 0,
                failed_records INTEGER DEFAULT 0,
                skipped_records INTEGER DEFAULT 0,
                config_snapshot JSONB,
                error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    async def _create_ingestion_stats_table(self, conn: asyncpg.Connection) -> None:
        """Create ingestion_stats table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.ingestion_stats (
                id BIGSERIAL PRIMARY KEY,
                pipeline_run_id TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                file_path TEXT,
                record_id TEXT,
                record_type TEXT,
                status TEXT NOT NULL,
                error_category TEXT,
                error_message TEXT,
                error_details JSONB,
                processing_time_ms INTEGER,
                record_size_bytes BIGINT,
                data_source TEXT,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES {self.schema}.pipeline_runs(id) ON DELETE CASCADE
            )
        """)
    
    async def _create_failed_records_table(self, conn: asyncpg.Connection) -> None:
        """Create failed_records table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.failed_records (
                id BIGSERIAL PRIMARY KEY,
                ingestion_stat_id BIGINT NOT NULL,
                original_data TEXT NOT NULL,
                normalized_data TEXT,
                failure_reason TEXT NOT NULL,
                stack_trace TEXT,
                retry_count INTEGER DEFAULT 0,
                last_retry_at TIMESTAMPTZ,
                resolved_at TIMESTAMPTZ,
                resolution_notes TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ingestion_stat_id) REFERENCES {self.schema}.ingestion_stats(id) ON DELETE CASCADE
            )
        """)
    
    async def _create_audit_events_table(self, conn: asyncpg.Connection) -> None:
        """Create audit_events table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.audit_events (
                id BIGSERIAL PRIMARY KEY,
                pipeline_run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                record_id TEXT,
                event_level TEXT NOT NULL DEFAULT 'INFO',
                message TEXT NOT NULL,
                details JSONB,
                user_context JSONB,
                system_context JSONB,
                correlation_id TEXT,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES {self.schema}.pipeline_runs(id) ON DELETE CASCADE
            )
        """)
    
    async def _create_quality_metrics_table(self, conn: asyncpg.Connection) -> None:
        """Create quality_metrics table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.quality_metrics (
                id BIGSERIAL PRIMARY KEY,
                pipeline_run_id TEXT NOT NULL,
                record_id TEXT,
                record_type TEXT,
                completeness_score DECIMAL(5,4),
                consistency_score DECIMAL(5,4),
                validity_score DECIMAL(5,4),
                accuracy_score DECIMAL(5,4),
                overall_score DECIMAL(5,4),
                missing_fields JSONB,
                invalid_fields JSONB,
                outlier_fields JSONB,
                quality_issues JSONB,
                metrics_details JSONB,
                sampled BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES {self.schema}.pipeline_runs(id) ON DELETE CASCADE
            )
        """)
    
    async def _create_performance_metrics_table(self, conn: asyncpg.Connection) -> None:
        """Create performance_metrics table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.performance_metrics (
                id BIGSERIAL PRIMARY KEY,
                pipeline_run_id TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL,
                duration_ms INTEGER NOT NULL,
                records_processed INTEGER DEFAULT 0,
                records_per_second DECIMAL(10,2),
                memory_usage_mb DECIMAL(10,2),
                cpu_usage_percent DECIMAL(5,2),
                disk_io_bytes BIGINT,
                network_io_bytes BIGINT,
                bottleneck_indicator TEXT,
                optimization_suggestions JSONB,
                FOREIGN KEY (pipeline_run_id) REFERENCES {self.schema}.pipeline_runs(id) ON DELETE CASCADE
            )
        """)
    
    async def _create_system_metrics_table(self, conn: asyncpg.Connection) -> None:
        """Create system_metrics table."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.system_metrics (
                id BIGSERIAL PRIMARY KEY,
                pipeline_run_id TEXT NOT NULL,
                hostname TEXT,
                os_name TEXT,
                os_version TEXT,
                python_version TEXT,
                cpu_model TEXT,
                cpu_cores INTEGER,
                cpu_threads INTEGER,
                memory_total_gb DECIMAL(10,2),
                memory_available_gb DECIMAL(10,2),
                disk_total_gb DECIMAL(10,2),
                disk_free_gb DECIMAL(10,2),
                gpu_available BOOLEAN DEFAULT FALSE,
                gpu_model TEXT,
                gpu_memory_gb DECIMAL(10,2),
                network_interfaces JSONB,
                environment_variables JSONB,
                package_versions JSONB,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pipeline_run_id) REFERENCES {self.schema}.pipeline_runs(id) ON DELETE CASCADE
            )
        """)
    
    async def _create_indexes(self, conn: asyncpg.Connection) -> None:
        """Create database indexes for performance."""
        indexes = [
            # Pipeline runs indexes
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name ON {self.schema}.pipeline_runs(name)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON {self.schema}.pipeline_runs(started_at)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON {self.schema}.pipeline_runs(status)",
            
            # Ingestion stats indexes
            f"CREATE INDEX IF NOT EXISTS idx_ingestion_stats_pipeline_run ON {self.schema}.ingestion_stats(pipeline_run_id)",
            f"CREATE INDEX IF NOT EXISTS idx_ingestion_stats_status ON {self.schema}.ingestion_stats(status)",
            f"CREATE INDEX IF NOT EXISTS idx_ingestion_stats_stage ON {self.schema}.ingestion_stats(stage_name)",
            f"CREATE INDEX IF NOT EXISTS idx_ingestion_stats_timestamp ON {self.schema}.ingestion_stats(timestamp)",
            
            # Audit events indexes
            f"CREATE INDEX IF NOT EXISTS idx_audit_events_pipeline_run ON {self.schema}.audit_events(pipeline_run_id)",
            f"CREATE INDEX IF NOT EXISTS idx_audit_events_type ON {self.schema}.audit_events(event_type)",
            f"CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON {self.schema}.audit_events(timestamp)",
            
            # Quality metrics indexes
            f"CREATE INDEX IF NOT EXISTS idx_quality_metrics_pipeline_run ON {self.schema}.quality_metrics(pipeline_run_id)",
            f"CREATE INDEX IF NOT EXISTS idx_quality_metrics_overall_score ON {self.schema}.quality_metrics(overall_score)",
            
            # Performance metrics indexes
            f"CREATE INDEX IF NOT EXISTS idx_performance_metrics_pipeline_run ON {self.schema}.performance_metrics(pipeline_run_id)",
            f"CREATE INDEX IF NOT EXISTS idx_performance_metrics_stage ON {self.schema}.performance_metrics(stage_name)",
            
            # System metrics indexes
            f"CREATE INDEX IF NOT EXISTS idx_system_metrics_pipeline_run ON {self.schema}.system_metrics(pipeline_run_id)",
            f"CREATE INDEX IF NOT EXISTS idx_system_metrics_hostname ON {self.schema}.system_metrics(hostname)",
        ]
        
        for index_sql in indexes:
            await conn.execute(index_sql)
    
    async def health_check(self) -> bool:
        """Check if PostgreSQL connection is healthy."""
        try:
            if not (self.pool or self.connection):
                return False
            
            async with self._get_connection() as conn:
                await conn.execute("SELECT 1")
            return True
            
        except Exception as e:
            logger.warning(f"PostgreSQL health check failed: {e}")
            return False
    
    # Pipeline Run Management
    
    async def start_pipeline_run(self, run_id: str, name: str, 
                               config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """Record the start of a pipeline run."""
        async with self._get_connection() as conn:
            await conn.execute(f"""
                INSERT INTO {self.schema}.pipeline_runs (
                    id, name, started_at, status, config_snapshot
                ) VALUES ($1, $2, $3, $4, $5)
            """, run_id, name, datetime.now(), "running", 
            json.dumps(config_snapshot) if config_snapshot else None)
        
        logger.debug(f"Started tracking pipeline run: {run_id}")
    
    async def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                                  error_message: Optional[str] = None) -> None:
        """Mark a pipeline run as completed."""
        async with self._get_connection() as conn:
            await conn.execute(f"""
                UPDATE {self.schema}.pipeline_runs 
                SET completed_at = $1, status = $2, error_message = $3, updated_at = $4
                WHERE id = $5
            """, datetime.now(), status, error_message, datetime.now(), run_id)
        
        logger.debug(f"Completed pipeline run: {run_id} with status: {status}")
    
    async def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                       successful: int = 0, failed: int = 0, 
                                       skipped: int = 0) -> None:
        """Update record counts for a pipeline run."""
        async with self._get_connection() as conn:
            await conn.execute(f"""
                UPDATE {self.schema}.pipeline_runs 
                SET total_records = total_records + $1,
                    successful_records = successful_records + $2,
                    failed_records = failed_records + $3,
                    skipped_records = skipped_records + $4,
                    updated_at = $5
                WHERE id = $6
            """, total, successful, failed, skipped, datetime.now(), run_id)
    
    async def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """Get pipeline run summary by ID."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                SELECT id, name, started_at, completed_at, status,
                       total_records, successful_records, failed_records, 
                       skipped_records, error_message
                FROM {self.schema}.pipeline_runs 
                WHERE id = $1
            """, run_id)
            
            if not row:
                return None
            
            return PipelineRunSummary(
                id=row['id'],
                name=row['name'],
                started_at=row['started_at'],
                completed_at=row['completed_at'],
                status=row['status'],
                total_records=row['total_records'],
                successful_records=row['successful_records'],
                failed_records=row['failed_records'],
                skipped_records=row['skipped_records'],
                error_message=row['error_message']
            )
    
    # Ingestion Statistics
    
    async def record_ingestion_stat(self, stat: IngestionStat) -> str:
        """Record an ingestion statistic."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                INSERT INTO {self.schema}.ingestion_stats (
                    pipeline_run_id, stage_name, file_path, record_id, record_type,
                    status, error_category, error_message, error_details,
                    processing_time_ms, record_size_bytes, data_source, timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING id
            """, 
            stat.pipeline_run_id, stat.stage_name, stat.file_path, stat.record_id,
            stat.record_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, 
            json.dumps(stat.error_details) if stat.error_details else None,
            stat.processing_time_ms, stat.record_size_bytes, stat.data_source, 
            stat.timestamp)
        
        return str(row['id'])
    
    async def record_failed_record(self, ingestion_stat_id: str, original_data: str,
                                 failure_reason: str, normalized_data: Optional[str] = None,
                                 stack_trace: Optional[str] = None) -> str:
        """Store a complete failed record for analysis."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                INSERT INTO {self.schema}.failed_records (
                    ingestion_stat_id, original_data, normalized_data,
                    failure_reason, stack_trace
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """, int(ingestion_stat_id), original_data, normalized_data, 
            failure_reason, stack_trace)
        
        return str(row['id'])
    
    # Quality Metrics
    
    async def record_quality_metric(self, metric: QualityMetric) -> str:
        """Record a quality metric."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                INSERT INTO {self.schema}.quality_metrics (
                    pipeline_run_id, record_id, record_type,
                    completeness_score, consistency_score, validity_score,
                    accuracy_score, overall_score, missing_fields,
                    invalid_fields, outlier_fields, quality_issues,
                    metrics_details, sampled, timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                RETURNING id
            """,
            metric.pipeline_run_id, metric.record_id, metric.record_type,
            metric.completeness_score, metric.consistency_score, metric.validity_score,
            metric.accuracy_score, metric.overall_score,
            json.dumps(metric.missing_fields) if metric.missing_fields else None,
            json.dumps(metric.invalid_fields) if metric.invalid_fields else None,
            json.dumps(metric.outlier_fields) if metric.outlier_fields else None,
            json.dumps(metric.quality_issues) if metric.quality_issues else None,
            json.dumps(metric.metrics_details) if metric.metrics_details else None,
            metric.sampled, metric.timestamp or datetime.now())
        
        return str(row['id'])
    
    # Audit Events
    
    async def record_audit_event(self, pipeline_run_id: str, event_type: str, 
                               stage_name: str, message: str, event_level: str = "INFO",
                               record_id: Optional[str] = None, 
                               details: Optional[Dict[str, Any]] = None,
                               correlation_id: Optional[str] = None) -> str:
        """Record an audit event."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                INSERT INTO {self.schema}.audit_events (
                    pipeline_run_id, event_type, stage_name, record_id,
                    event_level, message, details, correlation_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """,
            pipeline_run_id, event_type, stage_name, record_id,
            event_level, message, 
            json.dumps(details) if details else None, correlation_id)
        
        return str(row['id'])
    
    # Performance Metrics
    
    async def record_performance_metric(self, pipeline_run_id: str, stage_name: str,
                                      started_at: datetime, completed_at: datetime,
                                      records_processed: int = 0, 
                                      memory_usage_mb: Optional[float] = None,
                                      cpu_usage_percent: Optional[float] = None,
                                      bottleneck_indicator: Optional[str] = None) -> str:
        """Record performance metrics for a pipeline stage."""
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        records_per_second = records_processed / (duration_ms / 1000) if duration_ms > 0 else 0
        
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                INSERT INTO {self.schema}.performance_metrics (
                    pipeline_run_id, stage_name, started_at, completed_at,
                    duration_ms, records_processed, records_per_second,
                    memory_usage_mb, cpu_usage_percent, bottleneck_indicator
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING id
            """,
            pipeline_run_id, stage_name, started_at, completed_at,
            duration_ms, records_processed, records_per_second,
            memory_usage_mb, cpu_usage_percent, bottleneck_indicator)
        
        return str(row['id'])
    
    # System Metrics
    
    async def record_system_metric(self, pipeline_run_id: str, hostname: Optional[str] = None,
                                  os_name: Optional[str] = None, os_version: Optional[str] = None,
                                  python_version: Optional[str] = None, 
                                  cpu_model: Optional[str] = None,
                                  cpu_cores: Optional[int] = None, 
                                  memory_total_gb: Optional[float] = None,
                                  gpu_available: bool = False, 
                                  gpu_model: Optional[str] = None,
                                  additional_info: Optional[Dict[str, Any]] = None) -> str:
        """Record system metrics."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                INSERT INTO {self.schema}.system_metrics (
                    pipeline_run_id, hostname, os_name, os_version, python_version,
                    cpu_model, cpu_cores, cpu_threads, memory_total_gb, memory_available_gb,
                    disk_total_gb, disk_free_gb, gpu_available, gpu_model, gpu_memory_gb,
                    network_interfaces, environment_variables, package_versions
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                RETURNING id
            """,
            pipeline_run_id, hostname, os_name, os_version, python_version,
            cpu_model, cpu_cores,
            additional_info.get("cpu_threads") if additional_info else None,
            memory_total_gb,
            additional_info.get("memory_available_gb") if additional_info else None,
            additional_info.get("disk_total_gb") if additional_info else None,
            additional_info.get("disk_free_gb") if additional_info else None,
            gpu_available, gpu_model,
            additional_info.get("gpu_memory_gb") if additional_info else None,
            json.dumps(additional_info.get("network_interfaces")) if additional_info and additional_info.get("network_interfaces") else None,
            json.dumps(additional_info.get("environment_variables")) if additional_info and additional_info.get("environment_variables") else None,
            json.dumps(additional_info.get("package_versions")) if additional_info and additional_info.get("package_versions") else None)
        
        return str(row['id'])
    
    # Analytics and Reporting
    
    async def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get ingestion statistics summary."""
        where_conditions = []
        params = []
        param_count = 0
        
        if pipeline_run_id:
            param_count += 1
            where_conditions.append(f"pipeline_run_id = ${param_count}")
            params.append(pipeline_run_id)
        
        if start_date:
            param_count += 1
            where_conditions.append(f"timestamp >= ${param_count}")
            params.append(start_date)
        
        if end_date:
            param_count += 1
            where_conditions.append(f"timestamp <= ${param_count}")
            params.append(end_date)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self._get_connection() as conn:
            rows = await conn.fetch(f"""
                SELECT 
                    status,
                    error_category,
                    COUNT(*) as count,
                    AVG(processing_time_ms) as avg_processing_time,
                    SUM(record_size_bytes) as total_bytes
                FROM {self.schema}.ingestion_stats 
                {where_clause}
                GROUP BY status, error_category
            """, *params)
        
        summary = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "error_breakdown": {},
            "avg_processing_time_ms": 0,
            "total_bytes_processed": 0
        }
        
        total_processing_time = 0
        total_records = 0
        
        for row in rows:
            count = row['count']
            total_records += count
            summary["total_records"] += count
            
            if row['avg_processing_time']:
                total_processing_time += row['avg_processing_time'] * count
            
            if row['total_bytes']:
                summary["total_bytes_processed"] += row['total_bytes']
            
            status = row['status']
            if status == ProcessingStatus.SUCCESS.value:
                summary["successful_records"] += count
            elif status == ProcessingStatus.FAILURE.value:
                summary["failed_records"] += count
                error_category = row['error_category'] or 'unknown'
                summary["error_breakdown"][error_category] = summary["error_breakdown"].get(error_category, 0) + count
            elif status == ProcessingStatus.SKIPPED.value:
                summary["skipped_records"] += count
        
        if total_records > 0:
            summary["avg_processing_time_ms"] = total_processing_time / total_records
        
        return summary
    
    async def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get quality metrics summary."""
        where_clause = f"WHERE pipeline_run_id = $1" if pipeline_run_id else ""
        params = [pipeline_run_id] if pipeline_run_id else []
        
        async with self._get_connection() as conn:
            row = await conn.fetchrow(f"""
                SELECT 
                    COUNT(*) as total_records,
                    AVG(completeness_score) as avg_completeness,
                    AVG(consistency_score) as avg_consistency,
                    AVG(validity_score) as avg_validity,
                    AVG(accuracy_score) as avg_accuracy,
                    AVG(overall_score) as avg_overall_score,
                    MIN(overall_score) as min_score,
                    MAX(overall_score) as max_score
                FROM {self.schema}.quality_metrics 
                {where_clause}
            """, *params)
        
        return {
            "total_records": row['total_records'] or 0,
            "avg_completeness_score": float(row['avg_completeness']) if row['avg_completeness'] else None,
            "avg_consistency_score": float(row['avg_consistency']) if row['avg_consistency'] else None,
            "avg_validity_score": float(row['avg_validity']) if row['avg_validity'] else None,
            "avg_accuracy_score": float(row['avg_accuracy']) if row['avg_accuracy'] else None,
            "avg_overall_score": float(row['avg_overall_score']) if row['avg_overall_score'] else None,
            "min_overall_score": float(row['min_score']) if row['min_score'] else None,
            "max_overall_score": float(row['max_score']) if row['max_score'] else None
        }
    
    async def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """Get recent pipeline runs."""
        async with self._get_connection() as conn:
            rows = await conn.fetch(f"""
                SELECT id, name, started_at, completed_at, status,
                       total_records, successful_records, failed_records,
                       skipped_records, error_message
                FROM {self.schema}.pipeline_runs 
                ORDER BY started_at DESC 
                LIMIT $1
            """, limit)
        
        return [
            PipelineRunSummary(
                id=row['id'],
                name=row['name'],
                started_at=row['started_at'],
                completed_at=row['completed_at'],
                status=row['status'],
                total_records=row['total_records'],
                successful_records=row['successful_records'],
                failed_records=row['failed_records'],
                skipped_records=row['skipped_records'],
                error_message=row['error_message']
            )
            for row in rows
        ]
    
    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old tracking data."""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        async with self._get_connection() as conn:
            # Get pipeline runs to delete
            rows = await conn.fetch(f"""
                SELECT id FROM {self.schema}.pipeline_runs 
                WHERE started_at < $1
            """, cutoff_date)
            old_run_ids = [row['id'] for row in rows]
            
            if not old_run_ids:
                return 0
            
            # Delete pipeline runs (cascading will handle related data)
            result = await conn.execute(f"""
                DELETE FROM {self.schema}.pipeline_runs 
                WHERE started_at < $1
            """, cutoff_date)
            
            # Extract the number from "DELETE n" response
            try:
                deleted_count = int(result.split()[-1]) if result and result.split() else 0
            except (ValueError, IndexError):
                deleted_count = 0
            
            logger.info(f"Cleaned up {deleted_count} old pipeline runs and related data older than {days_to_keep} days")
            
            return deleted_count
    
    # Async context manager support
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()