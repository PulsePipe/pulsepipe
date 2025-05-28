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

# src/pulsepipe/persistence/sqlite_provider.py

"""
SQLite persistence provider implementation.

Provides SQLite-based persistence for healthcare data tracking and analytics
with async support and proper schema management.
"""

import aiosqlite
import json
from datetime import datetime, timedelta
from pathlib import Path
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


class SQLitePersistenceProvider(BasePersistenceProvider):
    """
    SQLite implementation of the persistence provider.
    
    Provides lightweight, file-based persistence for healthcare data tracking
    with async support and proper transaction management.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLite persistence provider.
        
        Args:
            config: SQLite configuration including database path
        """
        self.config = config
        self.connection: Optional[aiosqlite.Connection] = None
        
        # Extract configuration
        self.db_path = config.get("db_path", ".pulsepipe/state/ingestion.sqlite3")
        self.timeout = config.get("timeout", 30.0)
        self.enable_wal = config.get("enable_wal", True)
        self.enable_foreign_keys = config.get("enable_foreign_keys", True)
        self.cache_size = config.get("cache_size", -64000)  # 64MB cache
        
        # Ensure parent directory exists
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
    
    async def connect(self) -> None:
        """Establish connection to SQLite database."""
        try:
            self.connection = await aiosqlite.connect(
                self.db_path,
                timeout=self.timeout
            )
            
            # Configure SQLite settings
            if self.enable_foreign_keys:
                await self.connection.execute("PRAGMA foreign_keys = ON")
            
            if self.enable_wal:
                await self.connection.execute("PRAGMA journal_mode = WAL")
            
            await self.connection.execute(f"PRAGMA cache_size = {self.cache_size}")
            await self.connection.execute("PRAGMA synchronous = NORMAL")
            await self.connection.execute("PRAGMA temp_store = MEMORY")
            await self.connection.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
            
            await self.connection.commit()
            
            logger.info(f"Connected to SQLite database: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close connection to SQLite database."""
        if self.connection:
            await self.connection.close()
            self.connection = None
            logger.info("Disconnected from SQLite database")
    
    async def initialize_schema(self) -> None:
        """Initialize SQLite database schema."""
        if not self.connection:
            raise RuntimeError("Database not connected")
        
        logger.info("Initializing SQLite schema")
        
        # Create all tables
        await self._create_pipeline_runs_table()
        await self._create_ingestion_stats_table()
        await self._create_failed_records_table()
        await self._create_audit_events_table()
        await self._create_quality_metrics_table()
        await self._create_performance_metrics_table()
        await self._create_system_metrics_table()
        
        await self.connection.commit()
        logger.info("SQLite schema initialization complete")
    
    async def health_check(self) -> bool:
        """Check if SQLite connection is healthy."""
        try:
            if not self.connection:
                return False
            
            # Test the connection with a simple query
            await self.connection.execute("SELECT 1")
            return True
            
        except Exception as e:
            logger.warning(f"SQLite health check failed: {e}")
            return False
    
    async def _create_pipeline_runs_table(self) -> None:
        """Create pipeline_runs table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name ON pipeline_runs(name)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status)")
    
    async def _create_ingestion_stats_table(self) -> None:
        """Create ingestion_stats table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_pipeline_run ON ingestion_stats(pipeline_run_id)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_status ON ingestion_stats(status)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_stage ON ingestion_stats(stage_name)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_error_category ON ingestion_stats(error_category)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_stats_timestamp ON ingestion_stats(timestamp)")
    
    async def _create_failed_records_table(self) -> None:
        """Create failed_records table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_failed_records_ingestion_stat ON failed_records(ingestion_stat_id)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_failed_records_retry_count ON failed_records(retry_count)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_failed_records_resolved ON failed_records(resolved_at)")
    
    async def _create_audit_events_table(self) -> None:
        """Create audit_events table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_pipeline_run ON audit_events(pipeline_run_id)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_type ON audit_events(event_type)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_level ON audit_events(event_level)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON audit_events(timestamp)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_correlation ON audit_events(correlation_id)")
    
    async def _create_quality_metrics_table(self) -> None:
        """Create quality_metrics table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_pipeline_run ON quality_metrics(pipeline_run_id)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_record_type ON quality_metrics(record_type)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_overall_score ON quality_metrics(overall_score)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_quality_metrics_sampled ON quality_metrics(sampled)")
    
    async def _create_performance_metrics_table(self) -> None:
        """Create performance_metrics table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_pipeline_run ON performance_metrics(pipeline_run_id)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_stage ON performance_metrics(stage_name)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_duration ON performance_metrics(duration_ms)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_rps ON performance_metrics(records_per_second)")
    
    async def _create_system_metrics_table(self) -> None:
        """Create system_metrics table."""
        await self.connection.execute("""
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
        
        # Create indexes
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_pipeline_run ON system_metrics(pipeline_run_id)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_hostname ON system_metrics(hostname)")
        await self.connection.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_os ON system_metrics(os_name)")
    
    # Pipeline Run Management
    
    async def start_pipeline_run(self, run_id: str, name: str, 
                               config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """Record the start of a pipeline run."""
        config_json = json.dumps(config_snapshot) if config_snapshot else None
        
        await self.connection.execute("""
            INSERT INTO pipeline_runs (
                id, name, started_at, status, config_snapshot
            ) VALUES (?, ?, ?, ?, ?)
        """, (run_id, name, datetime.now().isoformat(), "running", config_json))
        
        await self.connection.commit()
        logger.debug(f"Started tracking pipeline run: {run_id}")
    
    async def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                                  error_message: Optional[str] = None) -> None:
        """Mark a pipeline run as completed."""
        await self.connection.execute("""
            UPDATE pipeline_runs 
            SET completed_at = ?, status = ?, error_message = ?, updated_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), status, error_message, datetime.now().isoformat(), run_id))
        
        await self.connection.commit()
        logger.debug(f"Completed pipeline run: {run_id} with status: {status}")
    
    async def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                       successful: int = 0, failed: int = 0, 
                                       skipped: int = 0) -> None:
        """Update record counts for a pipeline run."""
        await self.connection.execute("""
            UPDATE pipeline_runs 
            SET total_records = total_records + ?,
                successful_records = successful_records + ?,
                failed_records = failed_records + ?,
                skipped_records = skipped_records + ?,
                updated_at = ?
            WHERE id = ?
        """, (total, successful, failed, skipped, datetime.now().isoformat(), run_id))
        
        await self.connection.commit()
    
    async def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """Get pipeline run summary by ID."""
        async with self.connection.execute("""
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records, 
                   skipped_records, error_message
            FROM pipeline_runs 
            WHERE id = ?
        """, (run_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            
            return PipelineRunSummary(
                id=row[0],
                name=row[1],
                started_at=datetime.fromisoformat(row[2]),
                completed_at=datetime.fromisoformat(row[3]) if row[3] else None,
                status=row[4],
                total_records=row[5],
                successful_records=row[6],
                failed_records=row[7],
                skipped_records=row[8],
                error_message=row[9]
            )
    
    # Ingestion Statistics
    
    async def record_ingestion_stat(self, stat: IngestionStat) -> str:
        """Record an ingestion statistic."""
        error_details_json = json.dumps(stat.error_details) if stat.error_details else None
        
        async with self.connection.execute("""
            INSERT INTO ingestion_stats (
                pipeline_run_id, stage_name, file_path, record_id, record_type,
                status, error_category, error_message, error_details,
                processing_time_ms, record_size_bytes, data_source, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stat.pipeline_run_id, stat.stage_name, stat.file_path, stat.record_id,
            stat.record_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_json, stat.processing_time_ms,
            stat.record_size_bytes, stat.data_source, stat.timestamp.isoformat()
        )) as cursor:
            await self.connection.commit()
            return str(cursor.lastrowid)
    
    async def record_failed_record(self, ingestion_stat_id: str, original_data: str,
                                 failure_reason: str, normalized_data: Optional[str] = None,
                                 stack_trace: Optional[str] = None) -> str:
        """Store a complete failed record for analysis."""
        async with self.connection.execute("""
            INSERT INTO failed_records (
                ingestion_stat_id, original_data, normalized_data,
                failure_reason, stack_trace
            ) VALUES (?, ?, ?, ?, ?)
        """, (int(ingestion_stat_id), original_data, normalized_data, failure_reason, stack_trace)) as cursor:
            await self.connection.commit()
            return str(cursor.lastrowid)
    
    # Quality Metrics
    
    async def record_quality_metric(self, metric: QualityMetric) -> str:
        """Record a quality metric."""
        missing_fields_json = json.dumps(metric.missing_fields) if metric.missing_fields else None
        invalid_fields_json = json.dumps(metric.invalid_fields) if metric.invalid_fields else None
        outlier_fields_json = json.dumps(metric.outlier_fields) if metric.outlier_fields else None
        quality_issues_json = json.dumps(metric.quality_issues) if metric.quality_issues else None
        metrics_details_json = json.dumps(metric.metrics_details) if metric.metrics_details else None
        
        async with self.connection.execute("""
            INSERT INTO quality_metrics (
                pipeline_run_id, record_id, record_type,
                completeness_score, consistency_score, validity_score,
                accuracy_score, overall_score, missing_fields,
                invalid_fields, outlier_fields, quality_issues,
                metrics_details, sampled, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metric.pipeline_run_id, metric.record_id, metric.record_type,
            metric.completeness_score, metric.consistency_score, metric.validity_score,
            metric.accuracy_score, metric.overall_score, missing_fields_json,
            invalid_fields_json, outlier_fields_json, quality_issues_json,
            metrics_details_json, metric.sampled, 
            (metric.timestamp or datetime.now()).isoformat()
        )) as cursor:
            await self.connection.commit()
            return str(cursor.lastrowid)
    
    # Audit Events
    
    async def record_audit_event(self, pipeline_run_id: str, event_type: str, 
                               stage_name: str, message: str, event_level: str = "INFO",
                               record_id: Optional[str] = None, 
                               details: Optional[Dict[str, Any]] = None,
                               correlation_id: Optional[str] = None) -> str:
        """Record an audit event."""
        details_json = json.dumps(details) if details else None
        
        async with self.connection.execute("""
            INSERT INTO audit_events (
                pipeline_run_id, event_type, stage_name, record_id,
                event_level, message, details, correlation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline_run_id, event_type, stage_name, record_id,
            event_level, message, details_json, correlation_id
        )) as cursor:
            await self.connection.commit()
            return str(cursor.lastrowid)
    
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
        
        async with self.connection.execute("""
            INSERT INTO performance_metrics (
                pipeline_run_id, stage_name, started_at, completed_at,
                duration_ms, records_processed, records_per_second,
                memory_usage_mb, cpu_usage_percent, bottleneck_indicator
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline_run_id, stage_name, started_at.isoformat(), completed_at.isoformat(),
            duration_ms, records_processed, records_per_second,
            memory_usage_mb, cpu_usage_percent, bottleneck_indicator
        )) as cursor:
            await self.connection.commit()
            return str(cursor.lastrowid)
    
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
        network_interfaces_json = json.dumps(additional_info.get("network_interfaces")) if additional_info and additional_info.get("network_interfaces") else None
        environment_variables_json = json.dumps(additional_info.get("environment_variables")) if additional_info and additional_info.get("environment_variables") else None
        package_versions_json = json.dumps(additional_info.get("package_versions")) if additional_info and additional_info.get("package_versions") else None
        
        async with self.connection.execute("""
            INSERT INTO system_metrics (
                pipeline_run_id, hostname, os_name, os_version, python_version,
                cpu_model, cpu_cores, cpu_threads, memory_total_gb, memory_available_gb,
                disk_total_gb, disk_free_gb, gpu_available, gpu_model, gpu_memory_gb,
                network_interfaces, environment_variables, package_versions
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline_run_id, hostname, os_name, os_version, python_version,
            cpu_model, cpu_cores, 
            additional_info.get("cpu_threads") if additional_info else None,
            memory_total_gb,
            additional_info.get("memory_available_gb") if additional_info else None,
            additional_info.get("disk_total_gb") if additional_info else None,
            additional_info.get("disk_free_gb") if additional_info else None,
            gpu_available, gpu_model,
            additional_info.get("gpu_memory_gb") if additional_info else None,
            network_interfaces_json, environment_variables_json, package_versions_json
        )) as cursor:
            await self.connection.commit()
            return str(cursor.lastrowid)
    
    # Analytics and Reporting
    
    async def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get ingestion statistics summary."""
        where_conditions = []
        params = []
        
        if pipeline_run_id:
            where_conditions.append("pipeline_run_id = ?")
            params.append(pipeline_run_id)
        
        if start_date:
            where_conditions.append("timestamp >= ?")
            params.append(start_date.isoformat())
        
        if end_date:
            where_conditions.append("timestamp <= ?")
            params.append(end_date.isoformat())
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.connection.execute(f"""
            SELECT 
                status,
                error_category,
                COUNT(*) as count,
                AVG(processing_time_ms) as avg_processing_time,
                SUM(record_size_bytes) as total_bytes
            FROM ingestion_stats 
            {where_clause}
            GROUP BY status, error_category
        """, params) as cursor:
            results = await cursor.fetchall()
        
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
        
        for row in results:
            count = row[2]
            total_records += count
            summary["total_records"] += count
            
            if row[3]:
                total_processing_time += row[3] * count
            
            if row[4]:
                summary["total_bytes_processed"] += row[4]
            
            status = row[0]
            if status == ProcessingStatus.SUCCESS.value:
                summary["successful_records"] += count
            elif status == ProcessingStatus.FAILURE.value:
                summary["failed_records"] += count
                error_category = row[1] or 'unknown'
                summary["error_breakdown"][error_category] = summary["error_breakdown"].get(error_category, 0) + count
            elif status == ProcessingStatus.SKIPPED.value:
                summary["skipped_records"] += count
        
        if total_records > 0:
            summary["avg_processing_time_ms"] = total_processing_time / total_records
        
        return summary
    
    async def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get quality metrics summary."""
        where_clause = "WHERE pipeline_run_id = ?" if pipeline_run_id else ""
        params = [pipeline_run_id] if pipeline_run_id else []
        
        async with self.connection.execute(f"""
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
        """, params) as cursor:
            row = await cursor.fetchone()
        
        return {
            "total_records": row[0] or 0,
            "avg_completeness_score": row[1],
            "avg_consistency_score": row[2], 
            "avg_validity_score": row[3],
            "avg_accuracy_score": row[4],
            "avg_overall_score": row[5],
            "min_overall_score": row[6],
            "max_overall_score": row[7]
        }
    
    async def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """Get recent pipeline runs."""
        async with self.connection.execute("""
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records,
                   skipped_records, error_message
            FROM pipeline_runs 
            ORDER BY started_at DESC 
            LIMIT ?
        """, (limit,)) as cursor:
            results = await cursor.fetchall()
        
        return [
            PipelineRunSummary(
                id=row[0],
                name=row[1],
                started_at=datetime.fromisoformat(row[2]),
                completed_at=datetime.fromisoformat(row[3]) if row[3] else None,
                status=row[4],
                total_records=row[5],
                successful_records=row[6],
                failed_records=row[7],
                skipped_records=row[8],
                error_message=row[9]
            )
            for row in results
        ]
    
    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old tracking data."""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # Get pipeline runs to delete
        async with self.connection.execute(
            "SELECT id FROM pipeline_runs WHERE started_at < ?",
            (cutoff_date.isoformat(),)
        ) as cursor:
            old_run_ids = [row[0] for row in await cursor.fetchall()]
        
        if not old_run_ids:
            return 0
        
        total_deleted = 0
        
        # Delete failed_records first (they reference ingestion_stats)
        if old_run_ids:
            placeholders = ','.join('?' * len(old_run_ids))
            async with self.connection.execute(f"""
                DELETE FROM failed_records 
                WHERE ingestion_stat_id IN (
                    SELECT id FROM ingestion_stats 
                    WHERE pipeline_run_id IN ({placeholders})
                )
            """, old_run_ids) as cursor:
                total_deleted += cursor.rowcount
        
        # Delete other related data
        for table in ["system_metrics", "performance_metrics", "quality_metrics", 
                     "audit_events", "ingestion_stats"]:
            placeholders = ','.join('?' * len(old_run_ids))
            async with self.connection.execute(
                f"DELETE FROM {table} WHERE pipeline_run_id IN ({placeholders})",
                old_run_ids
            ) as cursor:
                total_deleted += cursor.rowcount
        
        # Delete pipeline runs
        placeholders = ','.join('?' * len(old_run_ids))
        async with self.connection.execute(
            f"DELETE FROM pipeline_runs WHERE id IN ({placeholders})",
            old_run_ids
        ) as cursor:
            total_deleted += cursor.rowcount
        
        await self.connection.commit()
        logger.info(f"Cleaned up {total_deleted} old tracking records older than {days_to_keep} days")
        
        return total_deleted