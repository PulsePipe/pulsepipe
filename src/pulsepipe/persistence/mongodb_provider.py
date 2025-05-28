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

# src/pulsepipe/persistence/mongodb_provider.py

"""
MongoDB persistence provider implementation.

Provides MongoDB-based persistence for healthcare data tracking and analytics
with secure connection support and HIPAA-compliant features.
"""

import ssl
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from bson import ObjectId
import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from pulsepipe.utils.log_factory import LogFactory
from .base import (
    BasePersistenceProvider, 
    PipelineRunSummary, 
    IngestionStat, 
    QualityMetric
)
from .models import ProcessingStatus, ErrorCategory

logger = LogFactory.get_logger(__name__)


class MongoDBPersistenceProvider(BasePersistenceProvider):
    """
    MongoDB implementation of the persistence provider.
    
    Provides secure, scalable persistence for healthcare data tracking
    with support for replica sets, authentication, and TLS encryption.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MongoDB persistence provider.
        
        Args:
            config: MongoDB configuration including connection details
        """
        self.config = config
        self.client: Optional[MongoClient] = None
        self.database: Optional[Database] = None
        self.collections: Dict[str, Collection] = {}
        
        # Extract configuration
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 27017)
        self.database_name = config.get("database", "pulsepipe_intelligence")
        self.username = config.get("username")
        self.password = config.get("password")
        self.auth_source = config.get("auth_source", "admin")
        self.replica_set = config.get("replica_set")
        self.tls_enabled = config.get("tls", True)
        self.tls_ca_file = config.get("tls_ca_file")
        self.tls_cert_file = config.get("tls_cert_file")
        self.tls_key_file = config.get("tls_key_file")
        self.connection_timeout_ms = config.get("connection_timeout_ms", 5000)
        self.server_selection_timeout_ms = config.get("server_selection_timeout_ms", 5000)
        
        # Collection names
        self.collection_names = {
            "pipeline_runs": "pipeline_runs",
            "ingestion_stats": "ingestion_stats", 
            "failed_records": "failed_records",
            "audit_events": "audit_events",
            "quality_metrics": "quality_metrics",
            "performance_metrics": "performance_metrics",
            "system_metrics": "system_metrics"
        }
    
    async def connect(self) -> None:
        """Establish secure connection to MongoDB."""
        try:
            # Build connection URI
            uri_parts = []
            
            if self.username and self.password:
                uri_parts.append(f"mongodb://{self.username}:{self.password}@")
            else:
                uri_parts.append("mongodb://")
            
            uri_parts.append(f"{self.host}:{self.port}")
            uri_parts.append(f"/{self.database_name}")
            
            # Add query parameters
            query_params = []
            if self.auth_source:
                query_params.append(f"authSource={self.auth_source}")
            if self.replica_set:
                query_params.append(f"replicaSet={self.replica_set}")
            
            if query_params:
                uri_parts.append("?" + "&".join(query_params))
            
            connection_uri = "".join(uri_parts)
            
            # Configure TLS options
            client_options = {
                "connectTimeoutMS": self.connection_timeout_ms,
                "serverSelectionTimeoutMS": self.server_selection_timeout_ms,
                "maxPoolSize": 100,
                "retryWrites": True,
                "retryReads": True
            }
            
            if self.tls_enabled:
                client_options["tls"] = True
                client_options["tlsAllowInvalidCertificates"] = False
                client_options["tlsAllowInvalidHostnames"] = False
                
                if self.tls_ca_file:
                    client_options["tlsCAFile"] = self.tls_ca_file
                if self.tls_cert_file:
                    client_options["tlsCertificateKeyFile"] = self.tls_cert_file
                elif self.tls_key_file:
                    client_options["tlsCertificateKeyFile"] = self.tls_key_file
            
            # Create client with secure defaults
            self.client = MongoClient(connection_uri, **client_options)
            
            # Test connection
            await self._test_connection()
            
            # Get database and collections
            self.database = self.client[self.database_name]
            
            # Initialize collection references
            for collection_key, collection_name in self.collection_names.items():
                self.collections[collection_key] = self.database[collection_name]
            
            logger.info(f"Connected to MongoDB: {self.host}:{self.port}/{self.database_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close connection to MongoDB."""
        if self.client:
            self.client.close()
            self.client = None
            self.database = None
            self.collections.clear()
            logger.info("Disconnected from MongoDB")
    
    async def _test_connection(self) -> None:
        """Test MongoDB connection with timeout."""
        if not self.client:
            raise ConnectionError("MongoDB client not initialized")
        
        try:
            # Test connection with admin command
            self.client.admin.command('ismaster')
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise ConnectionError(f"MongoDB connection test failed: {e}")
    
    async def initialize_schema(self) -> None:
        """Initialize MongoDB collections and indexes."""
        if not self.database:
            raise RuntimeError("Database not connected")
        
        logger.info("Initializing MongoDB schema and indexes")
        
        # Create indexes for each collection
        await self._create_pipeline_runs_indexes()
        await self._create_ingestion_stats_indexes()
        await self._create_failed_records_indexes()
        await self._create_audit_events_indexes()
        await self._create_quality_metrics_indexes()
        await self._create_performance_metrics_indexes()
        await self._create_system_metrics_indexes()
        
        logger.info("MongoDB schema initialization complete")
    
    async def _create_pipeline_runs_indexes(self) -> None:
        """Create indexes for pipeline_runs collection."""
        collection = self.collections["pipeline_runs"]
        
        # Create indexes
        collection.create_index("name")
        collection.create_index("started_at")
        collection.create_index("status")
        collection.create_index([("name", 1), ("started_at", -1)])
    
    async def _create_ingestion_stats_indexes(self) -> None:
        """Create indexes for ingestion_stats collection."""
        collection = self.collections["ingestion_stats"]
        
        collection.create_index("pipeline_run_id")
        collection.create_index("status")
        collection.create_index("stage_name")
        collection.create_index("error_category")
        collection.create_index("timestamp")
        collection.create_index([("pipeline_run_id", 1), ("status", 1)])
        collection.create_index([("stage_name", 1), ("timestamp", -1)])
    
    async def _create_failed_records_indexes(self) -> None:
        """Create indexes for failed_records collection."""
        collection = self.collections["failed_records"]
        
        collection.create_index("ingestion_stat_id")
        collection.create_index("retry_count")
        collection.create_index("resolved_at")
        collection.create_index("created_at")
    
    async def _create_audit_events_indexes(self) -> None:
        """Create indexes for audit_events collection."""
        collection = self.collections["audit_events"]
        
        collection.create_index("pipeline_run_id")
        collection.create_index("event_type")
        collection.create_index("event_level")
        collection.create_index("timestamp")
        collection.create_index("correlation_id")
        collection.create_index([("pipeline_run_id", 1), ("timestamp", -1)])
    
    async def _create_quality_metrics_indexes(self) -> None:
        """Create indexes for quality_metrics collection."""
        collection = self.collections["quality_metrics"]
        
        collection.create_index("pipeline_run_id")
        collection.create_index("record_type")
        collection.create_index("overall_score")
        collection.create_index("sampled")
        collection.create_index("timestamp")
        collection.create_index([("pipeline_run_id", 1), ("overall_score", -1)])
    
    async def _create_performance_metrics_indexes(self) -> None:
        """Create indexes for performance_metrics collection."""
        collection = self.collections["performance_metrics"]
        
        collection.create_index("pipeline_run_id")
        collection.create_index("stage_name")
        collection.create_index("duration_ms")
        collection.create_index("records_per_second")
        collection.create_index("started_at")
        collection.create_index([("pipeline_run_id", 1), ("stage_name", 1)])
    
    async def _create_system_metrics_indexes(self) -> None:
        """Create indexes for system_metrics collection."""
        collection = self.collections["system_metrics"]
        
        collection.create_index("pipeline_run_id")
        collection.create_index("hostname")
        collection.create_index("os_name")
        collection.create_index("timestamp")
    
    async def health_check(self) -> bool:
        """Check if MongoDB connection is healthy."""
        try:
            if not self.client:
                return False
            
            # Ping the database
            self.client.admin.command('ping')
            return True
            
        except Exception as e:
            logger.warning(f"MongoDB health check failed: {e}")
            return False
    
    # Pipeline Run Management
    
    async def start_pipeline_run(self, run_id: str, name: str, 
                               config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """Record the start of a pipeline run."""
        collection = self.collections["pipeline_runs"]
        
        document = {
            "_id": run_id,
            "name": name,
            "started_at": datetime.now(),
            "completed_at": None,
            "status": "running",
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "config_snapshot": config_snapshot,
            "error_message": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        collection.insert_one(document)
        logger.debug(f"Started tracking pipeline run: {run_id}")
    
    async def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                                  error_message: Optional[str] = None) -> None:
        """Mark a pipeline run as completed."""
        collection = self.collections["pipeline_runs"]
        
        update_doc = {
            "$set": {
                "completed_at": datetime.now(),
                "status": status,
                "error_message": error_message,
                "updated_at": datetime.now()
            }
        }
        
        collection.update_one({"_id": run_id}, update_doc)
        logger.debug(f"Completed pipeline run: {run_id} with status: {status}")
    
    async def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                       successful: int = 0, failed: int = 0, 
                                       skipped: int = 0) -> None:
        """Update record counts for a pipeline run."""
        collection = self.collections["pipeline_runs"]
        
        update_doc = {
            "$inc": {
                "total_records": total,
                "successful_records": successful,
                "failed_records": failed,
                "skipped_records": skipped
            },
            "$set": {
                "updated_at": datetime.now()
            }
        }
        
        collection.update_one({"_id": run_id}, update_doc)
    
    async def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """Get pipeline run summary by ID."""
        collection = self.collections["pipeline_runs"]
        
        document = collection.find_one({"_id": run_id})
        if not document:
            return None
        
        return PipelineRunSummary(
            id=document["_id"],
            name=document["name"],
            started_at=document["started_at"],
            completed_at=document.get("completed_at"),
            status=document["status"],
            total_records=document.get("total_records", 0),
            successful_records=document.get("successful_records", 0),
            failed_records=document.get("failed_records", 0),
            skipped_records=document.get("skipped_records", 0),
            error_message=document.get("error_message")
        )
    
    # Ingestion Statistics
    
    async def record_ingestion_stat(self, stat: IngestionStat) -> str:
        """Record an ingestion statistic."""
        collection = self.collections["ingestion_stats"]
        
        document = {
            "pipeline_run_id": stat.pipeline_run_id,
            "stage_name": stat.stage_name,
            "file_path": stat.file_path,
            "record_id": stat.record_id,
            "record_type": stat.record_type,
            "status": stat.status.value if stat.status else None,
            "error_category": stat.error_category.value if stat.error_category else None,
            "error_message": stat.error_message,
            "error_details": stat.error_details,
            "processing_time_ms": stat.processing_time_ms,
            "record_size_bytes": stat.record_size_bytes,
            "data_source": stat.data_source,
            "timestamp": stat.timestamp
        }
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
    async def record_failed_record(self, ingestion_stat_id: str, original_data: str,
                                 failure_reason: str, normalized_data: Optional[str] = None,
                                 stack_trace: Optional[str] = None) -> str:
        """Store a complete failed record for analysis."""
        collection = self.collections["failed_records"]
        
        document = {
            "ingestion_stat_id": ObjectId(ingestion_stat_id),
            "original_data": original_data,
            "normalized_data": normalized_data,
            "failure_reason": failure_reason,
            "stack_trace": stack_trace,
            "retry_count": 0,
            "last_retry_at": None,
            "resolved_at": None,
            "resolution_notes": None,
            "created_at": datetime.now()
        }
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
    # Quality Metrics
    
    async def record_quality_metric(self, metric: QualityMetric) -> str:
        """Record a quality metric."""
        collection = self.collections["quality_metrics"]
        
        document = {
            "pipeline_run_id": metric.pipeline_run_id,
            "record_id": metric.record_id,
            "record_type": metric.record_type,
            "completeness_score": metric.completeness_score,
            "consistency_score": metric.consistency_score,
            "validity_score": metric.validity_score,
            "accuracy_score": metric.accuracy_score,
            "overall_score": metric.overall_score,
            "missing_fields": metric.missing_fields,
            "invalid_fields": metric.invalid_fields,
            "outlier_fields": metric.outlier_fields,
            "quality_issues": metric.quality_issues,
            "metrics_details": metric.metrics_details,
            "sampled": metric.sampled,
            "timestamp": metric.timestamp or datetime.now()
        }
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
    # Audit Events
    
    async def record_audit_event(self, pipeline_run_id: str, event_type: str, 
                               stage_name: str, message: str, event_level: str = "INFO",
                               record_id: Optional[str] = None, 
                               details: Optional[Dict[str, Any]] = None,
                               correlation_id: Optional[str] = None) -> str:
        """Record an audit event."""
        collection = self.collections["audit_events"]
        
        document = {
            "pipeline_run_id": pipeline_run_id,
            "event_type": event_type,
            "stage_name": stage_name,
            "record_id": record_id,
            "event_level": event_level,
            "message": message,
            "details": details,
            "user_context": None,  # Could be populated from context
            "system_context": None,  # Could be populated from context
            "correlation_id": correlation_id,
            "timestamp": datetime.now()
        }
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
    # Performance Metrics
    
    async def record_performance_metric(self, pipeline_run_id: str, stage_name: str,
                                      started_at: datetime, completed_at: datetime,
                                      records_processed: int = 0, 
                                      memory_usage_mb: Optional[float] = None,
                                      cpu_usage_percent: Optional[float] = None,
                                      bottleneck_indicator: Optional[str] = None) -> str:
        """Record performance metrics for a pipeline stage."""
        collection = self.collections["performance_metrics"]
        
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        records_per_second = records_processed / (duration_ms / 1000) if duration_ms > 0 else 0
        
        document = {
            "pipeline_run_id": pipeline_run_id,
            "stage_name": stage_name,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": duration_ms,
            "records_processed": records_processed,
            "records_per_second": records_per_second,
            "memory_usage_mb": memory_usage_mb,
            "cpu_usage_percent": cpu_usage_percent,
            "disk_io_bytes": None,  # Could be populated from system metrics
            "network_io_bytes": None,  # Could be populated from system metrics
            "bottleneck_indicator": bottleneck_indicator,
            "optimization_suggestions": None  # Could be generated by analysis
        }
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
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
        collection = self.collections["system_metrics"]
        
        document = {
            "pipeline_run_id": pipeline_run_id,
            "hostname": hostname,
            "os_name": os_name,
            "os_version": os_version,
            "python_version": python_version,
            "cpu_model": cpu_model,
            "cpu_cores": cpu_cores,
            "cpu_threads": additional_info.get("cpu_threads") if additional_info else None,
            "memory_total_gb": memory_total_gb,
            "memory_available_gb": additional_info.get("memory_available_gb") if additional_info else None,
            "disk_total_gb": additional_info.get("disk_total_gb") if additional_info else None,
            "disk_free_gb": additional_info.get("disk_free_gb") if additional_info else None,
            "gpu_available": gpu_available,
            "gpu_model": gpu_model,
            "gpu_memory_gb": additional_info.get("gpu_memory_gb") if additional_info else None,
            "network_interfaces": additional_info.get("network_interfaces") if additional_info else None,
            "environment_variables": additional_info.get("environment_variables") if additional_info else None,
            "package_versions": additional_info.get("package_versions") if additional_info else None,
            "timestamp": datetime.now()
        }
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
    # Analytics and Reporting
    
    async def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get ingestion statistics summary."""
        collection = self.collections["ingestion_stats"]
        
        # Build match criteria
        match_criteria = {}
        if pipeline_run_id:
            match_criteria["pipeline_run_id"] = pipeline_run_id
        if start_date or end_date:
            timestamp_criteria = {}
            if start_date:
                timestamp_criteria["$gte"] = start_date
            if end_date:
                timestamp_criteria["$lte"] = end_date
            match_criteria["timestamp"] = timestamp_criteria
        
        # Aggregation pipeline
        pipeline = []
        if match_criteria:
            pipeline.append({"$match": match_criteria})
        
        pipeline.extend([
            {
                "$group": {
                    "_id": {
                        "status": "$status",
                        "error_category": "$error_category"
                    },
                    "count": {"$sum": 1},
                    "avg_processing_time": {"$avg": "$processing_time_ms"},
                    "total_bytes": {"$sum": "$record_size_bytes"}
                }
            }
        ])
        
        results = list(collection.aggregate(pipeline))
        
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
        
        for result in results:
            count = result["count"]
            total_records += count
            summary["total_records"] += count
            
            if result["avg_processing_time"]:
                total_processing_time += result["avg_processing_time"] * count
            
            if result["total_bytes"]:
                summary["total_bytes_processed"] += result["total_bytes"]
            
            status = result["_id"]["status"]
            if status == ProcessingStatus.SUCCESS.value:
                summary["successful_records"] += count
            elif status == ProcessingStatus.FAILURE.value:
                summary["failed_records"] += count
                error_category = result["_id"]["error_category"] or "unknown"
                summary["error_breakdown"][error_category] = summary["error_breakdown"].get(error_category, 0) + count
            elif status == ProcessingStatus.SKIPPED.value:
                summary["skipped_records"] += count
        
        if total_records > 0:
            summary["avg_processing_time_ms"] = total_processing_time / total_records
        
        return summary
    
    async def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get quality metrics summary."""
        collection = self.collections["quality_metrics"]
        
        match_criteria = {}
        if pipeline_run_id:
            match_criteria["pipeline_run_id"] = pipeline_run_id
        
        pipeline = []
        if match_criteria:
            pipeline.append({"$match": match_criteria})
        
        pipeline.append({
            "$group": {
                "_id": None,
                "total_records": {"$sum": 1},
                "avg_completeness": {"$avg": "$completeness_score"},
                "avg_consistency": {"$avg": "$consistency_score"},
                "avg_validity": {"$avg": "$validity_score"},
                "avg_accuracy": {"$avg": "$accuracy_score"},
                "avg_overall_score": {"$avg": "$overall_score"},
                "min_score": {"$min": "$overall_score"},
                "max_score": {"$max": "$overall_score"}
            }
        })
        
        results = list(collection.aggregate(pipeline))
        
        if not results:
            return {
                "total_records": 0,
                "avg_completeness_score": None,
                "avg_consistency_score": None,
                "avg_validity_score": None,
                "avg_accuracy_score": None,
                "avg_overall_score": None,
                "min_overall_score": None,
                "max_overall_score": None
            }
        
        result = results[0]
        
        return {
            "total_records": result["total_records"],
            "avg_completeness_score": result["avg_completeness"],
            "avg_consistency_score": result["avg_consistency"],
            "avg_validity_score": result["avg_validity"],
            "avg_accuracy_score": result["avg_accuracy"],
            "avg_overall_score": result["avg_overall_score"],
            "min_overall_score": result["min_score"],
            "max_overall_score": result["max_score"]
        }
    
    async def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """Get recent pipeline runs."""
        collection = self.collections["pipeline_runs"]
        
        cursor = collection.find().sort("started_at", -1).limit(limit)
        
        runs = []
        for document in cursor:
            runs.append(PipelineRunSummary(
                id=document["_id"],
                name=document["name"],
                started_at=document["started_at"],
                completed_at=document.get("completed_at"),
                status=document["status"],
                total_records=document.get("total_records", 0),
                successful_records=document.get("successful_records", 0),
                failed_records=document.get("failed_records", 0),
                skipped_records=document.get("skipped_records", 0),
                error_message=document.get("error_message")
            ))
        
        return runs
    
    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old tracking data."""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # Get pipeline runs to delete
        pipeline_runs_collection = self.collections["pipeline_runs"]
        old_runs_cursor = pipeline_runs_collection.find(
            {"started_at": {"$lt": cutoff_date}},
            {"_id": 1}
        )
        old_run_ids = [doc["_id"] for doc in old_runs_cursor]
        
        if not old_run_ids:
            return 0
        
        total_deleted = 0
        
        # Delete related data first
        for collection_name in ["failed_records", "system_metrics", "performance_metrics", 
                               "quality_metrics", "audit_events", "ingestion_stats"]:
            collection = self.collections[collection_name]
            
            if collection_name == "failed_records":
                # For failed_records, we need to find by ingestion_stat_id
                ingestion_stats_collection = self.collections["ingestion_stats"]
                ingestion_stat_ids = [
                    doc["_id"] for doc in ingestion_stats_collection.find(
                        {"pipeline_run_id": {"$in": old_run_ids}},
                        {"_id": 1}
                    )
                ]
                if ingestion_stat_ids:
                    result = collection.delete_many({"ingestion_stat_id": {"$in": ingestion_stat_ids}})
                    total_deleted += result.deleted_count
            else:
                result = collection.delete_many({"pipeline_run_id": {"$in": old_run_ids}})
                total_deleted += result.deleted_count
        
        # Delete pipeline runs
        result = pipeline_runs_collection.delete_many({"_id": {"$in": old_run_ids}})
        total_deleted += result.deleted_count
        
        logger.info(f"Cleaned up {total_deleted} old tracking records older than {days_to_keep} days")
        
        return total_deleted