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

# src/pulsepipe/persistence/database/mongodb_impl.py

"""
MongoDB implementation of database connection and adapter.

Provides MongoDB-specific implementations with document-based storage patterns.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager

try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.collection import Collection
    from pymongo.database import Database
    from bson import ObjectId
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False

from .connection import DatabaseConnection, DatabaseResult
from .dialect import SQLDialect
from .exceptions import (
    ConnectionError,
    QueryError,
    TransactionError,
    ConfigurationError,
    NotSupportedError,
    wrap_database_error
)


class MongoDBConnection(DatabaseConnection):
    """
    MongoDB implementation of DatabaseConnection.
    
    Uses pymongo to provide document-based storage with a SQL-like interface.
    """
    
    def __init__(self, connection_string: str, database: str, 
                 collection_prefix: str = "audit_", **kwargs):
        """
        Initialize MongoDB connection.
        
        Args:
            connection_string: MongoDB connection string
            database: Database name
            collection_prefix: Prefix for collection names
            **kwargs: Additional connection parameters
        """
        if not PYMONGO_AVAILABLE:
            raise ConfigurationError(
                "MongoDB support requires pymongo. Install with: pip install pymongo"
            )
        
        self.connection_string = connection_string
        self.database_name = database
        self.collection_prefix = collection_prefix
        self.connection_options = kwargs
        
        self._client: Optional[MongoClient] = None
        self._database: Optional[Database] = None
        self._in_transaction = False
        self._session = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish MongoDB connection."""
        try:
            self._client = MongoClient(
                self.connection_string,
                **self.connection_options
            )
            
            # Test connection
            self._client.admin.command('ping')
            
            self._database = self._client[self.database_name]
            
            # Create indexes for common queries
            self._create_indexes()
            
        except pymongo.errors.PyMongoError as e:
            raise ConnectionError(
                f"Failed to connect to MongoDB: {self.connection_string}",
                {
                    "connection_string": self.connection_string,
                    "database": self.database_name
                },
                e
            )
    
    def _create_indexes(self) -> None:
        """Create indexes for efficient querying."""
        try:
            # Pipeline runs collection indexes
            pipeline_runs = self._database[f"{self.collection_prefix}pipeline_runs"]
            pipeline_runs.create_index("name")
            pipeline_runs.create_index("started_at")
            pipeline_runs.create_index("status")
            
            # Ingestion stats collection indexes
            ingestion_stats = self._database[f"{self.collection_prefix}ingestion_stats"]
            ingestion_stats.create_index("pipeline_run_id")
            ingestion_stats.create_index("status")
            ingestion_stats.create_index("stage_name")
            ingestion_stats.create_index("timestamp")
            ingestion_stats.create_index([("pipeline_run_id", 1), ("timestamp", -1)])
            
            # Audit events collection indexes
            audit_events = self._database[f"{self.collection_prefix}audit_events"]
            audit_events.create_index("pipeline_run_id")
            audit_events.create_index("event_type")
            audit_events.create_index("timestamp")
            audit_events.create_index("correlation_id")
            
            # Quality metrics collection indexes
            quality_metrics = self._database[f"{self.collection_prefix}quality_metrics"]
            quality_metrics.create_index("pipeline_run_id")
            quality_metrics.create_index("record_type")
            quality_metrics.create_index("overall_score")
            
            # Performance metrics collection indexes
            performance_metrics = self._database[f"{self.collection_prefix}performance_metrics"]
            performance_metrics.create_index("pipeline_run_id")
            performance_metrics.create_index("stage_name")
            performance_metrics.create_index("started_at")
            
        except pymongo.errors.PyMongoError as e:
            # Index creation failure is not critical
            pass
    
    def execute(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> DatabaseResult:
        """
        Execute a MongoDB operation.
        
        This method translates SQL-like operations to MongoDB operations.
        The query parameter should be a MongoDB operation descriptor.
        """
        if not self._database:
            raise ConnectionError("Database connection is not established")
        
        try:
            # Parse the "query" as a MongoDB operation descriptor
            operation = json.loads(query) if isinstance(query, str) else query
            
            collection_name = operation.get("collection")
            operation_type = operation.get("operation")
            
            if not collection_name or not operation_type:
                raise QueryError("Invalid MongoDB operation descriptor")
            
            collection = self._database[collection_name]
            
            if operation_type == "insert_one":
                result = collection.insert_one(operation.get("document", {}))
                return DatabaseResult(
                    rows=[],
                    lastrowid=str(result.inserted_id),
                    rowcount=1
                )
            
            elif operation_type == "find_one":
                doc = collection.find_one(operation.get("filter", {}))
                rows = [self._convert_objectid_to_str(doc)] if doc else []
                return DatabaseResult(rows=rows)
            
            elif operation_type == "find":
                cursor = collection.find(
                    operation.get("filter", {}),
                    operation.get("projection"),
                    limit=operation.get("limit"),
                    skip=operation.get("skip")
                )
                if operation.get("sort"):
                    cursor = cursor.sort(operation["sort"])
                
                rows = [self._convert_objectid_to_str(doc) for doc in cursor]
                return DatabaseResult(rows=rows)
            
            elif operation_type == "update_one":
                result = collection.update_one(
                    operation.get("filter", {}),
                    operation.get("update", {})
                )
                return DatabaseResult(
                    rows=[],
                    rowcount=result.modified_count
                )
            
            elif operation_type == "delete_many":
                result = collection.delete_many(operation.get("filter", {}))
                return DatabaseResult(
                    rows=[],
                    rowcount=result.deleted_count
                )
            
            elif operation_type == "aggregate":
                pipeline = operation.get("pipeline", [])
                rows = [self._convert_objectid_to_str(doc) for doc in collection.aggregate(pipeline)]
                return DatabaseResult(rows=rows)
            
            else:
                raise QueryError(f"Unsupported MongoDB operation: {operation_type}")
                
        except (pymongo.errors.PyMongoError, ValueError, json.JSONDecodeError) as e:
            raise wrap_database_error(
                e,
                f"MongoDB operation failed: {query[:100]}...",
                {"query": query, "params": params}
            )
    
    def executemany(self, query: str, params_list: List[Union[Tuple, Dict]]) -> DatabaseResult:
        """Execute multiple MongoDB operations."""
        total_rowcount = 0
        last_id = None
        
        for params in params_list:
            result = self.execute(query, params)
            total_rowcount += result.rowcount or 0
            if result.lastrowid:
                last_id = result.lastrowid
        
        return DatabaseResult(
            rows=[],
            lastrowid=last_id,
            rowcount=total_rowcount
        )
    
    def commit(self) -> None:
        """Commit the current transaction (if in transaction)."""
        if self._in_transaction and self._session:
            try:
                self._session.commit_transaction()
                self._in_transaction = False
            except pymongo.errors.PyMongoError as e:
                raise TransactionError("Failed to commit transaction", original_error=e)
    
    def rollback(self) -> None:
        """Rollback the current transaction (if in transaction)."""
        if self._in_transaction and self._session:
            try:
                self._session.abort_transaction()
                self._in_transaction = False
            except pymongo.errors.PyMongoError as e:
                raise TransactionError("Failed to rollback transaction", original_error=e)
    
    def close(self) -> None:
        """Close the database connection."""
        if self._session:
            self._session.end_session()
            self._session = None
        
        if self._client:
            try:
                self._client.close()
                self._client = None
                self._database = None
            except pymongo.errors.PyMongoError as e:
                raise ConnectionError("Failed to close database connection", original_error=e)
    
    def is_connected(self) -> bool:
        """Check if the connection is still active."""
        if not self._client or not self._database:
            return False
        
        try:
            self._client.admin.command('ping')
            return True
        except pymongo.errors.PyMongoError:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection."""
        return {
            "database_type": "mongodb",
            "connection_string": self.connection_string,
            "database": self.database_name,
            "collection_prefix": self.collection_prefix,
            "is_connected": self.is_connected()
        }
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        if not self._client:
            raise ConnectionError("Database connection is not established")
        
        # MongoDB transactions require replica sets
        try:
            self._session = self._client.start_session()
            self._session.start_transaction()
            self._in_transaction = True
            
            yield self
            self.commit()
            
        except Exception:
            self.rollback()
            raise
        finally:
            if self._session:
                self._session.end_session()
                self._session = None
    
    def _convert_objectid_to_str(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Convert ObjectId fields to strings for compatibility."""
        if doc is None:
            return doc
        
        converted = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                converted[key] = str(value)
            elif isinstance(value, dict):
                converted[key] = self._convert_objectid_to_str(value)
            elif isinstance(value, list):
                converted[key] = [
                    self._convert_objectid_to_str(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                converted[key] = value
        
        return converted
    
    def get_raw_client(self) -> MongoClient:
        """
        Get the underlying pymongo client for advanced operations.
        
        Returns:
            Raw pymongo MongoClient object
        """
        if not self._client:
            raise ConnectionError("Database connection is not established")
        return self._client
    
    def get_database(self) -> Database:
        """
        Get the MongoDB database object.
        
        Returns:
            pymongo Database object
        """
        if not self._database:
            raise ConnectionError("Database connection is not established")
        return self._database


class MongoDBAdapter(SQLDialect):
    """
    MongoDB adapter that translates SQL-like operations to MongoDB operations.
    
    This adapter allows the TrackingRepository to work with MongoDB using
    a document-based approach while maintaining the same interface.
    """
    
    def __init__(self, collection_prefix: str = "audit_"):
        """
        Initialize MongoDB adapter.
        
        Args:
            collection_prefix: Prefix for collection names
        """
        self.collection_prefix = collection_prefix
    
    def get_pipeline_run_insert_sql(self) -> str:
        """Get MongoDB operation for inserting a pipeline run record."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "insert_one",
            "document": {}  # Will be filled by parameters
        })
    
    def get_pipeline_run_update_sql(self) -> str:
        """Get MongoDB operation for updating a pipeline run record."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "update_one",
            "filter": {},  # Will be filled by parameters
            "update": {}   # Will be filled by parameters
        })
    
    def get_pipeline_run_select_sql(self) -> str:
        """Get MongoDB operation for selecting a pipeline run by ID."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "find_one",
            "filter": {}  # Will be filled by parameters
        })
    
    def get_pipeline_runs_list_sql(self) -> str:
        """Get MongoDB operation for listing recent pipeline runs."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "find",
            "filter": {},
            "sort": [["started_at", -1]],
            "limit": 0  # Will be filled by parameters
        })
    
    def get_ingestion_stat_insert_sql(self) -> str:
        """Get MongoDB operation for inserting an ingestion statistic."""
        return json.dumps({
            "collection": f"{self.collection_prefix}ingestion_stats",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_failed_record_insert_sql(self) -> str:
        """Get MongoDB operation for inserting a failed record."""
        return json.dumps({
            "collection": f"{self.collection_prefix}failed_records",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_audit_event_insert_sql(self) -> str:
        """Get MongoDB operation for inserting an audit event."""
        return json.dumps({
            "collection": f"{self.collection_prefix}audit_events",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_quality_metric_insert_sql(self) -> str:
        """Get MongoDB operation for inserting a quality metric."""
        return json.dumps({
            "collection": f"{self.collection_prefix}quality_metrics",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_performance_metric_insert_sql(self) -> str:
        """Get MongoDB operation for inserting a performance metric."""
        return json.dumps({
            "collection": f"{self.collection_prefix}performance_metrics",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_ingestion_summary_sql(self, pipeline_run_id: Optional[str] = None,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> Tuple[str, List[Any]]:
        """Get MongoDB aggregation for ingestion summary with optional filters."""
        match_stage = {}
        
        if pipeline_run_id:
            match_stage["pipeline_run_id"] = pipeline_run_id
        
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = self.format_datetime(start_date)
            if end_date:
                date_filter["$lte"] = self.format_datetime(end_date)
            match_stage["timestamp"] = date_filter
        
        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})
        
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
            },
            {
                "$project": {
                    "status": "$_id.status",
                    "error_category": "$_id.error_category",
                    "count": 1,
                    "avg_processing_time": 1,
                    "total_bytes": 1,
                    "_id": 0
                }
            }
        ])
        
        operation = json.dumps({
            "collection": f"{self.collection_prefix}ingestion_stats",
            "operation": "aggregate",
            "pipeline": pipeline
        })
        
        return operation, []
    
    def get_quality_summary_sql(self, pipeline_run_id: Optional[str] = None) -> Tuple[str, List[Any]]:
        """Get MongoDB aggregation for quality summary with optional filters."""
        match_stage = {}
        if pipeline_run_id:
            match_stage["pipeline_run_id"] = pipeline_run_id
        
        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})
        
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
        
        operation = json.dumps({
            "collection": f"{self.collection_prefix}quality_metrics",
            "operation": "aggregate",
            "pipeline": pipeline
        })
        
        return operation, []
    
    def get_cleanup_sql(self, cutoff_date: datetime) -> List[Tuple[str, List[Any]]]:
        """Get MongoDB operations for cleaning up old data."""
        # For MongoDB, we'll delete documents older than cutoff_date
        cleanup_operations = []
        
        # Collections to clean up
        collections = [
            "failed_records",
            "system_metrics", 
            "performance_metrics",
            "quality_metrics",
            "audit_events",
            "ingestion_stats",
            "pipeline_runs"
        ]
        
        for collection in collections:
            operation = json.dumps({
                "collection": f"{self.collection_prefix}{collection}",
                "operation": "delete_many",
                "filter": {"started_at" if collection == "pipeline_runs" else "timestamp": {"$lt": self.format_datetime(cutoff_date)}}
            })
            cleanup_operations.append((operation, []))
        
        return cleanup_operations
    
    def format_datetime(self, dt: datetime) -> str:
        """Format datetime for MongoDB storage via JSON serialization."""
        return dt.isoformat()
    
    def parse_datetime(self, dt_obj: Any) -> datetime:
        """Parse datetime from MongoDB storage format."""
        if isinstance(dt_obj, datetime):
            return dt_obj
        elif isinstance(dt_obj, str):
            return datetime.fromisoformat(dt_obj)
        else:
            raise ValueError(f"Cannot parse datetime from: {type(dt_obj)}")
    
    def get_auto_increment_syntax(self) -> str:
        """MongoDB uses ObjectId for auto-generated IDs."""
        raise NotSupportedError("MongoDB uses ObjectId for auto-generated IDs")
    
    def get_json_column_type(self) -> str:
        """MongoDB natively supports document storage."""
        return "DOCUMENT"
    
    def serialize_json(self, data: Any) -> Any:
        """MongoDB natively supports document storage."""
        return data
    
    def deserialize_json(self, data: Any) -> Any:
        """MongoDB natively supports document storage."""
        return data
    
    def escape_identifier(self, identifier: str) -> str:
        """MongoDB field names don't need escaping in most cases."""
        return identifier
    
    def get_limit_syntax(self, limit: int, offset: Optional[int] = None) -> str:
        """Get MongoDB limit/skip syntax (not used directly)."""
        return f"limit: {limit}" + (f", skip: {offset}" if offset else "")
    
    def supports_feature(self, feature: str) -> bool:
        """Check if MongoDB supports a specific feature."""
        mongodb_features = {
            "transactions",  # Requires replica sets
            "document_storage",
            "flexible_schema",
            "aggregation_pipeline",
            "text_search",
            "geospatial_queries",
            "json_queries",
            "horizontal_scaling"
        }
        return feature in mongodb_features