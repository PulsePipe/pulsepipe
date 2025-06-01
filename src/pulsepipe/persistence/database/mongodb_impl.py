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
from .dialect import DatabaseDialect
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
        if self._database is None:
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
                # For insert operations, params should be the document to insert
                if params:
                    document = self._params_to_document(params, operation_type)
                else:
                    document = operation.get("document", {})
                
                result = collection.insert_one(document)
                return DatabaseResult(
                    rows=[],
                    lastrowid=str(result.inserted_id),
                    rowcount=1
                )
            
            elif operation_type == "find_one":
                # For find_one, params might be filter criteria
                filter_criteria = self._params_to_filter(params) if params else operation.get("filter", {})
                doc = collection.find_one(filter_criteria)
                rows = [self._convert_objectid_to_str(doc)] if doc else []
                return DatabaseResult(rows=rows)
            
            elif operation_type == "find":
                cursor = collection.find(
                    operation.get("filter", {}),
                    operation.get("projection")
                )
                
                # Apply skip only if specified and not None
                if operation.get("skip") is not None:
                    cursor = cursor.skip(operation["skip"])
                
                # Apply limit only if specified and not None/0
                if operation.get("limit") is not None and operation.get("limit") > 0:
                    cursor = cursor.limit(operation["limit"])
                
                # Apply sort if specified
                if operation.get("sort"):
                    cursor = cursor.sort(operation["sort"])
                
                rows = [self._convert_objectid_to_str(doc) for doc in cursor]
                return DatabaseResult(rows=rows)
            
            elif operation_type == "update_one":
                # For update operations, we need to construct filter and update from params
                if params and len(params) >= 2:
                    # Assume last param is the ID/filter, others are update values
                    update_doc, filter_doc = self._params_to_update(params, operation_type)
                    # Handle upsert options from the operation descriptor
                    options = operation.get("options", {})
                    result = collection.update_one(filter_doc, update_doc, **options)
                else:
                    # Handle upsert options from the operation descriptor
                    options = operation.get("options", {})
                    result = collection.update_one(
                        operation.get("filter", {}),
                        operation.get("update", {}),
                        **options
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
        if self._client is None or self._database is None:
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
    
    def _params_to_document(self, params: Union[Tuple, Dict], operation_type: str) -> Dict[str, Any]:
        """Convert parameters to MongoDB document for insert operations."""
        if isinstance(params, dict):
            return params
        
        # For tuple parameters, we need to map them to field names based on operation type
        if operation_type == "insert_one":
            # Map based on collection type - this is specific to the tracking repository schema
            if len(params) == 5:  # pipeline_run insert
                return {
                    "id": params[0],
                    "name": params[1], 
                    "started_at": params[2],
                    "status": params[3],
                    "config_snapshot": params[4],
                    "total_records": 0,
                    "successful_records": 0,
                    "failed_records": 0,
                    "skipped_records": 0,
                    "completed_at": None,
                    "error_message": None,
                    "updated_at": params[2]  # started_at
                }
            elif len(params) == 13:  # ingestion_stat insert
                return {
                    "pipeline_run_id": params[0],
                    "stage_name": params[1],
                    "file_path": params[2],
                    "record_id": params[3],
                    "record_type": params[4],
                    "status": params[5],
                    "error_category": params[6],
                    "error_message": params[7],
                    "error_details": params[8],
                    "processing_time_ms": params[9],
                    "record_size_bytes": params[10],
                    "data_source": params[11],
                    "timestamp": params[12]
                }
            elif len(params) == 8:  # audit_event insert
                return {
                    "pipeline_run_id": params[0],
                    "event_type": params[1],
                    "stage_name": params[2],
                    "record_id": params[3],
                    "event_level": params[4],
                    "message": params[5],
                    "details": params[6],
                    "correlation_id": params[7],
                    "timestamp": datetime.now().isoformat()
                }
            elif len(params) == 10:  # performance_metric insert
                return {
                    "pipeline_run_id": params[0],
                    "stage_name": params[1],
                    "started_at": params[2],
                    "completed_at": params[3],
                    "duration_ms": params[4],
                    "records_processed": params[5],
                    "records_per_second": params[6],
                    "memory_usage_mb": params[7],
                    "cpu_usage_percent": params[8],
                    "bottleneck_indicator": params[9]
                }
            elif len(params) == 15:  # quality_metric insert
                return {
                    "pipeline_run_id": params[0],
                    "record_id": params[1],
                    "record_type": params[2],
                    "completeness_score": params[3],
                    "consistency_score": params[4],
                    "validity_score": params[5],
                    "accuracy_score": params[6],
                    "overall_score": params[7],
                    "missing_fields": params[8],
                    "invalid_fields": params[9],
                    "outlier_fields": params[10],
                    "quality_issues": params[11],
                    "metrics_details": params[12],
                    "sampled": params[13],
                    "timestamp": params[14]
                }
            elif len(params) == 5:  # failed_record insert
                return {
                    "ingestion_stat_id": params[0],
                    "original_data": params[1],
                    "normalized_data": params[2],
                    "failure_reason": params[3],
                    "stack_trace": params[4],
                    "timestamp": datetime.now().isoformat()
                }
                
        return {}
    
    def _params_to_filter(self, params: Union[Tuple, Dict]) -> Dict[str, Any]:
        """Convert parameters to MongoDB filter criteria."""
        if isinstance(params, dict):
            return params
        elif isinstance(params, tuple) and len(params) == 1:
            # For bookmark operations, single parameter is typically a path
            # For other operations, it's typically an id
            # We need context to distinguish, so we'll use a heuristic:
            # If it looks like a file path (contains / or .), treat as path
            param = params[0]
            if isinstance(param, str) and ('/' in param or '\\' in param or '.' in param):
                return {"path": param}
            else:
                return {"id": param}
        return {}
    
    def _params_to_update(self, params: Union[Tuple, Dict], operation_type: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Convert parameters to MongoDB update document and filter."""
        if isinstance(params, dict):
            return params.get("update", {}), params.get("filter", {})
        
        # For bookmark operations (path, status)
        if len(params) == 2:
            update_doc = {
                "$setOnInsert": {
                    "path": params[0],
                    "status": params[1],
                    "timestamp": datetime.now().isoformat()
                }
            }
            filter_doc = {"path": params[0]}
            return update_doc, filter_doc
        
        # For pipeline run updates
        elif len(params) == 5:  # pipeline_run completion update
            update_doc = {
                "$set": {
                    "completed_at": params[0],
                    "status": params[1],
                    "error_message": params[2],
                    "updated_at": params[3]
                }
            }
            filter_doc = {"id": params[4]}
            return update_doc, filter_doc
        elif len(params) == 6:  # pipeline_run count update
            update_doc = {
                "$inc": {
                    "total_records": params[0],
                    "successful_records": params[1],
                    "failed_records": params[2],
                    "skipped_records": params[3]
                },
                "$set": {
                    "updated_at": params[4]
                }
            }
            filter_doc = {"id": params[5]}
            return update_doc, filter_doc
            
        return {}, {}
    
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
        if self._database is None:
            raise ConnectionError("Database connection is not established")
        return self._database


class MongoDBAdapter(DatabaseDialect):
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
    
    def get_pipeline_run_insert(self) -> str:
        """Get MongoDB operation for inserting a pipeline run record."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "insert_one",
            "document": {}  # Will be filled by parameters
        })
    
    def get_pipeline_run_update(self) -> str:
        """Get MongoDB operation for updating a pipeline run record."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "update_one",
            "filter": {},  # Will be filled by parameters
            "update": {}   # Will be filled by parameters
        })
    
    def get_pipeline_run_select(self) -> str:
        """Get MongoDB operation for selecting a pipeline run by ID."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "find_one",
            "filter": {}  # Will be filled by parameters
        })
    
    def get_pipeline_runs_list(self) -> str:
        """Get MongoDB operation for listing recent pipeline runs."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "find",
            "filter": {},
            "sort": [["started_at", -1]],
            "limit": 0  # Will be filled by parameters
        })
    
    def get_ingestion_stat_insert(self) -> str:
        """Get MongoDB operation for inserting an ingestion statistic."""
        return json.dumps({
            "collection": f"{self.collection_prefix}ingestion_stats",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_failed_record_insert(self) -> str:
        """Get MongoDB operation for inserting a failed record."""
        return json.dumps({
            "collection": f"{self.collection_prefix}failed_records",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_audit_event_insert(self) -> str:
        """Get MongoDB operation for inserting an audit event."""
        return json.dumps({
            "collection": f"{self.collection_prefix}audit_events",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_quality_metric_insert(self) -> str:
        """Get MongoDB operation for inserting a quality metric."""
        return json.dumps({
            "collection": f"{self.collection_prefix}quality_metrics",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_performance_metric_insert(self) -> str:
        """Get MongoDB operation for inserting a performance metric."""
        return json.dumps({
            "collection": f"{self.collection_prefix}performance_metrics",
            "operation": "insert_one",
            "document": {}
        })
    
    def get_pipeline_run_count_update(self) -> str:
        """Get MongoDB operation for updating pipeline run counts."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "update_one",
            "filter": {},  # Will be filled by parameters
            "update": {}   # Will be filled by parameters
        })
    
    def get_recent_pipeline_runs(self, limit: int = 10) -> str:
        """Get MongoDB operation for recent pipeline runs."""
        return json.dumps({
            "collection": f"{self.collection_prefix}pipeline_runs",
            "operation": "find",
            "filter": {},
            "sort": [["started_at", -1]],
            "limit": limit
        })
    
    def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
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
    
    def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Tuple[str, List[Any]]:
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
    
    def get_cleanup(self, cutoff_date: datetime) -> List[Tuple[str, List[Any]]]:
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
    
    # Bookmark Store Methods
    
    def get_bookmark_table_create(self) -> str:
        """Get MongoDB operation for creating bookmarks collection."""
        return json.dumps({
            "collection": f"{self.collection_prefix}bookmarks",
            "operation": "create_index",
            "keys": [["path", 1]],
            "options": {"unique": True}
        })
    
    def get_bookmark_check(self) -> str:
        """Get MongoDB operation for checking if a bookmark exists."""
        return json.dumps({
            "collection": f"{self.collection_prefix}bookmarks",
            "operation": "find_one",
            "filter": {}  # Will be filled by parameters
        })
    
    def get_bookmark_insert(self) -> str:
        """Get MongoDB operation for inserting a bookmark."""
        return json.dumps({
            "collection": f"{self.collection_prefix}bookmarks",
            "operation": "update_one",
            "filter": {},  # Will be filled by parameters
            "update": {"$setOnInsert": {}},  # Will be filled by parameters
            "options": {"upsert": True}
        })
    
    def get_bookmark_list(self) -> str:
        """Get MongoDB operation for listing all bookmarks."""
        return json.dumps({
            "collection": f"{self.collection_prefix}bookmarks",
            "operation": "find",
            "filter": {},
            "sort": [["path", 1]],
            "projection": {"path": 1, "_id": 0}
        })
    
    def get_bookmark_clear(self) -> str:
        """Get MongoDB operation for clearing all bookmarks."""
        return json.dumps({
            "collection": f"{self.collection_prefix}bookmarks",
            "operation": "delete_many",
            "filter": {}
        })