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

# tests/test_mongodb_connection.py

"""
Unit tests for MongoDB database connection implementation.

Tests MongoDBConnection and MongoDBAdapter classes.
"""

import pytest
import json
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock

from pulsepipe.persistence.database.mongodb_impl import MongoDBConnection, MongoDBAdapter
from pulsepipe.persistence.database.connection import DatabaseResult
from pulsepipe.persistence.database.exceptions import (
    ConnectionError,
    QueryError,
    TransactionError,
    ConfigurationError
)


class TestMongoDBConnection:
    """Test MongoDBConnection class."""
    
    def test_init_missing_pymongo(self):
        """Test initialization when pymongo is not available."""
        with patch('pulsepipe.persistence.database.mongodb_impl.PYMONGO_AVAILABLE', False):
            with pytest.raises(ConfigurationError) as exc_info:
                MongoDBConnection("mongodb://localhost:27017/", "test")
            
            assert "pymongo" in str(exc_info.value)
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_init_success(self, mock_mongo_client):
        """Test successful initialization."""
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection(
            connection_string="mongodb://localhost:27017/",
            database="testdb",
            collection_prefix="test_",
            username="user",
            password="pass"
        )
        
        assert conn.connection_string == "mongodb://localhost:27017/"
        assert conn.database_name == "testdb"
        assert conn.collection_prefix == "test_"
        
        # Verify client creation
        mock_mongo_client.assert_called_once_with(
            "mongodb://localhost:27017/",
            username="user",
            password="pass"
        )
        
        # Verify ping was called to test connection
        mock_client.admin.command.assert_called_with('ping')
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_init_connection_error(self, mock_mongo_client):
        """Test initialization with connection error."""
        import pymongo.errors
        
        mock_client = MagicMock()
        # Use actual PyMongoError to match what the code catches
        mock_client.admin.command.side_effect = pymongo.errors.PyMongoError("Connection failed")
        mock_mongo_client.return_value = mock_client
        
        with pytest.raises(ConnectionError) as exc_info:
            MongoDBConnection("mongodb://localhost:27017/", "test")
        
        assert "Failed to connect to MongoDB" in str(exc_info.value)
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_insert_one(self, mock_mongo_client):
        """Test executing insert_one operation."""
        # Setup mocks
        mock_result = MagicMock()
        mock_result.inserted_id = "507f1f77bcf86cd799439011"
        
        mock_collection = MagicMock()
        mock_collection.insert_one.return_value = mock_result
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "insert_one",
            "document": {"name": "test", "value": 123}
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert result.lastrowid == "507f1f77bcf86cd799439011"
        assert result.rowcount == 1
        
        mock_collection.insert_one.assert_called_once_with({"name": "test", "value": 123})
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_find_one(self, mock_mongo_client):
        """Test executing find_one operation."""
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = {"_id": "507f1f77bcf86cd799439011", "name": "test"}
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "find_one",
            "filter": {"name": "test"}
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert len(result.rows) == 1
        assert result.rows[0]["_id"] == "507f1f77bcf86cd799439011"
        assert result.rows[0]["name"] == "test"
        
        mock_collection.find_one.assert_called_once_with({"name": "test"})
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_find_one_not_found(self, mock_mongo_client):
        """Test executing find_one that returns no results."""
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = None
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "find_one",
            "filter": {"name": "nonexistent"}
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert len(result.rows) == 0
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_find_with_options(self, mock_mongo_client):
        """Test executing find operation with options."""
        mock_cursor = MagicMock()
        # Make the cursor iterable
        mock_cursor.__iter__.return_value = iter([
            {"_id": "1", "name": "test1"},
            {"_id": "2", "name": "test2"}
        ])
        mock_cursor.sort.return_value = mock_cursor
        
        mock_collection = MagicMock()
        mock_collection.find.return_value = mock_cursor
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "find",
            "filter": {"active": True},
            "projection": {"name": 1},
            "limit": 10,
            "skip": 5,
            "sort": [["name", 1]]  # JSON serialization converts tuples to lists
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert len(result.rows) == 2
        
        mock_collection.find.assert_called_once_with(
            {"active": True},
            {"name": 1},
            limit=10,
            skip=5
        )
        mock_cursor.sort.assert_called_once_with([["name", 1]])
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_update_one(self, mock_mongo_client):
        """Test executing update_one operation."""
        mock_result = MagicMock()
        mock_result.modified_count = 1
        
        mock_collection = MagicMock()
        mock_collection.update_one.return_value = mock_result
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "update_one",
            "filter": {"_id": "123"},
            "update": {"$set": {"name": "updated"}}
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert result.rowcount == 1
        
        mock_collection.update_one.assert_called_once_with(
            {"_id": "123"},
            {"$set": {"name": "updated"}}
        )
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_delete_many(self, mock_mongo_client):
        """Test executing delete_many operation."""
        mock_result = MagicMock()
        mock_result.deleted_count = 5
        
        mock_collection = MagicMock()
        mock_collection.delete_many.return_value = mock_result
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "delete_many",
            "filter": {"active": False}
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert result.rowcount == 5
        
        mock_collection.delete_many.assert_called_once_with({"active": False})
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_aggregate(self, mock_mongo_client):
        """Test executing aggregate operation."""
        mock_collection = MagicMock()
        mock_collection.aggregate.return_value = [
            {"_id": "group1", "count": 10},
            {"_id": "group2", "count": 5}
        ]
        
        mock_database = MagicMock()
        mock_database.__getitem__.return_value = mock_collection
        
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "aggregate",
            "pipeline": [
                {"$group": {"_id": "$category", "count": {"$sum": 1}}}
            ]
        }
        
        result = conn.execute(json.dumps(operation))
        
        assert isinstance(result, DatabaseResult)
        assert len(result.rows) == 2
        assert result.rows[0]["_id"] == "group1"
        assert result.rows[0]["count"] == 10
        
        mock_collection.aggregate.assert_called_once_with([
            {"$group": {"_id": "$category", "count": {"$sum": 1}}}
        ])
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_invalid_operation(self, mock_mongo_client):
        """Test executing invalid operation."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {
            "collection": "test_collection",
            "operation": "invalid_operation"
        }
        
        with pytest.raises(QueryError):
            conn.execute(json.dumps(operation))
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_missing_operation_info(self, mock_mongo_client):
        """Test executing operation with missing info."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        operation = {"collection": "test_collection"}  # Missing operation
        
        with pytest.raises(QueryError):
            conn.execute(json.dumps(operation))
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_execute_no_connection(self, mock_mongo_client):
        """Test execute without database connection."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        conn._database = None
        
        with pytest.raises(ConnectionError):
            conn.execute('{"collection": "test", "operation": "find_one"}')
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_executemany(self, mock_mongo_client):
        """Test executemany method."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        # Mock the execute method to track calls
        conn.execute = MagicMock(side_effect=[
            DatabaseResult([], "id1", 1),
            DatabaseResult([], "id2", 1),
            DatabaseResult([], "id3", 1)
        ])
        
        query = '{"collection": "test", "operation": "insert_one"}'
        params_list = [{"doc1": 1}, {"doc2": 2}, {"doc3": 3}]
        
        result = conn.executemany(query, params_list)
        
        assert result.rowcount == 3
        assert result.lastrowid == "id3"
        assert conn.execute.call_count == 3
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_commit_with_transaction(self, mock_mongo_client):
        """Test commit with active transaction."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        conn._session = mock_session
        conn._in_transaction = True
        
        conn.commit()
        
        mock_session.commit_transaction.assert_called_once()
        assert conn._in_transaction is False
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_commit_error(self, mock_mongo_client):
        """Test commit with error."""
        import pymongo.errors
        
        mock_session = MagicMock()
        mock_session.commit_transaction.side_effect = pymongo.errors.PyMongoError("Commit failed")
        
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        conn._session = mock_session
        conn._in_transaction = True
        
        with pytest.raises(TransactionError) as exc_info:
            conn.commit()
        
        assert "Failed to commit transaction" in str(exc_info.value)
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_rollback_with_transaction(self, mock_mongo_client):
        """Test rollback with active transaction."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        conn._session = mock_session
        conn._in_transaction = True
        
        conn.rollback()
        
        mock_session.abort_transaction.assert_called_once()
        assert conn._in_transaction is False
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_close(self, mock_mongo_client):
        """Test close method."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        conn._session = mock_session
        
        conn.close()
        
        mock_session.end_session.assert_called_once()
        mock_client.close.assert_called_once()
        assert conn._client is None
        assert conn._database is None
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_is_connected_true(self, mock_mongo_client):
        """Test is_connected returns True."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        assert conn.is_connected() is True
        mock_client.admin.command.assert_called_with('ping')
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_is_connected_false(self, mock_mongo_client):
        """Test is_connected returns False on error."""
        import pymongo.errors
        
        mock_client = MagicMock()
        # First call succeeds (for init), second call fails (for is_connected)
        mock_client.admin.command.side_effect = [None, pymongo.errors.PyMongoError("Connection error")]
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        assert conn.is_connected() is False
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_get_connection_info(self, mock_mongo_client):
        """Test get_connection_info method."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection(
            "mongodb://localhost:27017/",
            "testdb",
            collection_prefix="test_"
        )
        
        info = conn.get_connection_info()
        
        assert info["database_type"] == "mongodb"
        assert info["connection_string"] == "mongodb://localhost:27017/"
        assert info["database"] == "testdb"
        assert info["collection_prefix"] == "test_"
        assert "is_connected" in info
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_transaction_success(self, mock_mongo_client):
        """Test successful transaction context manager."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.start_session.return_value = mock_session
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        with conn.transaction():
            pass
        
        mock_client.start_session.assert_called_once()
        mock_session.start_transaction.assert_called_once()
        mock_session.commit_transaction.assert_called_once()
        mock_session.end_session.assert_called_once()
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_transaction_rollback_on_exception(self, mock_mongo_client):
        """Test transaction rollback on exception."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.start_session.return_value = mock_session
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        with pytest.raises(ValueError):
            with conn.transaction():
                raise ValueError("Test exception")
        
        mock_session.abort_transaction.assert_called_once()
        mock_session.end_session.assert_called_once()
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_convert_objectid_to_str(self, mock_mongo_client):
        """Test ObjectId conversion to string."""
        from bson import ObjectId
        
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "name": "test",
            "nested": {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "value": 123
            },
            "list": [
                {"_id": ObjectId("507f1f77bcf86cd799439013"), "item": 1},
                {"_id": ObjectId("507f1f77bcf86cd799439014"), "item": 2}
            ]
        }
        
        converted = conn._convert_objectid_to_str(doc)
        
        assert converted["_id"] == "507f1f77bcf86cd799439011"
        assert converted["name"] == "test"
        assert converted["nested"]["_id"] == "507f1f77bcf86cd799439012"
        assert converted["list"][0]["_id"] == "507f1f77bcf86cd799439013"
        assert converted["list"][1]["_id"] == "507f1f77bcf86cd799439014"
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_get_raw_client(self, mock_mongo_client):
        """Test get_raw_client method."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        raw_client = conn.get_raw_client()
        assert raw_client is mock_client
        
        conn.close()
    
    @patch('pulsepipe.persistence.database.mongodb_impl.MongoClient')
    def test_get_database(self, mock_mongo_client):
        """Test get_database method."""
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_client.__getitem__.return_value = mock_database
        mock_mongo_client.return_value = mock_client
        
        conn = MongoDBConnection("mongodb://localhost:27017/", "test")
        
        database = conn.get_database()
        assert database is mock_database
        
        conn.close()


class TestMongoDBAdapter:
    """Test MongoDBAdapter class."""
    
    @pytest.fixture
    def adapter(self):
        """Create a MongoDBAdapter instance."""
        return MongoDBAdapter(collection_prefix="test_")
    
    def test_init(self, adapter):
        """Test adapter initialization."""
        assert adapter.collection_prefix == "test_"
    
    def test_get_pipeline_run_insert_sql(self, adapter):
        """Test pipeline run insert operation generation."""
        operation_json = adapter.get_pipeline_run_insert_sql()
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_pipeline_runs"
        assert operation["operation"] == "insert_one"
        assert "document" in operation
    
    def test_get_pipeline_run_update_sql(self, adapter):
        """Test pipeline run update operation generation."""
        operation_json = adapter.get_pipeline_run_update_sql()
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_pipeline_runs"
        assert operation["operation"] == "update_one"
        assert "filter" in operation
        assert "update" in operation
    
    def test_get_pipeline_run_select_sql(self, adapter):
        """Test pipeline run select operation generation."""
        operation_json = adapter.get_pipeline_run_select_sql()
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_pipeline_runs"
        assert operation["operation"] == "find_one"
        assert "filter" in operation
    
    def test_get_pipeline_runs_list_sql(self, adapter):
        """Test pipeline runs list operation generation."""
        operation_json = adapter.get_pipeline_runs_list_sql()
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_pipeline_runs"
        assert operation["operation"] == "find"
        # The sort field should be a list with tuples for MongoDB
        assert operation["sort"] == [["started_at", -1]]
        assert "limit" in operation
    
    def test_get_ingestion_summary_sql_no_filters(self, adapter):
        """Test ingestion summary aggregation without filters."""
        operation_json, params = adapter.get_ingestion_summary_sql()
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_ingestion_stats"
        assert operation["operation"] == "aggregate"
        assert "pipeline" in operation
        assert len(params) == 0
        
        pipeline = operation["pipeline"]
        assert any("$group" in stage for stage in pipeline)
    
    def test_get_ingestion_summary_sql_with_filters(self, adapter):
        """Test ingestion summary aggregation with filters."""
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
        
        operation_json, params = adapter.get_ingestion_summary_sql(
            pipeline_run_id="test-123",
            start_date=start_date,
            end_date=end_date
        )
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_ingestion_stats"
        assert operation["operation"] == "aggregate"
        
        pipeline = operation["pipeline"]
        
        # Should have a $match stage first
        assert pipeline[0]["$match"]["pipeline_run_id"] == "test-123"
        assert "$gte" in pipeline[0]["$match"]["timestamp"]
        assert "$lte" in pipeline[0]["$match"]["timestamp"]
        
        # Should still have $group stage
        assert any("$group" in stage for stage in pipeline)
    
    def test_get_quality_summary_sql_no_filter(self, adapter):
        """Test quality summary aggregation without filter."""
        operation_json, params = adapter.get_quality_summary_sql()
        operation = json.loads(operation_json)
        
        assert operation["collection"] == "test_quality_metrics"
        assert operation["operation"] == "aggregate"
        assert len(params) == 0
        
        pipeline = operation["pipeline"]
        # Should have only $group stage (no $match)
        assert len(pipeline) == 1
        assert "$group" in pipeline[0]
    
    def test_get_quality_summary_sql_with_filter(self, adapter):
        """Test quality summary aggregation with filter."""
        operation_json, params = adapter.get_quality_summary_sql(pipeline_run_id="test-123")
        operation = json.loads(operation_json)
        
        pipeline = operation["pipeline"]
        # Should have $match stage first
        assert len(pipeline) == 2
        assert pipeline[0]["$match"]["pipeline_run_id"] == "test-123"
        assert "$group" in pipeline[1]
    
    def test_get_cleanup_sql(self, adapter):
        """Test cleanup operations generation."""
        cutoff_date = datetime.now() - timedelta(days=30)
        operations = adapter.get_cleanup_sql(cutoff_date)
        
        assert isinstance(operations, list)
        assert len(operations) > 0
        
        # Check that all operations are delete_many
        for operation_json, params in operations:
            operation = json.loads(operation_json)
            assert operation["operation"] == "delete_many"
            assert "filter" in operation
            assert len(params) == 0  # MongoDB operations don't use separate params
    
    def test_format_datetime(self, adapter):
        """Test datetime formatting (should return ISO format string)."""
        dt = datetime(2023, 1, 15, 10, 30, 45)
        formatted = adapter.format_datetime(dt)
        
        assert formatted == "2023-01-15T10:30:45"  # Should return ISO format string
    
    def test_parse_datetime_datetime_object(self, adapter):
        """Test datetime parsing with datetime object."""
        dt = datetime(2023, 1, 15, 10, 30, 45)
        parsed = adapter.parse_datetime(dt)
        
        assert parsed is dt
    
    def test_parse_datetime_string(self, adapter):
        """Test datetime parsing with string."""
        dt_str = "2023-01-15T10:30:45"
        parsed = adapter.parse_datetime(dt_str)
        
        assert isinstance(parsed, datetime)
        assert parsed.year == 2023
        assert parsed.month == 1
        assert parsed.day == 15
    
    def test_parse_datetime_invalid(self, adapter):
        """Test datetime parsing with invalid input."""
        with pytest.raises(ValueError):
            adapter.parse_datetime(123)  # Invalid type
    
    def test_get_auto_increment_syntax(self, adapter):
        """Test auto-increment syntax (not supported)."""
        from pulsepipe.persistence.database.exceptions import NotSupportedError
        
        with pytest.raises(NotSupportedError):
            adapter.get_auto_increment_syntax()
    
    def test_get_json_column_type(self, adapter):
        """Test JSON column type."""
        col_type = adapter.get_json_column_type()
        assert col_type == "DOCUMENT"
    
    def test_serialize_json(self, adapter):
        """Test JSON serialization (should return as-is)."""
        data = {"key": "value", "number": 123}
        result = adapter.serialize_json(data)
        
        assert result is data
    
    def test_deserialize_json(self, adapter):
        """Test JSON deserialization (should return as-is)."""
        data = {"key": "value", "number": 123}
        result = adapter.deserialize_json(data)
        
        assert result is data
    
    def test_escape_identifier(self, adapter):
        """Test identifier escaping (no escaping needed)."""
        escaped = adapter.escape_identifier("field_name")
        assert escaped == "field_name"
    
    def test_get_limit_syntax(self, adapter):
        """Test LIMIT syntax."""
        syntax = adapter.get_limit_syntax(10, 20)
        assert "limit: 10" in syntax
        assert "skip: 20" in syntax
    
    def test_get_limit_syntax_no_offset(self, adapter):
        """Test LIMIT syntax without offset."""
        syntax = adapter.get_limit_syntax(10)
        assert "limit: 10" in syntax
        assert "skip" not in syntax
    
    def test_supports_feature(self, adapter):
        """Test feature support checking."""
        assert adapter.supports_feature("transactions") is True
        assert adapter.supports_feature("document_storage") is True
        assert adapter.supports_feature("flexible_schema") is True
        assert adapter.supports_feature("aggregation_pipeline") is True
        assert adapter.supports_feature("horizontal_scaling") is True
        assert adapter.supports_feature("nonexistent_feature") is False
    
    def test_get_database_type(self, adapter):
        """Test database type detection."""
        db_type = adapter.get_database_type()
        assert db_type == "mongodb"