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

# tests/test_performance_decorators.py

"""
Unit tests for performance tracking decorators.

Tests automatic performance tracking for sync/async functions,
stage decorators, and context managers.
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, MagicMock

from pulsepipe.pipelines.performance.decorators import (
    track_performance,
    track_async_performance,
    track_stage_performance,
    performance_context,
    _get_tracker_from_args,
    _serialize_args,
    _extract_counts
)
from pulsepipe.pipelines.performance.tracker import PerformanceTracker


class TestTrackPerformanceDecorator:
    """Test track_performance decorator for synchronous functions."""
    
    def test_no_tracker_available(self):
        """Test decorator when no tracker is available."""
        @track_performance()
        def test_function():
            return "result"
        
        result = test_function()
        assert result == "result"
    
    def test_basic_tracking(self):
        """Test basic function tracking with tracker."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_performance()
        def test_function(tracker=None):
            time.sleep(0.01)
            return [1, 2, 3]
        
        result = test_function(tracker=tracker)
        
        assert result == [1, 2, 3]
        assert len(tracker.get_step_history()) == 1
        step = tracker.get_step_history()[0]
        assert step.step_name == "test_function"
        assert step.records_processed == 3  # Length of result list
        assert step.success_count == 3
        assert step.duration_ms > 0
    
    def test_custom_step_name(self):
        """Test decorator with custom step name."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_performance(step_name="custom_step")
        def test_function(tracker=None):
            return "result"
        
        test_function(tracker=tracker)
        
        step = tracker.get_step_history()[0]
        assert step.step_name == "custom_step"
    
    def test_track_args(self):
        """Test tracking function arguments."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_performance(track_args=True)
        def test_function(a, b, tracker=None, c=None):
            return f"{a}-{b}-{c}"
        
        test_function("arg1", "arg2", tracker=tracker, c="kwarg1")
        
        step = tracker.get_step_history()[0]
        assert 'args' in step.metadata
        # The serialized args structure has args and kwargs keys
        # Since tracker is passed as kwarg, it shouldn't be in args
        assert step.metadata['args']['args'] == ["arg1", "arg2"]
        assert 'kwargs' in step.metadata['args']
        assert step.metadata['args']['kwargs']['c'] == "kwarg1"
    
    def test_track_result(self):
        """Test tracking function result."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_performance(track_result=True)
        def test_function(tracker=None):
            return [1, 2, 3, 4, 5]
        
        test_function(tracker=tracker)
        
        step = tracker.get_step_history()[0]
        assert step.metadata['result_type'] == 'list'
        assert step.metadata['result_size'] == 5
    
    def test_count_records_attribute(self):
        """Test counting records from result attribute."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        class MockResult:
            def __init__(self):
                self.record_count = 42
        
        @track_performance(count_records="record_count")
        def test_function(tracker=None):
            return MockResult()
        
        test_function(tracker=tracker)
        
        step = tracker.get_step_history()[0]
        assert step.records_processed == 42
        assert step.success_count == 42
    
    def test_count_bytes_attribute(self):
        """Test counting bytes from result attribute."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        class MockResult:
            def __init__(self):
                self.byte_size = 1024
        
        @track_performance(count_bytes="byte_size")
        def test_function(tracker=None):
            return MockResult()
        
        test_function(tracker=tracker)
        
        step = tracker.get_step_history()[0]
        assert step.bytes_processed == 1024
    
    def test_exception_handling(self):
        """Test tracking when function raises exception."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_performance()
        def failing_function(tracker=None):
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            failing_function(tracker=tracker)
        
        step = tracker.get_step_history()[0]
        assert step.failure_count == 1
        assert step.success_count == 0
        assert step.metadata['error'] == "Test error"
        assert step.metadata['error_type'] == "ValueError"
    
    def test_tracker_from_context(self):
        """Test getting tracker from context object."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        class MockContext:
            def __init__(self):
                self.performance_tracker = tracker
        
        @track_performance()
        def test_function(context):
            return "result"
        
        context = MockContext()
        test_function(context)
        
        assert len(tracker.get_step_history()) == 1


class TestTrackAsyncPerformanceDecorator:
    """Test track_async_performance decorator for asynchronous functions."""
    
    @pytest.mark.asyncio
    async def test_basic_async_tracking(self):
        """Test basic async function tracking."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_async_performance()
        async def async_function(tracker=None):
            await asyncio.sleep(0.01)
            return [1, 2, 3]
        
        result = await async_function(tracker=tracker)
        
        assert result == [1, 2, 3]
        assert len(tracker.get_step_history()) == 1
        step = tracker.get_step_history()[0]
        assert step.step_name == "async_function"
        assert step.metadata['async'] == True
        assert step.records_processed == 3
        assert step.duration_ms > 0
    
    @pytest.mark.asyncio
    async def test_async_exception_handling(self):
        """Test async tracking when function raises exception."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        @track_async_performance()
        async def failing_async_function(tracker=None):
            await asyncio.sleep(0.01)
            raise RuntimeError("Async error")
        
        with pytest.raises(RuntimeError, match="Async error"):
            await failing_async_function(tracker=tracker)
        
        step = tracker.get_step_history()[0]
        assert step.failure_count == 1
        assert step.metadata['error'] == "Async error"
        assert step.metadata['error_type'] == "RuntimeError"
    
    @pytest.mark.asyncio
    async def test_async_no_tracker(self):
        """Test async decorator when no tracker is available."""
        @track_async_performance()
        async def async_function():
            return "async_result"
        
        result = await async_function()
        assert result == "async_result"


class TestTrackStagePerformanceDecorator:
    """Test track_stage_performance decorator for pipeline stages."""
    
    def test_sync_stage_tracking(self):
        """Test tracking synchronous pipeline stage."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        class MockContext:
            def __init__(self):
                self.performance_tracker = tracker
        
        class MockStage:
            @track_stage_performance("test_stage")
            def process(self, context, data):
                return [1, 2, 3, 4]
        
        stage = MockStage()
        context = MockContext()
        result = stage.process(context, "input_data")
        
        assert result == [1, 2, 3, 4]
        assert len(tracker.get_step_history()) == 1
        step = tracker.get_step_history()[0]
        assert step.step_name == "test_stage"
        assert step.metadata['stage'] == "test_stage"
        assert step.metadata['stage_class'] == "MockStage"
        assert step.records_processed == 4
        assert step.success_count == 4
    
    @pytest.mark.asyncio
    async def test_async_stage_tracking(self):
        """Test tracking asynchronous pipeline stage."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        class MockContext:
            def __init__(self):
                self.performance_tracker = tracker
        
        class MockAsyncStage:
            @track_stage_performance("async_stage")
            async def process(self, context, data):
                await asyncio.sleep(0.01)
                return ["a", "b", "c"]
        
        stage = MockAsyncStage()
        context = MockContext()
        result = await stage.process(context, "input_data")
        
        assert result == ["a", "b", "c"]
        assert len(tracker.get_step_history()) == 1
        step = tracker.get_step_history()[0]
        assert step.step_name == "async_stage"
        assert step.records_processed == 3
        assert step.success_count == 3
    
    def test_stage_no_tracker(self):
        """Test stage decorator when no tracker is available."""
        class MockContext:
            pass
        
        class MockStage:
            @track_stage_performance("test_stage")
            def process(self, context, data):
                return "result"
        
        stage = MockStage()
        context = MockContext()
        result = stage.process(context, "input")
        
        assert result == "result"
    
    def test_stage_exception_handling(self):
        """Test stage tracking when stage raises exception."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        class MockContext:
            def __init__(self):
                self.performance_tracker = tracker
        
        class MockStage:
            @track_stage_performance("failing_stage")
            def process(self, context, data):
                raise ValueError("Stage failed")
        
        stage = MockStage()
        context = MockContext()
        
        with pytest.raises(ValueError, match="Stage failed"):
            stage.process(context, "input")
        
        step = tracker.get_step_history()[0]
        assert step.failure_count == 1
        assert step.success_count == 0
        assert step.metadata['error'] == "Stage failed"


class TestPerformanceContext:
    """Test performance_context context manager."""
    
    def test_basic_context(self):
        """Test basic context manager usage."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        with performance_context(tracker, "test_context") as ctx:
            ctx.update_progress(records=10, bytes_=100, successes=9, failures=1)
            time.sleep(0.01)
        
        assert len(tracker.get_step_history()) == 1
        step = tracker.get_step_history()[0]
        assert step.step_name == "test_context"
        assert step.records_processed == 10
        assert step.bytes_processed == 100
        assert step.success_count == 9
        assert step.failure_count == 1
        assert step.duration_ms > 0
    
    def test_context_with_metadata(self):
        """Test context manager with metadata."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        metadata = {"key": "value", "type": "test"}
        
        with performance_context(tracker, "test_context", metadata) as ctx:
            ctx.update_progress(records=5)
        
        step = tracker.get_step_history()[0]
        assert step.metadata["key"] == "value"
        assert step.metadata["type"] == "test"
    
    def test_context_exception_handling(self):
        """Test context manager when exception occurs."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        with pytest.raises(RuntimeError, match="Context error"):
            with performance_context(tracker, "failing_context") as ctx:
                ctx.update_progress(records=5, successes=5)
                raise RuntimeError("Context error")
        
        step = tracker.get_step_history()[0]
        assert step.records_processed == 5
        assert step.success_count == 5
        assert step.failure_count == 1  # Exception adds a failure
        assert step.metadata['error'] == "Context error"
        assert step.metadata['error_type'] == "RuntimeError"
    
    def test_multiple_progress_updates(self):
        """Test multiple progress updates within context."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        with performance_context(tracker, "multi_update_context") as ctx:
            ctx.update_progress(records=10, successes=10)
            ctx.update_progress(records=5, successes=4, failures=1)
            ctx.update_progress(bytes_=500)
        
        step = tracker.get_step_history()[0]
        assert step.records_processed == 15
        assert step.bytes_processed == 500
        assert step.success_count == 14
        assert step.failure_count == 1


class TestHelperFunctions:
    """Test helper functions for decorators."""
    
    def test_get_tracker_from_args_kwargs(self):
        """Test extracting tracker from kwargs."""
        tracker = PerformanceTracker("test", "test")
        
        # Test direct tracker in kwargs
        result = _get_tracker_from_args((), {'performance_tracker': tracker})
        assert result == tracker
        
        result = _get_tracker_from_args((), {'tracker': tracker})
        assert result == tracker
    
    def test_get_tracker_from_context(self):
        """Test extracting tracker from context object."""
        tracker = PerformanceTracker("test", "test")
        
        class MockContext:
            def __init__(self):
                self.performance_tracker = tracker
        
        context = MockContext()
        result = _get_tracker_from_args((), {'context': context})
        assert result == tracker
    
    def test_get_tracker_from_args_positional(self):
        """Test extracting tracker from positional arguments."""
        tracker = PerformanceTracker("test", "test")
        
        # Direct tracker in args
        result = _get_tracker_from_args((tracker,), {})
        assert result == tracker
        
        # Tracker in object
        class MockObject:
            def __init__(self):
                self.performance_tracker = tracker
        
        obj = MockObject()
        result = _get_tracker_from_args((obj,), {})
        assert result == tracker
    
    def test_get_tracker_none(self):
        """Test when no tracker is available."""
        result = _get_tracker_from_args(("string", 123), {'key': 'value'})
        assert result is None
    
    def test_serialize_args_basic(self):
        """Test basic argument serialization."""
        args = ("string", 123, True)
        kwargs = {"key": "value", "num": 42}
        
        result = _serialize_args(args, kwargs)
        
        assert result['args'] == ["string", 123, True]
        assert result['kwargs'] == {"key": "value", "num": 42}
    
    def test_serialize_args_complex_objects(self):
        """Test serialization with complex objects."""
        class MockObject:
            pass
        
        args = (MockObject(), "string")
        kwargs = {"obj": MockObject(), "list": [1, 2, 3]}
        
        result = _serialize_args(args, kwargs)
        
        assert result['args'][0] == "<MockObject>"
        assert result['args'][1] == "string"
        assert result['kwargs']['obj'] == "<MockObject>"
        assert result['kwargs']['list'] == [1, 2, 3]
    
    def test_serialize_args_limits(self):
        """Test serialization limits."""
        # Test arg limit (only first 3)
        args = (1, 2, 3, 4, 5)
        kwargs = {}
        
        result = _serialize_args(args, kwargs)
        assert len(result['args']) == 3
        assert result['args'] == [1, 2, 3]
        
        # Test kwarg limit (only first 5)
        kwargs = {f"key_{i}": i for i in range(10)}
        result = _serialize_args((), kwargs)
        assert len(result['kwargs']) == 5
    
    def test_serialize_args_large_objects(self):
        """Test serialization of large objects."""
        large_list = list(range(100))  # Large list that will exceed 200 char limit when str()
        kwargs = {"large": large_list}
        
        result = _serialize_args((), kwargs)
        assert result['kwargs']['large'] == "<list>"
    
    def test_serialize_args_exception(self):
        """Test serialization when exception occurs."""
        # Mock serialization to raise exception
        class BadObject:
            def __str__(self):
                raise Exception("Cannot serialize")
        
        # This should not crash but return error message
        result = _serialize_args((BadObject(),), {})
        # The function should handle this gracefully
        assert isinstance(result, dict)
    
    def test_extract_counts_list(self):
        """Test extracting counts from list result."""
        result = [1, 2, 3, 4, 5]
        records, bytes_, success = _extract_counts(result, None, None)
        
        assert records == 5
        assert bytes_ == 0
        assert success == 5
    
    def test_extract_counts_string(self):
        """Test extracting counts from string result."""
        result = "hello world"
        records, bytes_, success = _extract_counts(result, None, None)
        
        assert records == 11  # Length of string
        assert bytes_ == 11   # Bytes from string
        assert success == 11
    
    def test_extract_counts_attributes(self):
        """Test extracting counts from object attributes."""
        class MockResult:
            def __init__(self):
                self.record_count = 42
                self.byte_size = 1024
        
        result = MockResult()
        records, bytes_, success = _extract_counts(result, "record_count", "byte_size")
        
        assert records == 42
        assert bytes_ == 1024
        assert success == 42
    
    def test_extract_counts_dict(self):
        """Test extracting counts from dictionary result."""
        result = {"record_count": 100, "byte_size": 2048}
        records, bytes_, success = _extract_counts(result, "record_count", "byte_size")
        
        assert records == 100
        assert bytes_ == 2048
        assert success == 100
    
    def test_extract_counts_none(self):
        """Test extracting counts from None result."""
        records, bytes_, success = _extract_counts(None, None, None)
        
        assert records == 0
        assert bytes_ == 0
        assert success == 0
    
    def test_extract_counts_single_object(self):
        """Test extracting counts from single object."""
        class SingleObject:
            pass
        
        result = SingleObject()
        records, bytes_, success = _extract_counts(result, None, None)
        
        assert records == 1
        assert bytes_ == 0
        assert success == 1
    
    def test_extract_counts_invalid_attributes(self):
        """Test extracting counts with invalid attributes."""
        result = {"key": "value"}
        records, bytes_, success = _extract_counts(result, "nonexistent", "also_nonexistent")
        
        # When specific attribute names are provided but don't exist,
        # the function should return 0 (not fall back to len())
        assert records == 0  # Attribute doesn't exist
        assert bytes_ == 0   # Attribute doesn't exist
        assert success == 0  # Same as records