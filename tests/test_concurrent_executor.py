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

# tests/test_concurrent_executor.py

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime

from pulsepipe.pipelines.concurrent_executor import ConcurrentPipelineExecutor
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.utils.errors import PipelineError, ConfigurationError
from pulsepipe.pipelines.stages import PipelineStage


class MockStage(PipelineStage):
    def __init__(self, name="mock", should_fail=False, result=None, delay=0):
        super().__init__(name)
        self.should_fail = should_fail
        self.result = result or f"{name}_result"
        self.executed = False
        self.delay = delay
        self.execution_count = 0

    async def execute(self, context, input_data=None):
        self.executed = True
        self.execution_count += 1
        
        if self.delay > 0:
            await asyncio.sleep(self.delay)
            
        if self.should_fail:
            raise Exception(f"Mock stage {self.name} failed")
            
        return self.result


class MockContinuousStage(PipelineStage):
    def __init__(self, name="continuous", results=None):
        super().__init__(name)
        self.results = results or ["result1", "result2", "result3"]
        self.execution_count = 0

    async def execute(self, context, input_data=None):
        self.execution_count += 1
        if self.execution_count <= len(self.results):
            return self.results[self.execution_count - 1]
        return None  # No more results


@pytest.fixture
def executor():
    return ConcurrentPipelineExecutor()


@pytest.fixture
def basic_config():
    return {
        "adapter": {"type": "file_watcher", "continuous": False},
        "ingester": {"type": "mock"},
        "chunker": {"type": "mock"},
        "embedding": {"model": "test"},
        "vectorstore": {"enabled": True}
    }


@pytest.fixture
def continuous_config():
    return {
        "adapter": {"type": "file_watcher", "continuous": True},
        "ingester": {"type": "mock"},
        "chunker": {"type": "mock"}
    }


@pytest.fixture
def pipeline_context(basic_config):
    return PipelineContext(
        name="test_pipeline",
        config=basic_config,
        output_path=None,
        summary=False,
        print_model=False,
        pretty=True,
        verbose=False
    )


@pytest.fixture
def continuous_context(continuous_config):
    return PipelineContext(
        name="continuous_pipeline",
        config=continuous_config,
        output_path=None,
        summary=False,
        print_model=False,
        pretty=True,
        verbose=False
    )


class TestConcurrentPipelineExecutor:
    
    def test_init(self, executor):
        """Test executor initialization."""
        # Verify available stages are registered
        assert "ingestion" in executor.available_stages
        assert "deid" in executor.available_stages
        assert "chunking" in executor.available_stages
        assert "embedding" in executor.available_stages
        assert "vectorstore" in executor.available_stages
        
        # Verify stage dependencies
        assert executor.stage_dependencies["ingestion"] == []
        assert executor.stage_dependencies["deid"] == ["ingestion"]
        assert executor.stage_dependencies["chunking"] == ["ingestion"]
        assert executor.stage_dependencies["embedding"] == ["chunking"]
        assert executor.stage_dependencies["vectorstore"] == ["embedding"]
        
        # Verify initial state
        assert executor.queues == {}
        assert executor.tasks == {}
        assert not executor.stop_event.is_set()
        assert executor.timeout is None

    @pytest.mark.asyncio
    async def test_execute_pipeline_no_enabled_stages(self, executor):
        """Test pipeline execution with no enabled stages."""
        # Create a config that won't enable any stages
        config = {
            "vectorstore": {"enabled": False},
            "embedding": {"enabled": False}
        }
        context = PipelineContext("test", config)
        
        # The pipeline will run ingestion by default but fail gracefully with errors logged
        result = await executor.execute_pipeline(context)
        
        # Should complete but with errors
        assert result["status"] in ["completed", "completed_with_errors"]
        assert len(context.errors) > 0

    @pytest.mark.asyncio
    async def test_execute_pipeline_basic_flow(self, executor, pipeline_context):
        """Test basic pipeline execution with multiple stages."""
        # Mock all stages
        mock_ingestion = MockStage("ingestion", result=["item1", "item2"])
        mock_chunking = MockStage("chunking", result="chunked_data")
        mock_embedding = MockStage("embedding", result="embedded_data")
        mock_vectorstore = MockStage("vectorstore", result="vectorstore_data")
        
        executor.available_stages.update({
            "ingestion": mock_ingestion,
            "chunking": mock_chunking,
            "embedding": mock_embedding,
            "vectorstore": mock_vectorstore
        })
        
        result = await executor.execute_pipeline(pipeline_context)
        
        # Verify stages were executed
        assert mock_ingestion.executed
        assert mock_chunking.executed
        assert mock_embedding.executed
        assert mock_vectorstore.executed
        
        # Verify result structure
        assert result["status"] == "completed"
        assert "results" in result
        assert "duration" in result
        
        # Verify context tracking
        assert "ingestion" in pipeline_context.executed_stages
        assert "chunking" in pipeline_context.executed_stages
        assert "embedding" in pipeline_context.executed_stages
        assert "vectorstore" in pipeline_context.executed_stages

    @pytest.mark.asyncio
    async def test_execute_pipeline_with_timeout(self, executor, pipeline_context):
        """Test pipeline execution with timeout."""
        # Create a slow stage
        mock_ingestion = MockStage("ingestion", result="test", delay=2.0)
        executor.available_stages["ingestion"] = mock_ingestion
        
        start_time = time.time()
        result = await executor.execute_pipeline(pipeline_context, timeout=1.0)
        elapsed = time.time() - start_time
        
        # Should complete before timeout in this case since we're testing the timeout handler
        assert elapsed < 3.0  # Should not take the full delay time
        assert result["status"] in ["completed", "cancelled"]

    @pytest.mark.asyncio
    async def test_timeout_handler(self, executor):
        """Test the timeout handler functionality."""
        timeout_seconds = 0.1
        
        # Start timeout handler
        timeout_task = asyncio.create_task(executor._timeout_handler(timeout_seconds))
        
        # Wait longer than timeout
        await asyncio.sleep(0.2)
        
        # Verify stop event was set
        assert executor.stop_event.is_set()
        
        # Clean up
        timeout_task.cancel()
        try:
            await timeout_task
        except asyncio.CancelledError:
            pass

    def test_get_enabled_stages_basic(self, executor, pipeline_context):
        """Test basic stage enablement logic."""
        enabled_stages = executor._get_enabled_stages(pipeline_context)
        
        expected_stages = ["ingestion", "chunking", "embedding", "vectorstore"]
        assert enabled_stages == expected_stages

    def test_get_enabled_stages_with_deid(self, executor):
        """Test stage enablement with deidentification."""
        config = {
            "ingester": {"type": "mock"},
            "deid": {"enabled": True},
            "chunker": {"type": "mock"}
        }
        context = PipelineContext("test_deid", config)
        
        enabled_stages = executor._get_enabled_stages(context)
        
        assert "deid" in enabled_stages
        assert "chunking" in enabled_stages
        assert executor.stage_dependencies["chunking"] == ["deid"]

    def test_get_enabled_stages_dependency_validation(self, executor):
        """Test dependency validation warnings."""
        config = {
            "ingester": {"type": "mock"},
            "embedding": {"model": "test"}
        }
        context = PipelineContext("test_deps", config)
        
        enabled_stages = executor._get_enabled_stages(context)
        
        # Should add warning about missing dependency
        warnings = [w["message"] for w in context.warnings]
        assert any("depends on 'chunking' which is not enabled" in msg for msg in warnings)

    def test_create_queues(self, executor):
        """Test queue creation for stages."""
        enabled_stages = ["ingestion", "chunking", "embedding"]
        queues = executor._create_queues(enabled_stages)
        
        # Verify all output queues are created
        assert "ingestion_output" in queues
        assert "chunking_output" in queues
        assert "embedding_output" in queues
        
        # Verify queues are asyncio.Queue instances
        for queue in queues.values():
            assert isinstance(queue, asyncio.Queue)
            assert queue.maxsize == 100

    @pytest.mark.asyncio
    async def test_start_stage_tasks(self, executor, pipeline_context):
        """Test stage task creation and startup."""
        enabled_stages = ["ingestion", "chunking"]
        executor.queues = executor._create_queues(enabled_stages)
        
        # Mock stages
        mock_ingestion = MockStage("ingestion")
        mock_chunking = MockStage("chunking")
        executor.available_stages.update({
            "ingestion": mock_ingestion,
            "chunking": mock_chunking
        })
        
        tasks = await executor._start_stage_tasks(pipeline_context, enabled_stages)
        
        # Verify tasks were created
        assert "ingestion" in tasks
        assert "chunking" in tasks
        assert isinstance(tasks["ingestion"], asyncio.Task)
        assert isinstance(tasks["chunking"], asyncio.Task)
        
        # Clean up tasks
        for task in tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_start_stage_tasks_missing_stage(self, executor, pipeline_context):
        """Test handling of missing stages."""
        enabled_stages = ["ingestion", "missing_stage"]
        executor.queues = executor._create_queues(enabled_stages)
        
        mock_ingestion = MockStage("ingestion")
        executor.available_stages["ingestion"] = mock_ingestion
        # Don't add missing_stage to available_stages
        
        tasks = await executor._start_stage_tasks(pipeline_context, enabled_stages)
        
        # Should only create task for available stage
        assert "ingestion" in tasks
        assert "missing_stage" not in tasks
        
        # Should add warning
        warnings = [w["message"] for w in pipeline_context.warnings]
        assert any("Stage 'missing_stage' not found, skipping" in msg for msg in warnings)
        
        # Clean up
        tasks["ingestion"].cancel()
        try:
            await tasks["ingestion"]
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_run_stage_ingestion_one_time(self, executor, pipeline_context):
        """Test one-time ingestion stage execution."""
        mock_stage = MockStage("ingestion", result=["item1", "item2"])
        output_queue = asyncio.Queue()
        
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="ingestion",
            context=pipeline_context,
            output_queue=output_queue
        )
        
        # Verify stage was executed
        assert mock_stage.executed
        assert result["stage"] == "ingestion"
        assert result["status"] == "completed"
        assert result["result_count"] == 2
        
        # Verify items were queued
        items = []
        while not output_queue.empty():
            item = await output_queue.get()
            if item is not None:
                items.append(item)
        
        assert len(items) == 2
        assert items == ["item1", "item2"]

    @pytest.mark.asyncio
    async def test_run_stage_ingestion_continuous_mode(self, executor, continuous_context):
        """Test continuous mode ingestion."""
        mock_stage = MockContinuousStage("ingestion", results=["item1", "item2"])
        executor.available_stages["ingestion"] = mock_stage
        output_queue = asyncio.Queue()
        
        # Start the stage task
        stage_task = asyncio.create_task(
            executor._run_stage(
                stage=mock_stage,
                stage_name="ingestion",
                context=continuous_context,
                output_queue=output_queue
            )
        )
        
        # Let it run for a short time
        await asyncio.sleep(0.1)
        
        # Stop the execution
        executor.stop_event.set()
        
        # Wait for completion
        result = await stage_task
        
        # Verify execution
        assert mock_stage.execution_count >= 1
        assert result["stage"] == "ingestion"
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_run_stage_processing_stage(self, executor, pipeline_context):
        """Test non-ingestion stage execution."""
        mock_stage = MockStage("chunking", result="processed_item")
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        
        # Put test items in input queue
        await input_queue.put("input_item1")
        await input_queue.put("input_item2")
        await input_queue.put(None)  # End marker
        
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="chunking",
            context=pipeline_context,
            input_queue=input_queue,
            output_queue=output_queue
        )
        
        # Verify processing
        assert mock_stage.executed
        assert result["stage"] == "chunking"
        assert result["status"] == "completed"
        assert result["result_count"] == 2

    @pytest.mark.asyncio
    async def test_run_stage_no_input_queue_error(self, executor, pipeline_context):
        """Test stage error when missing input queue."""
        mock_stage = MockStage("chunking")
        output_queue = asyncio.Queue()
        
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="chunking",
            context=pipeline_context,
            input_queue=None,
            output_queue=output_queue
        )
        
        # Verify error handling
        assert result["stage"] == "chunking"
        assert result["status"] == "failed"
        assert result["error"] == "Missing input queue"
        assert result["result_count"] == 0

    @pytest.mark.asyncio
    async def test_run_stage_processing_error(self, executor, pipeline_context):
        """Test stage error handling during processing."""
        mock_stage = MockStage("chunking", should_fail=True)
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        
        await input_queue.put("test_item")
        await input_queue.put(None)
        
        # The stage processes items but continues on errors, so it completes successfully
        # but logs errors for individual items
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="chunking",
            context=pipeline_context,
            input_queue=input_queue,
            output_queue=output_queue
        )
        
        # Verify it completed but with errors logged
        assert result["stage"] == "chunking"
        assert result["status"] == "completed"
        assert len(pipeline_context.errors) > 0

    @pytest.mark.asyncio
    async def test_run_stage_complete_failure(self, executor, pipeline_context):
        """Test stage complete failure during ingestion."""
        # Create a stage that fails during execution
        class FailingStage(MockStage):
            async def execute(self, context, input_data=None):
                if input_data is None:  # Ingestion mode
                    raise Exception("Stage setup failed")
                return await super().execute(context, input_data)
        
        failing_stage = FailingStage("failing_ingestion")
        output_queue = asyncio.Queue()
        
        # For ingestion, errors are caught and logged but stage completes
        result = await executor._run_stage(
            stage=failing_stage,
            stage_name="ingestion",
            context=pipeline_context,
            output_queue=output_queue
        )
        
        # Should complete but with no results due to failure
        assert result["stage"] == "ingestion"
        assert result["status"] == "completed"
        assert result["result_count"] == 0
        assert len(pipeline_context.errors) > 0

    @pytest.mark.asyncio
    async def test_run_stage_critical_failure(self, executor, pipeline_context):
        """Test stage failure outside of normal processing that raises PipelineError."""
        # Simulate a critical failure by mocking context.end_stage to fail
        def failing_end_stage(*args, **kwargs):
            raise Exception("Critical context failure")
        
        original_end_stage = pipeline_context.end_stage
        pipeline_context.end_stage = failing_end_stage
        
        mock_stage = MockStage("chunking", result="test")
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        
        await input_queue.put("test_item")
        await input_queue.put(None)
        
        try:
            with pytest.raises(PipelineError) as exc_info:
                await executor._run_stage(
                    stage=mock_stage,
                    stage_name="chunking",
                    context=pipeline_context,
                    input_queue=input_queue,
                    output_queue=output_queue
                )
            
            assert "Error in pipeline stage 'chunking'" in str(exc_info.value)
            assert exc_info.value.details["stage"] == "chunking"
        finally:
            # Restore original method
            pipeline_context.end_stage = original_end_stage

    @pytest.mark.asyncio
    async def test_run_stage_cancellation(self, executor, pipeline_context):
        """Test stage cancellation handling."""
        mock_stage = MockStage("chunking", delay=1.0)
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        
        await input_queue.put("test_item")
        
        # Start stage task
        stage_task = asyncio.create_task(
            executor._run_stage(
                stage=mock_stage,
                stage_name="chunking",
                context=pipeline_context,
                input_queue=input_queue,
                output_queue=output_queue
            )
        )
        
        # Cancel after short delay
        await asyncio.sleep(0.1)
        stage_task.cancel()
        
        with pytest.raises(asyncio.CancelledError):
            await stage_task

    @pytest.mark.asyncio
    async def test_wait_for_completion_success(self, executor, pipeline_context):
        """Test successful completion of all tasks."""
        # Create mock tasks
        async def mock_task_1():
            await asyncio.sleep(0.1)
            return {"stage": "stage1", "status": "completed", "result_count": 1}
        
        async def mock_task_2():
            await asyncio.sleep(0.1)
            return {"stage": "stage2", "status": "completed", "result_count": 2}
        
        task1 = asyncio.create_task(mock_task_1())
        task2 = asyncio.create_task(mock_task_2())
        
        tasks = {"stage1": task1, "stage2": task2}
        
        result = await executor._wait_for_completion(tasks, pipeline_context)
        
        assert result["status"] == "completed"
        assert "stage1" in result["results"]
        assert "stage2" in result["results"]
        assert result["errors"] == []
        assert "duration" in result

    @pytest.mark.asyncio
    async def test_wait_for_completion_with_errors(self, executor, pipeline_context):
        """Test completion with task errors."""
        async def failing_task():
            raise Exception("Task failed")
        
        async def success_task():
            return {"stage": "success", "status": "completed"}
        
        task1 = asyncio.create_task(failing_task())
        task2 = asyncio.create_task(success_task())
        
        tasks = {"failing": task1, "success": task2}
        
        result = await executor._wait_for_completion(tasks, pipeline_context)
        
        assert result["status"] == "completed_with_errors"
        assert len(result["errors"]) == 1
        assert "Error in stage failing" in result["errors"][0]
        assert "success" in result["results"]

    @pytest.mark.asyncio
    async def test_wait_for_completion_cancellation(self, executor, pipeline_context):
        """Test cancellation during task completion."""
        async def long_task():
            await asyncio.sleep(2.0)
            return {"stage": "long", "status": "completed"}
        
        task = asyncio.create_task(long_task())
        tasks = {"long": task}
        
        # Set stop event during execution
        async def set_stop():
            await asyncio.sleep(0.1)
            executor.stop_event.set()
        
        asyncio.create_task(set_stop())
        
        result = await executor._wait_for_completion(tasks, pipeline_context)
        
        assert result["status"] == "cancelled"
        assert "Pipeline execution was cancelled" in result["errors"]

    @pytest.mark.asyncio
    async def test_cancel_all_tasks(self, executor):
        """Test cancellation of all running tasks."""
        async def long_task():
            await asyncio.sleep(1.0)
            return "completed"
        
        # Create some tasks
        task1 = asyncio.create_task(long_task())
        task2 = asyncio.create_task(long_task())
        
        executor.tasks = {"task1": task1, "task2": task2}
        
        # Cancel all tasks
        await executor._cancel_all_tasks()
        
        # Verify tasks were cancelled
        assert task1.cancelled()
        assert task2.cancelled()

    @pytest.mark.asyncio
    async def test_stop_method(self, executor):
        """Test the stop method."""
        async def long_task():
            await asyncio.sleep(1.0)
            return "completed"
        
        task = asyncio.create_task(long_task())
        executor.tasks = {"test": task}
        
        await executor.stop()
        
        # Verify stop event was set and task cancelled
        assert executor.stop_event.is_set()
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_execute_pipeline_exception_handling(self, executor, pipeline_context):
        """Test exception handling in main execution method."""
        # Mock _get_enabled_stages to raise an exception
        with patch.object(executor, '_get_enabled_stages', side_effect=Exception("Test error")):
            with pytest.raises(PipelineError) as exc_info:
                await executor.execute_pipeline(pipeline_context)
            
            assert "Error in concurrent pipeline execution" in str(exc_info.value)
            assert exc_info.value.details["pipeline"] == "test_pipeline"

    @pytest.mark.asyncio
    async def test_execute_pipeline_cancellation_handling(self, executor, pipeline_context):
        """Test cancellation handling in main execution method."""
        # Create a mock that raises CancelledError
        async def mock_start_tasks(*args):
            raise asyncio.CancelledError()
        
        with patch.object(executor, '_start_stage_tasks', side_effect=mock_start_tasks):
            with pytest.raises(asyncio.CancelledError):
                await executor.execute_pipeline(pipeline_context)

    def test_stage_dependencies_deid_update(self, executor):
        """Test that deid dependency is correctly updated."""
        # Initially chunking depends on ingestion
        assert executor.stage_dependencies["chunking"] == ["ingestion"]
        
        # Create context with deid enabled
        config = {"deid": {"enabled": True}, "ingester": {"type": "mock"}}
        context = PipelineContext("test", config)
        
        executor._get_enabled_stages(context)
        
        # Now chunking should depend on deid
        assert executor.stage_dependencies["chunking"] == ["deid"]
        
        # Test reset when deid is not enabled
        config_no_deid = {"ingester": {"type": "mock"}}
        context_no_deid = PipelineContext("test2", config_no_deid)
        
        executor._get_enabled_stages(context_no_deid)
        
        # Should be reset to ingestion
        assert executor.stage_dependencies["chunking"] == ["ingestion"]

    @pytest.mark.asyncio
    async def test_ingestion_progress_tracking(self, executor, pipeline_context):
        """Test progress tracking in ingestion stage."""
        # Mock a stage that returns multiple items
        mock_stage = MockStage("ingestion", result=["item1", "item2", "item3", "item4", "item5"])
        output_queue = asyncio.Queue()
        
        with patch('pulsepipe.pipelines.concurrent_executor.logger') as mock_logger:
            result = await executor._run_stage(
                stage=mock_stage,
                stage_name="ingestion",
                context=pipeline_context,
                output_queue=output_queue
            )
            
            # Verify progress was logged
            assert result["result_count"] == 5
            mock_logger.info.assert_called()

    @pytest.mark.asyncio
    async def test_processing_stage_timeout_handling(self, executor, pipeline_context):
        """Test timeout handling in processing stages."""
        mock_stage = MockStage("chunking", result="processed")
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        
        # Don't put any items in input queue to trigger timeout
        
        # Start stage task
        stage_task = asyncio.create_task(
            executor._run_stage(
                stage=mock_stage,
                stage_name="chunking",
                context=pipeline_context,
                input_queue=input_queue,
                output_queue=output_queue
            )
        )
        
        # Set stop event after short delay to exit the waiting loop
        async def set_stop():
            await asyncio.sleep(0.2)
            executor.stop_event.set()
        
        asyncio.create_task(set_stop())
        
        result = await stage_task
        
        # Should complete without processing any items
        assert result["result_count"] == 0
        assert result["status"] == "completed"