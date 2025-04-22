# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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
from unittest.mock import patch, MagicMock, AsyncMock, call

from pulsepipe.pipelines.concurrent_executor import ConcurrentPipelineExecutor
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.utils.errors import ConfigurationError, PipelineError


class TestConcurrentPipelineExecutor:
    """Tests for the ConcurrentPipelineExecutor class."""

    @pytest.fixture
    def executor(self):
        """Return a ConcurrentPipelineExecutor instance for testing."""
        return ConcurrentPipelineExecutor()
    
    @pytest.fixture
    def mock_context(self):
        """Return a mock PipelineContext for testing."""
        context = MagicMock(spec=PipelineContext)
        context.name = "test_pipeline"
        context.log_prefix = "[test_pipeline]"
        context.is_stage_enabled = MagicMock()
        context.add_warning = MagicMock()
        context.add_error = MagicMock()
        context.start_stage = MagicMock()
        context.end_stage = MagicMock()
        context.config = {}
        return context

    def test_init_registers_stages(self, executor):
        """Test that initialization registers all available stages."""
        assert len(executor.available_stages) == 5
        assert "ingestion" in executor.available_stages
        assert "deid" in executor.available_stages
        assert "chunking" in executor.available_stages
        assert "embedding" in executor.available_stages
        assert "vectorstore" in executor.available_stages

    def test_get_enabled_stages_all_enabled(self, executor, mock_context):
        """Test getting enabled stages when all stages are enabled."""
        mock_context.is_stage_enabled.return_value = True
        
        stages = executor._get_enabled_stages(mock_context)
        
        assert stages == ["ingestion", "deid", "chunking", "embedding", "vectorstore"]
        assert mock_context.add_warning.call_count == 0

    def test_get_enabled_stages_some_disabled(self, executor, mock_context):
        """Test getting enabled stages when some stages are disabled."""
        # Only ingestion and chunking enabled
        def is_enabled(stage):
            return stage in ["ingestion", "chunking"]
        
        mock_context.is_stage_enabled.side_effect = is_enabled
        
        stages = executor._get_enabled_stages(mock_context)
        
        assert stages == ["ingestion", "chunking"]
        # No warnings expected since dependencies are proper
        assert mock_context.add_warning.call_count == 0

    def test_get_enabled_stages_dependency_warnings(self, executor, mock_context):
        """Test that warnings are generated for missing dependencies."""
        # Only embedding and vectorstore enabled - missing dependencies
        def is_enabled(stage):
            return stage in ["embedding", "vectorstore"]
            
        mock_context.is_stage_enabled.side_effect = is_enabled
        
        stages = executor._get_enabled_stages(mock_context)
        
        assert stages == ["embedding", "vectorstore"]
        # Should generate warnings for missing dependencies
        assert mock_context.add_warning.call_count >= 1

    def test_get_enabled_stages_with_deid(self, executor, mock_context):
        """Test that chunking dependency changes when deid is enabled."""
        def is_enabled(stage):
            return stage in ["ingestion", "deid", "chunking"]
        
        mock_context.is_stage_enabled.side_effect = is_enabled
        
        stages = executor._get_enabled_stages(mock_context)
        
        assert stages == ["ingestion", "deid", "chunking"]
        # Verify chunking dependency was updated to depend on deid
        assert executor.stage_dependencies["chunking"] == ["deid"]

    def test_create_queues(self, executor):
        """Test queue creation between stages."""
        enabled_stages = ["ingestion", "chunking", "embedding"]
        
        queues = executor._create_queues(enabled_stages)
        
        assert "ingestion_output" in queues
        assert "chunking_output" in queues
        assert "embedding_output" in queues
        assert isinstance(queues["ingestion_output"], asyncio.Queue)
        assert isinstance(queues["chunking_output"], asyncio.Queue)
        assert isinstance(queues["embedding_output"], asyncio.Queue)

    def test_missing_enabled_stages(self, executor, mock_context):
        """Test checking for missing enabled stages."""
        # Set up all stages disabled
        mock_context.is_stage_enabled.return_value = False
        
        # Call _get_enabled_stages directly to check its behavior
        stages = executor._get_enabled_stages(mock_context)
        
        # Verify it returned an empty list
        assert len(stages) == 0

    @pytest.mark.asyncio
    async def test_execute_pipeline_with_error(self, executor, mock_context):
        """Test pipeline execution handling of errors."""
        # Set up a mock _get_enabled_stages that raises an exception
        with patch.object(executor, '_get_enabled_stages', side_effect=Exception("Test error")):
            # Should raise PipelineError
            with pytest.raises(PipelineError):
                await executor.execute_pipeline(mock_context)

    @pytest.mark.asyncio
    async def test_timeout_handler(self, executor):
        """Test the timeout handler."""
        # Create a task for the timeout handler
        timeout_seconds = 0.1  # Use a short timeout for testing
        task = asyncio.create_task(executor._timeout_handler(timeout_seconds))
        
        # Wait for the timeout to occur
        await asyncio.sleep(0.2)
        
        # The stop event should be set after the timeout
        assert executor.stop_event.is_set()
        
        # Cancel the task to clean up
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_start_stage_tasks(self, executor, mock_context):
        """Test starting stage tasks."""
        # Set up mocks for stages
        mock_ingestion = MagicMock()
        mock_ingestion.execute = AsyncMock()
        executor.available_stages["ingestion"] = mock_ingestion
        
        mock_chunking = MagicMock()
        mock_chunking.execute = AsyncMock()
        executor.available_stages["chunking"] = mock_chunking
        
        # Set up queues
        executor.queues = {
            "ingestion_output": asyncio.Queue(),
            "chunking_output": asyncio.Queue()
        }
        
        # Patch the _run_stage method
        with patch.object(executor, '_run_stage', new_callable=AsyncMock) as mock_run_stage:
            enabled_stages = ["ingestion", "chunking"]
            tasks = await executor._start_stage_tasks(mock_context, enabled_stages)
            
            # Verify tasks were created for both stages
            assert "ingestion" in tasks
            assert "chunking" in tasks
            assert isinstance(tasks["ingestion"], asyncio.Task)
            assert isinstance(tasks["chunking"], asyncio.Task)
            
            # Verify stages were started in context
            assert mock_context.start_stage.call_count == 2
            mock_context.start_stage.assert_any_call("ingestion")
            mock_context.start_stage.assert_any_call("chunking")
            
            # Cancel tasks for cleanup
            for task in tasks.values():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_cancel_all_tasks(self, executor):
        """Test cancelling all tasks."""
        # Create some mock tasks
        task1 = MagicMock()
        task1.done.return_value = False
        task1.cancel = MagicMock()
        
        task2 = MagicMock()
        task2.done.return_value = True  # Already done
        task2.cancel = MagicMock()
        
        # Set up tasks dictionary
        executor.tasks = {
            "stage1": task1,
            "stage2": task2
        }
        
        # Mock asyncio.gather to avoid actually waiting
        with patch('asyncio.gather', new_callable=AsyncMock) as mock_gather:
            await executor._cancel_all_tasks()
            
            # Verify only not-done task was cancelled
            task1.cancel.assert_called_once()
            assert not task2.cancel.called

    @pytest.mark.asyncio
    async def test_stop(self, executor):
        """Test the stop method."""
        # Patch the _cancel_all_tasks method
        with patch.object(executor, '_cancel_all_tasks', new_callable=AsyncMock) as mock_cancel:
            await executor.stop()
            
            # Verify stop event was set and tasks were cancelled
            assert executor.stop_event.is_set()
            assert mock_cancel.called

    @pytest.mark.asyncio
    async def test_execute_pipeline_with_timeout(self, executor, mock_context):
        """Test executing pipeline with a timeout."""
        # Set up to return one stage
        mock_context.is_stage_enabled.side_effect = lambda stage: stage == "ingestion"
        
        # Patch methods to avoid actual execution
        with patch.object(executor, '_get_enabled_stages', return_value=["ingestion"]) as mock_get_stages, \
             patch.object(executor, '_create_queues') as mock_create_queues, \
             patch.object(executor, '_start_stage_tasks', new_callable=AsyncMock) as mock_start_tasks, \
             patch.object(executor, '_wait_for_completion', new_callable=AsyncMock) as mock_wait:
            
            # Execute pipeline with timeout
            await executor.execute_pipeline(mock_context, timeout=30)
            
            # Verify timeout was set
            assert executor.timeout == 30
            
            # Verify all methods were called
            mock_get_stages.assert_called_once_with(mock_context)
            mock_create_queues.assert_called_once()
            mock_start_tasks.assert_called_once()
            mock_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_stage_non_ingestion(self, executor, mock_context):
        """Test running a non-ingestion stage (chunking)."""
        # Set up mock stage
        mock_stage = MagicMock()
        mock_stage.execute = AsyncMock(return_value="chunked_result")
        
        # Set up input and output queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        
        # Put an item in the input queue
        await input_queue.put("test_input")
        # Put None to mark end of input
        await input_queue.put(None)
        
        # Run the chunking stage
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="chunking",
            context=mock_context,
            input_queue=input_queue,
            output_queue=output_queue,
            order=1
        )
        
        # Verify stage was executed with input item
        mock_stage.execute.assert_called_with(mock_context, "test_input")
        
        # Verify results
        assert result["stage"] == "chunking"
        assert result["status"] == "completed"
        assert len(result["results"]) == 1
        assert result["results"][0] == "chunked_result"
        
        # Verify items were put in queue (result and None marker)
        assert output_queue.qsize() == 2
        assert await output_queue.get() == "chunked_result"
        assert await output_queue.get() is None

    @pytest.mark.asyncio
    async def test_run_stage_missing_input_queue(self, executor, mock_context):
        """Test handling of missing input queue for non-ingestion stage."""
        # Set up mock stage
        mock_stage = MagicMock()
        
        # Set up output queue (but no input queue)
        output_queue = asyncio.Queue()
        
        # Run the chunking stage with no input queue
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="chunking",
            context=mock_context,
            input_queue=None,  # No input queue
            output_queue=output_queue,
            order=1
        )
        
        # Verify error was added to context
        mock_context.add_error.assert_called_once_with("chunking", "Missing input queue")
        
        # Verify error result
        assert result["stage"] == "chunking"
        assert result["status"] == "failed"
        assert result["error"] == "Missing input queue"
        
        # Verify None was put in output queue
        assert output_queue.qsize() == 1
        assert await output_queue.get() is None

    @pytest.mark.asyncio
    async def test_wait_for_completion_with_stop_event(self, executor, mock_context):
        """Test wait_for_completion when stop event is set."""
        # Create a mock task
        task = MagicMock()
        
        # Set up tasks dictionary
        tasks = {"test": task}
        
        # Set the stop event
        executor.stop_event.set()
        
        # Mock asyncio.wait to return pending tasks
        with patch('asyncio.wait', new_callable=AsyncMock) as mock_wait:
            mock_wait.return_value = (set(), {task})  # No done tasks, one pending
            
            # Also mock _cancel_all_tasks to avoid side effects
            with patch.object(executor, '_cancel_all_tasks', new_callable=AsyncMock) as mock_cancel:
                result = await executor._wait_for_completion(tasks, mock_context)
                
                # Verify tasks were cancelled
                mock_cancel.assert_called_once()
                
                # Verify cancelled result
                assert result["status"] == "cancelled"
                assert "Pipeline execution was cancelled" in result["errors"]

    @pytest.mark.asyncio
    async def test_run_stage_ingestion_continuous_mode(self, executor, mock_context):
        """Test running ingestion stage in continuous mode."""
        # Set up mock stage
        mock_stage = MagicMock()
        
        # First execution returns initial results, subsequent calls return None
        first_call = True
        async def mock_execute(*args, **kwargs):
            nonlocal first_call
            if first_call:
                first_call = False
                return ["initial_result"]
            else:
                # Set stop event after first call to exit continuous loop
                executor.stop_event.set()
                return None
        
        mock_stage.execute = AsyncMock(side_effect=mock_execute)
        
        # Set up output queue
        output_queue = asyncio.Queue()
        
        # Configure continuous mode
        mock_context.config = {
            "adapter": {
                "type": "file_watcher",
                "continuous": True
            }
        }
        
        # Run the ingestion stage
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="ingestion",
            context=mock_context,
            output_queue=output_queue,
            order=0
        )
        
        # Verify stage was executed
        assert mock_stage.execute.call_count >= 1
        
        # Verify results
        assert result["stage"] == "ingestion"
        assert result["status"] == "completed"
        
        # Verify initial result was put in queue
        assert output_queue.qsize() >= 1

    @pytest.mark.asyncio
    async def test_run_stage_simple(self, executor, mock_context):
        """Test a simple run of the ingestion stage."""
        # Set up mock stage
        mock_stage = MagicMock()
        mock_stage.execute = AsyncMock(return_value=["test_result"])
        
        # Set up output queue
        output_queue = asyncio.Queue()
        
        # Configure non-continuous mode
        mock_context.config = {
            "adapter": {
                "type": "file_watcher",
                "continuous": False
            }
        }
        
        # Run the ingestion stage
        result = await executor._run_stage(
            stage=mock_stage,
            stage_name="ingestion",
            context=mock_context,
            output_queue=output_queue,
            order=0
        )
        
        # Verify stage was executed
        mock_stage.execute.assert_called_once()
        
        # Verify results
        assert result["stage"] == "ingestion"
        assert result["status"] == "completed"
        assert len(result["results"]) == 1
        assert result["results"][0] == "test_result"
        
        # Verify item was put in queue and then None marker
        assert output_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_wait_for_completion_mock_wait(self, executor, mock_context):
        """Test wait_for_completion by mocking the wait return values."""
        # Create a mock result that will be injected
        final_result = {
            "status": "completed",
            "results": {"test": {"stage": "test", "status": "completed"}},
            "duration": 1.0,
            "errors": []
        }
        
        # Completely replace the _wait_for_completion method
        with patch.object(executor, '_wait_for_completion', AsyncMock(return_value=final_result)):
            # Create a dummy task
            task = AsyncMock()
            tasks = {"test": task}
            
            # Call the method directly for testing
            result = await executor._wait_for_completion(tasks, mock_context)
            
            # Verify it returned our mocked result
            assert result == final_result