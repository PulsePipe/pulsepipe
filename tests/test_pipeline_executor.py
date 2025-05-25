import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime

from pulsepipe.pipelines.executor import PipelineExecutor
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.utils.errors import PipelineError, ConfigurationError
from pulsepipe.pipelines.stages import PipelineStage


class MockStage(PipelineStage):
    def __init__(self, name="mock", should_fail=False, result=None):
        super().__init__(name)
        self.should_fail = should_fail
        self.result = result or f"{name}_result"
        self.executed = False

    async def execute(self, context, input_data=None):
        self.executed = True
        if self.should_fail:
            raise Exception(f"Mock stage {self.name} failed")
        return self.result


@pytest.fixture
def executor():
    return PipelineExecutor()


@pytest.fixture
def basic_config():
    return {
        "ingester": {"type": "mock"},
        "chunker": {"type": "mock"},
        "embedding": {"model": "test"},
        "vectorstore": {"enabled": True}
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


class TestPipelineExecutor:
    
    def test_init(self):
        executor = PipelineExecutor()
        
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

    # TODO: Fix stage mocking to avoid real stage execution
    # @pytest.mark.asyncio
    # async def test_execute_pipeline_no_enabled_stages(self, executor):
    #     # Create context with no enabled stages
    #     config = {}
    #     context = PipelineContext("test", config)
    #     
    #     with pytest.raises(ConfigurationError) as exc_info:
    #         await executor.execute_pipeline(context)
    #     
    #     assert "No pipeline stages are enabled" in str(exc_info.value)
    #     assert exc_info.value.details["pipeline"] == "test"

    @pytest.mark.asyncio
    async def test_execute_pipeline_basic_flow(self, executor, pipeline_context):
        # Mock all stages to return predictable results
        mock_ingestion = MockStage("ingestion", result="ingested_data")
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
        
        # Verify stages were executed in order
        assert mock_ingestion.executed
        assert mock_chunking.executed
        assert mock_embedding.executed
        assert mock_vectorstore.executed
        
        # Verify final result
        assert result == "vectorstore_data"
        
        # Verify context was updated
        assert "ingestion" in pipeline_context.executed_stages
        assert "chunking" in pipeline_context.executed_stages
        assert "embedding" in pipeline_context.executed_stages
        assert "vectorstore" in pipeline_context.executed_stages

    @pytest.mark.asyncio
    async def test_execute_pipeline_with_deid(self, executor):
        config = {
            "ingester": {"type": "mock"},
            "deid": {"enabled": True},
            "chunker": {"type": "mock"}
        }
        context = PipelineContext("test_deid", config)
        
        mock_ingestion = MockStage("ingestion", result="ingested_data")
        mock_deid = MockStage("deid", result="deid_data")
        mock_chunking = MockStage("chunking", result="chunked_data")
        
        executor.available_stages.update({
            "ingestion": mock_ingestion,
            "deid": mock_deid,
            "chunking": mock_chunking
        })
        
        result = await executor.execute_pipeline(context)
        
        # Verify stages were executed
        assert mock_ingestion.executed
        assert mock_deid.executed
        assert mock_chunking.executed
        
        # Verify dependency was updated
        assert executor.stage_dependencies["chunking"] == ["deid"]
        
        assert result == "chunked_data"

    @pytest.mark.asyncio
    async def test_execute_pipeline_stage_failure(self, executor, pipeline_context):
        # Create a failing stage
        mock_ingestion = MockStage("ingestion", result="ingested_data")
        mock_chunking = MockStage("chunking", should_fail=True)
        
        executor.available_stages.update({
            "ingestion": mock_ingestion,
            "chunking": mock_chunking
        })
        
        with pytest.raises(PipelineError) as exc_info:
            await executor.execute_pipeline(pipeline_context)
        
        # Verify error details
        assert "Error in pipeline stage 'chunking'" in str(exc_info.value)
        assert exc_info.value.details["pipeline"] == "test_pipeline"
        assert exc_info.value.details["stage"] == "chunking"
        
        # Verify ingestion completed but chunking failed
        assert mock_ingestion.executed
        assert mock_chunking.executed
        
        # Verify error was recorded in context
        assert len(pipeline_context.errors) == 1
        assert pipeline_context.errors[0]["stage"] == "chunking"

    # TODO: Fix stage mocking to avoid real stage execution
    # @pytest.mark.asyncio
    # async def test_execute_pipeline_missing_stage(self, executor, pipeline_context):
    #     # Remove a stage to simulate missing stage
    #     del executor.available_stages["chunking"]
    #     
    #     result = await executor.execute_pipeline(pipeline_context)
    #     
    #     # Should complete but skip missing stage
    #     assert len(pipeline_context.warnings) >= 1
    #     warning_messages = [w["message"] for w in pipeline_context.warnings]
    #     assert any("Stage 'chunking' not found, skipping" in msg for msg in warning_messages)

    # TODO: Fix stage mocking to avoid real stage execution
    # @pytest.mark.asyncio
    # async def test_execute_pipeline_result_logging(self, executor, pipeline_context):
    #     mock_ingestion = MockStage("ingestion", result=["item1", "item2", "item3"])
    #     mock_chunking = MockStage("chunking", result=None)
    #     
    #     executor.available_stages.update({
    #         "ingestion": mock_ingestion,
    #         "chunking": mock_chunking
    #     })
    #     
    #     with patch('pulsepipe.pipelines.executor.logger') as mock_logger:
    #         await executor.execute_pipeline(pipeline_context)
    #         
    #         # Verify logging for list results
    #         mock_logger.info.assert_any_call(
    #             f"{pipeline_context.log_prefix} ingestion produced 3 items"
    #         )
    #         
    #         # Verify logging for None results
    #         mock_logger.warning.assert_any_call(
    #             f"{pipeline_context.log_prefix} chunking produced None result"
    #         )

    def test_get_enabled_stages_basic(self, executor, pipeline_context):
        enabled_stages = executor._get_enabled_stages(pipeline_context)
        
        # Should include ingestion, chunking, embedding, vectorstore
        # (deid is not enabled in basic_config)
        expected_stages = ["ingestion", "chunking", "embedding", "vectorstore"]
        assert enabled_stages == expected_stages

    def test_get_enabled_stages_with_deid(self, executor):
        config = {
            "ingester": {"type": "mock"},
            "deid": {"enabled": True},
            "chunker": {"type": "mock"}
        }
        context = PipelineContext("test_deid", config)
        
        enabled_stages = executor._get_enabled_stages(context)
        
        # Should include deid and update chunking dependency
        assert "deid" in enabled_stages
        assert "chunking" in enabled_stages
        assert executor.stage_dependencies["chunking"] == ["deid"]

    def test_get_enabled_stages_dependency_validation(self, executor):
        # Create config with embedding but no chunking
        config = {
            "ingester": {"type": "mock"},
            "embedding": {"model": "test"}
        }
        context = PipelineContext("test_deps", config)
        
        enabled_stages = executor._get_enabled_stages(context)
        
        # Should add warning about missing dependency
        warnings = [w["message"] for w in context.warnings]
        assert any("depends on 'chunking' which is not enabled" in msg for msg in warnings)

    def test_get_enabled_stages_ingestion_always_included(self, executor):
        # Test that ingestion is always included even if not explicitly enabled
        config = {
            "chunker": {"type": "mock"}
        }
        context = PipelineContext("test_ingestion", config)
        
        enabled_stages = executor._get_enabled_stages(context)
        
        assert "ingestion" in enabled_stages

    def test_get_enabled_stages_partial_pipeline(self, executor):
        # Test with only ingestion and chunking enabled
        config = {
            "ingester": {"type": "mock"},
            "chunker": {"type": "mock"}
        }
        context = PipelineContext("test_partial", config)
        
        enabled_stages = executor._get_enabled_stages(context)
        
        assert enabled_stages == ["ingestion", "chunking"]
        assert "embedding" not in enabled_stages
        assert "vectorstore" not in enabled_stages

    def test_get_enabled_stages_deid_dependency_reset(self, executor):
        # First enable deid to change chunking dependency
        config_with_deid = {
            "ingester": {"type": "mock"},
            "deid": {"enabled": True},
            "chunker": {"type": "mock"}
        }
        context_with_deid = PipelineContext("test_deid", config_with_deid)
        executor._get_enabled_stages(context_with_deid)
        assert executor.stage_dependencies["chunking"] == ["deid"]
        
        # Then test without deid to ensure dependency is reset
        config_no_deid = {
            "ingester": {"type": "mock"},
            "chunker": {"type": "mock"}
        }
        context_no_deid = PipelineContext("test_no_deid", config_no_deid)
        executor._get_enabled_stages(context_no_deid)
        assert executor.stage_dependencies["chunking"] == ["ingestion"]

    # TODO: Fix stage mocking to avoid real stage execution  
    # @pytest.mark.asyncio
    # async def test_execute_pipeline_context_stage_tracking(self, executor, pipeline_context):
    #     mock_ingestion = MockStage("ingestion", result="test_result")
    #     executor.available_stages["ingestion"] = mock_ingestion
    #     
    #     # Mock context methods to verify they're called
    #     pipeline_context.start_stage = Mock()
    #     pipeline_context.end_stage = Mock()
    #     
    #     await executor.execute_pipeline(pipeline_context)
    #     
    #     # Verify context stage tracking methods were called
    #     pipeline_context.start_stage.assert_called_with("ingestion")
    #     pipeline_context.end_stage.assert_called_with("ingestion", "test_result")

    # TODO: Fix to avoid real Weaviate connections
    # @pytest.mark.asyncio 
    # async def test_execute_pipeline_end_time_set(self, executor, pipeline_context):
    #     mock_ingestion = MockStage("ingestion", result="test_result")
    #     executor.available_stages["ingestion"] = mock_ingestion
    #     
    #     # Ensure end_time is None initially
    #     pipeline_context.end_time = None
    #     
    #     await executor.execute_pipeline(pipeline_context)
    #     
    #     # end_time should still be None as it's set when get_summary is called
    #     assert pipeline_context.end_time is None

    def test_stage_dependencies_structure(self, executor):
        # Verify all expected stages have dependencies defined
        expected_stages = ["ingestion", "deid", "chunking", "embedding", "vectorstore"]
        for stage in expected_stages:
            assert stage in executor.stage_dependencies
            assert isinstance(executor.stage_dependencies[stage], list)