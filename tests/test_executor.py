import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from pulsepipe.pipelines.executor import PipelineExecutor
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.utils.errors import ConfigurationError, PipelineError

class TestPipelineExecutor:
    def setup_method(self):
        self.executor = PipelineExecutor()
        self.context = MagicMock(spec=PipelineContext)
        self.context.name = "test_pipeline"
        self.context.log_prefix = "[test_pipeline]"
        self.context.is_stage_enabled = MagicMock()
        self.context.add_warning = MagicMock()
        self.context.add_error = MagicMock()
        self.context.start_stage = MagicMock()
        self.context.end_stage = MagicMock()
    
    def test_init_registers_stages(self):
        assert len(self.executor.available_stages) == 5
        assert "ingestion" in self.executor.available_stages
        assert "deid" in self.executor.available_stages
        assert "chunking" in self.executor.available_stages
        assert "embedding" in self.executor.available_stages
        assert "vectorstore" in self.executor.available_stages
    
    def test_get_enabled_stages_all_enabled(self):
        self.context.is_stage_enabled.return_value = True
        
        stages = self.executor._get_enabled_stages(self.context)
        
        assert stages == ["ingestion", "deid", "chunking", "embedding", "vectorstore"]
        assert self.context.add_warning.call_count == 0
    
    def test_get_enabled_stages_some_disabled(self):
        # Only ingestion and chunking enabled
        def is_enabled(stage):
            return stage in ["ingestion", "chunking"]
        
        self.context.is_stage_enabled.side_effect = is_enabled
        
        stages = self.executor._get_enabled_stages(self.context)
        
        assert stages == ["ingestion", "chunking"]
        # No warnings should be raised since dependencies are met
        assert self.context.add_warning.call_count == 0
    
    def test_get_enabled_stages_dependency_warnings(self):
        # Only embedding and vectorstore enabled - missing dependencies
        # Create a mock side effect function that checks the stage name
        def mock_is_stage_enabled(stage):
            return stage in ["embedding", "vectorstore"]
            
        self.context.is_stage_enabled.side_effect = mock_is_stage_enabled
        
        stages = self.executor._get_enabled_stages(self.context)
        
        assert stages == ["embedding", "vectorstore"]
        # Should have warnings
        assert self.context.add_warning.call_count >= 1
    
    def test_get_enabled_stages_with_deid(self):
        # Test that chunking dependency changes when deid is enabled
        def is_enabled(stage):
            return stage in ["ingestion", "deid", "chunking"]
        
        self.context.is_stage_enabled.side_effect = is_enabled
        
        stages = self.executor._get_enabled_stages(self.context)
        
        assert stages == ["ingestion", "deid", "chunking"]
        # Verify chunking dependency was updated to include deid
        assert self.executor.stage_dependencies["chunking"] == ["deid"]
    
    @pytest.mark.asyncio
    async def test_execute_pipeline_empty_stages(self):
        # No stages enabled
        self.context.is_stage_enabled.return_value = False
        
        with pytest.raises(ConfigurationError) as excinfo:
            await self.executor.execute_pipeline(self.context)
        
        assert "No pipeline stages are enabled" in str(excinfo.value)
    
    @pytest.mark.asyncio
    async def test_execute_pipeline_success(self):
        # Setup test where only ingestion stage is enabled
        self.context.is_stage_enabled.side_effect = lambda stage: stage == "ingestion"
        
        # Mock the ingestion stage
        mock_stage = MagicMock()
        mock_stage.execute = AsyncMock(return_value=["test_result"])
        self.executor.available_stages["ingestion"] = mock_stage
        
        result = await self.executor.execute_pipeline(self.context)
        
        assert result == ["test_result"]
        self.context.start_stage.assert_called_once_with("ingestion")
        mock_stage.execute.assert_called_once_with(self.context, None)
        self.context.end_stage.assert_called_once_with("ingestion", ["test_result"])
    
    @pytest.mark.asyncio
    async def test_execute_pipeline_multiple_stages(self):
        # Setup test where ingestion and chunking stages are enabled
        def is_enabled(stage):
            return stage in ["ingestion", "chunking"]
        
        self.context.is_stage_enabled.side_effect = is_enabled
        
        # Mock the stages
        mock_ingestion = MagicMock()
        mock_ingestion.execute = AsyncMock(return_value=["ingestion_result"])
        self.executor.available_stages["ingestion"] = mock_ingestion
        
        mock_chunking = MagicMock()
        mock_chunking.execute = AsyncMock(return_value=["chunked_result"])
        self.executor.available_stages["chunking"] = mock_chunking
        
        # Reset chunking dependency to just ingestion for this test
        self.executor.stage_dependencies["chunking"] = ["ingestion"]
        
        result = await self.executor.execute_pipeline(self.context)
        
        assert result == ["chunked_result"]
        
        # Verify stages executed in correct order with correct inputs
        mock_ingestion.execute.assert_called_once_with(self.context, None)
        mock_chunking.execute.assert_called_once_with(self.context, ["ingestion_result"])
    
    @pytest.mark.asyncio
    async def test_execute_pipeline_stage_error(self):
        # Setup test where ingestion stage fails
        self.context.is_stage_enabled.side_effect = lambda stage: stage == "ingestion"
        
        # Mock the ingestion stage to raise an error
        mock_stage = MagicMock()
        mock_stage.execute = AsyncMock(side_effect=ValueError("Test error"))
        self.executor.available_stages["ingestion"] = mock_stage
        
        with pytest.raises(PipelineError) as excinfo:
            await self.executor.execute_pipeline(self.context)
        
        assert "Error in pipeline stage 'ingestion': Test error" in str(excinfo.value)
        self.context.add_error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_pipeline_missing_stage(self):
        # Test what happens when a stage is enabled but not found
        self.context.is_stage_enabled.side_effect = lambda stage: stage == "unknown_stage"
        
        # Temporarily modify available_stages to mock an enabled but invalid stage
        original_get_enabled = self.executor._get_enabled_stages
        self.executor._get_enabled_stages = MagicMock(return_value=["unknown_stage"])
        
        try:
            # Should not raise an error but add a warning
            result = await self.executor.execute_pipeline(self.context)
            assert result is None
            self.context.add_warning.assert_called_once_with("executor", "Stage 'unknown_stage' not found, skipping")
        finally:
            # Restore original method
            self.executor._get_enabled_stages = original_get_enabled