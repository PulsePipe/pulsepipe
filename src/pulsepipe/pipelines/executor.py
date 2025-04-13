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

# src/pulsepipe/pipelines/executor.py

"""
Pipeline executor for PulsePipe.

Orchestrates the execution of pipeline stages in the correct order.
"""

from typing import Any, List
import asyncio
import traceback
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import PipelineError, ConfigurationError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage, IngestionStage, ChunkingStage
from pulsepipe.pipelines.stages.deid import DeidentificationStage
from pulsepipe.pipelines.stages.embedding import EmbeddingStage
from pulsepipe.pipelines.stages.vectorstore import VectorStoreStage

logger = LogFactory.get_logger(__name__)

class PipelineExecutor:
    """
    Executes a pipeline by running its stages in the correct order.
    
    The executor:
    - Validates stage dependencies
    - Tracks stage execution
    - Handles errors and timeouts
    - Reports progress
    """
    
    def __init__(self):
        """Initialize the pipeline executor."""
        # Register available stages
        self.available_stages = {
            "ingestion": IngestionStage(),
            "deid": DeidentificationStage(),
            "chunking": ChunkingStage(),
            "embedding": EmbeddingStage(),
            "vectorstore": VectorStoreStage(),
        }
        
        # Define stage dependencies (which stages depend on which)
        self.stage_dependencies = {
            "ingestion": [],           # Ingestion has no dependencies
            "deid": ["ingestion"],     # Deid depends on ingestion
            "chunking": ["ingestion"], # Chunking can depend on either ingestion or deid
            "embedding": ["chunking"], # Embedding depends on chunking
            "vectorstore": ["embedding"] # Vectorstore depends on embedding
        }
    

    async def execute_pipeline(self, context: PipelineContext) -> Any:
        """
        Execute a pipeline according to its configuration.
        
        Args:
            context: Pipeline execution context with configuration
            
        Returns:
            Final pipeline result
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        logger.info(f"{context.log_prefix} Starting pipeline execution")
        
        # Determine which stages are enabled and their execution order
        enabled_stages = self._get_enabled_stages(context)
        
        if not enabled_stages:
            raise ConfigurationError(
                "No pipeline stages are enabled",
                details={"pipeline": context.name}
            )
        
        logger.info(f"{context.log_prefix} Enabled stages: {', '.join(enabled_stages)}")
        
        # Execute each stage in sequence
        result = None
        
        for stage_name in enabled_stages:
            stage = self.available_stages.get(stage_name)
            
            if not stage:
                context.add_warning("executor", f"Stage '{stage_name}' not found, skipping")
                continue
            
            # Mark stage start in context
            context.start_stage(stage_name)
            
            try:
                # Execute the stage
                logger.info(f"{context.log_prefix} Executing stage: {stage_name}")
                
                # Pass the result from the previous stage as input
                stage_result = await stage.execute(context, result)
                
                # Record successful result in context
                context.end_stage(stage_name, stage_result)
                
                # Set as current result for the next stage
                result = stage_result
                
                # Add debug log to verify the result type
                if result is not None:
                    if isinstance(result, list):
                        logger.info(f"{context.log_prefix} {stage_name} produced {len(result)} items")
                    else:
                        logger.info(f"{context.log_prefix} {stage_name} produced result of type: {type(result).__name__}")
                else:
                    logger.warning(f"{context.log_prefix} {stage_name} produced None result")
                
            except Exception as e:
                # Print more detailed error information
                logger.error(f"{context.log_prefix} Error in stage '{stage_name}': {str(e)}")
                logger.error(f"{context.log_prefix} Traceback: {traceback.format_exc()}")
                
                # Record error in context
                context.add_error(stage_name, f"Failed to execute stage: {str(e)}")
                
                # Raise pipeline error
                raise PipelineError(
                    f"Error in pipeline stage '{stage_name}': {str(e)}",
                    details={"pipeline": context.name, "stage": stage_name},
                    cause=e
                )
        
        logger.info(f"{context.log_prefix} Pipeline execution completed successfully")
        
        # Set end time in context
        context.end_time = None  # This will be set when get_summary is called
        
        # Return final result
        return result
    

    def _get_enabled_stages(self, context: PipelineContext) -> List[str]:
        """
        Determine which stages are enabled and their correct execution order.
        
        Args:
            context: Pipeline execution context
            
        Returns:
            List of stage names in execution order
        """
        # Check which stages are enabled in the configuration
        enabled_stages = []
        
        # Always include ingestion if it's enabled
        if context.is_stage_enabled("ingestion"):
            enabled_stages.append("ingestion")
        
        # Check if de-identification is enabled
        deid_enabled = context.is_stage_enabled("deid")
        if deid_enabled:
            enabled_stages.append("deid")
            # Update chunking dependency to include deid
            self.stage_dependencies["chunking"] = ["deid"]
        else:
            # Reset chunking dependency to just ingestion
            self.stage_dependencies["chunking"] = ["ingestion"]
        
        # Check if chunking is enabled
        if context.is_stage_enabled("chunking"):
            enabled_stages.append("chunking")
            
        # Check if embedding is enabled
        if context.is_stage_enabled("embedding"):
            enabled_stages.append("embedding")
            
        # Check if vectorstore is enabled
        if context.is_stage_enabled("vectorstore"):
            enabled_stages.append("vectorstore")
        
        # Validate dependencies
        for stage in enabled_stages:
            for dependency in self.stage_dependencies.get(stage, []):
                if dependency not in enabled_stages:
                    context.add_warning("executor", 
                                      f"Stage '{stage}' depends on '{dependency}' which is not enabled")
        
        return enabled_stages
