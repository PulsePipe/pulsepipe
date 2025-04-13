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

# src/pulsepipe/pipelines/concurrent_executor.py

# src/pulsepipe/pipelines/concurrent_executor.py

"""
Concurrent pipeline executor for PulsePipe.

Orchestrates the execution of pipeline stages in parallel using queues.
"""

import asyncio
from typing import Dict, List, Any, Optional
import traceback
import time

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import PipelineError, ConfigurationError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage

logger = LogFactory.get_logger(__name__)

class ConcurrentPipelineExecutor:
    """
    Executes a pipeline with stages running concurrently.
    
    Uses queues between stages to allow for continuous processing.
    Each stage runs as a separate task, consuming from its input queue
    and producing to its output queue.
    """
    
    def __init__(self):
        """Initialize the concurrent pipeline executor."""
        from pulsepipe.pipelines.stages import (
            IngestionStage, DeidentificationStage, 
            ChunkingStage, EmbeddingStage, VectorStoreStage
        )
        
        # Register available stages
        self.available_stages = {
            "ingestion": IngestionStage(),
            "deid": DeidentificationStage(),
            "chunking": ChunkingStage(),
            "embedding": EmbeddingStage(),
            "vectorstore": VectorStoreStage(),
        }
        
        # Define stage dependencies
        self.stage_dependencies = {
            "ingestion": [],                        # Ingestion has no dependencies
            "deid": ["ingestion"],                  # Deid depends on ingestion
            "chunking": ["ingestion"],              # Chunking depends on either ingestion or deid
            "embedding": ["chunking"],              # Embedding depends on chunking
            "vectorstore": ["embedding"]            # Vectorstore depends on embedding
        }
        
        # Stage-specific queues
        self.queues = {}
        
        # Tasks for each stage
        self.tasks = {}
        
        # Stop event for signaling shutdown
        self.stop_event = asyncio.Event()
        
    async def execute_pipeline(self, context: PipelineContext) -> Any:
        """
        Execute a pipeline with concurrent stages.
        
        Args:
            context: Pipeline execution context with configuration
            
        Returns:
            Final results dict with stage-specific outputs
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        logger.info(f"{context.log_prefix} Starting concurrent pipeline execution")
        
        try:
            # Determine which stages are enabled
            enabled_stages = self._get_enabled_stages(context)
            
            if not enabled_stages:
                raise ConfigurationError(
                    "No pipeline stages are enabled",
                    details={"pipeline": context.name}
                )
            
            logger.info(f"{context.log_prefix} Enabled stages: {', '.join(enabled_stages)}")
            
            # Create queues between stages
            self.queues = self._create_queues(enabled_stages)
            
            # Create and start stage tasks
            tasks = await self._start_stage_tasks(context, enabled_stages)
            self.tasks = tasks
            
            # Wait for completed tasks or stop signal
            results = await self._wait_for_completion(tasks, context)
            
            # Process results
            return results
            
        except asyncio.CancelledError:
            logger.info(f"{context.log_prefix} Pipeline execution was cancelled")
            # Cleanup tasks
            await self._cancel_all_tasks()
            raise
        except Exception as e:
            logger.error(f"{context.log_prefix} Error in pipeline execution: {str(e)}")
            logger.error(f"{context.log_prefix} Traceback: {traceback.format_exc()}")
            # Cleanup tasks
            await self._cancel_all_tasks()
            raise PipelineError(
                f"Error in concurrent pipeline execution: {str(e)}",
                details={"pipeline": context.name},
                cause=e
            )
    
    def _get_enabled_stages(self, context: PipelineContext) -> List[str]:
        """Determine which stages are enabled and their execution order."""
        enabled_stages = []
        
        # Check each stage
        if context.is_stage_enabled("ingestion"):
            enabled_stages.append("ingestion")
        
        if context.is_stage_enabled("deid"):
            enabled_stages.append("deid")
            # Update chunking dependency
            self.stage_dependencies["chunking"] = ["deid"]
        else:
            # Reset chunking dependency
            self.stage_dependencies["chunking"] = ["ingestion"]
        
        if context.is_stage_enabled("chunking"):
            enabled_stages.append("chunking")
            
        if context.is_stage_enabled("embedding"):
            enabled_stages.append("embedding")
            
        if context.is_stage_enabled("vectorstore"):
            enabled_stages.append("vectorstore")
        
        # Validate dependencies
        for stage in enabled_stages:
            for dependency in self.stage_dependencies.get(stage, []):
                if dependency not in enabled_stages:
                    context.add_warning("executor", 
                                      f"Stage '{stage}' depends on '{dependency}' which is not enabled")
        
        return enabled_stages
    
    def _create_queues(self, enabled_stages: List[str]) -> Dict[str, asyncio.Queue]:
        """Create queues between stages."""
        queues = {}
        
        # Create a queue for each stage output
        for stage in enabled_stages:
            queues[f"{stage}_output"] = asyncio.Queue()
        
        return queues
    
    async def _start_stage_tasks(
        self, context: PipelineContext, enabled_stages: List[str]
    ) -> Dict[str, asyncio.Task]:
        """Create and start tasks for each stage."""
        tasks = {}
        
        # Create a task for each stage
        for stage_name in enabled_stages:
            stage = self.available_stages.get(stage_name)
            if not stage:
                context.add_warning("executor", f"Stage '{stage_name}' not found, skipping")
                continue
            
            # Get input queue based on dependencies
            dependencies = self.stage_dependencies.get(stage_name, [])
            if dependencies:
                # Use the output queue of the last dependency
                input_queue = self.queues.get(f"{dependencies[-1]}_output")
            else:
                # No input queue for stages without dependencies (e.g., ingestion)
                input_queue = None
            
            # Get output queue
            output_queue = self.queues.get(f"{stage_name}_output")
            
            # Create task
            task = asyncio.create_task(
                self._run_stage(
                    stage=stage,
                    stage_name=stage_name,
                    context=context,
                    input_queue=input_queue,
                    output_queue=output_queue
                )
            )
            tasks[stage_name] = task
        
        return tasks
    
    async def _run_stage(
        self,
        stage: PipelineStage,
        stage_name: str,
        context: PipelineContext,
        input_queue: Optional[asyncio.Queue] = None,
        output_queue: Optional[asyncio.Queue] = None
    ) -> Dict[str, Any]:
        """Run a single stage as a worker."""
        stage_results = []
        
        try:
            # Mark stage start
            context.start_stage(stage_name)
            logger.info(f"{context.log_prefix} Started stage worker: {stage_name}")
            
            # Special case for ingestion (no input queue)
            if stage_name == "ingestion":
                result = await stage.execute(context)
                if result:
                    # Put result in output queue
                    if isinstance(result, list):
                        # For batch results, put each item separately
                        for item in result:
                            await output_queue.put(item)
                            stage_results.append(item)
                    else:
                        # Single result
                        await output_queue.put(result)
                        stage_results.append(result)
                    
                # Mark stage completion for ingestion
                logger.info(f"{context.log_prefix} Ingestion completed, sent {len(stage_results)} items to next stage")
                
                # Signal the end of this stage's output
                await output_queue.put(None)
            else:
                # For other stages, process items from input queue
                while not self.stop_event.is_set():
                    try:
                        # Get item from input queue with timeout
                        item = await asyncio.wait_for(input_queue.get(), timeout=5.0)
                        
                        # Check for end-of-queue marker
                        if item is None:
                            logger.info(f"{context.log_prefix} Received end-of-queue marker in {stage_name}")
                            break
                        
                        # Process item
                        result = await stage.execute(context, item)
                        
                        # Put result in output queue if we have one
                        if result and output_queue:
                            await output_queue.put(result)
                            stage_results.append(result)
                        
                        # Mark item as processed
                        input_queue.task_done()
                        
                    except asyncio.TimeoutError:
                        # Check if we should continue waiting
                        if self.stop_event.is_set():
                            logger.info(f"{context.log_prefix} Stop event detected in {stage_name}, exiting")
                            break
                        # Continue waiting for more items
                        continue
                
                # Signal the end of this stage's output
                if output_queue:
                    await output_queue.put(None)
                
                logger.info(f"{context.log_prefix} Stage {stage_name} completed, processed {len(stage_results)} items")
            
            # Mark stage completion
            context.end_stage(stage_name, stage_results)
            
            return {
                "stage": stage_name,
                "status": "completed",
                "result_count": len(stage_results),
                "results": stage_results
            }
            
        except asyncio.CancelledError:
            logger.info(f"{context.log_prefix} Stage {stage_name} was cancelled")
            raise
        
        except Exception as e:
            logger.error(f"{context.log_prefix} Error in stage {stage_name}: {str(e)}")
            logger.error(f"{context.log_prefix} Traceback: {traceback.format_exc()}")
            
            # Record error in context
            context.add_error(stage_name, f"Failed to execute stage: {str(e)}")
            
            # Propagate error
            raise PipelineError(
                f"Error in pipeline stage '{stage_name}': {str(e)}",
                details={"pipeline": context.name, "stage": stage_name},
                cause=e
            )
    
    async def _wait_for_completion(
        self, tasks: Dict[str, asyncio.Task], context: PipelineContext
    ) -> Dict[str, Any]:
        """Wait for all tasks to complete or for cancellation."""
        results = {}
        errors = []
        
        # Create a task set for asyncio.wait
        task_set = set(tasks.values())
        
        # Wait for all tasks to complete
        while task_set:
            # Wait for the first task to complete or for cancellation
            done, pending = await asyncio.wait(
                task_set, 
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0  # Short timeout to check for cancellation
            )
            
            # Check for cancellation
            if self.stop_event.is_set():
                logger.info(f"{context.log_prefix} Stop event detected, cancelling remaining tasks")
                await self._cancel_all_tasks()
                return {"status": "cancelled", "errors": ["Pipeline execution was cancelled"]}
            
            # Process completed tasks
            for task in done:
                # Get the stage name for this task
                stage_name = next((name for name, t in tasks.items() if t == task), "unknown")
                
                try:
                    # Get the result
                    result = task.result()
                    results[stage_name] = result
                    logger.info(f"{context.log_prefix} Stage {stage_name} completed successfully")
                except Exception as e:
                    errors.append(f"Error in stage {stage_name}: {str(e)}")
                    logger.error(f"{context.log_prefix} Stage {stage_name} failed: {str(e)}")
            
            # Update the task set
            task_set = pending
        
        # Return the combined results
        return {
            "status": "completed" if not errors else "completed_with_errors",
            "results": results,
            "errors": errors
        }
    
    async def _cancel_all_tasks(self):
        """Cancel all running tasks."""
        for stage_name, task in self.tasks.items():
            if not task.done():
                logger.info(f"Cancelling task for stage: {stage_name}")
                task.cancel()
        
        # Wait for all tasks to be cancelled
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
    
    async def stop(self):
        """Signal all tasks to stop."""
        self.stop_event.set()
        await self._cancel_all_tasks()
