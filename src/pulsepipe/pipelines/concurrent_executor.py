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

"""
Concurrent pipeline executor for PulsePipe.

Orchestrates the execution of pipeline stages in parallel using queues.
"""

import asyncio
import time
from typing import Dict, List, Any, Optional
import traceback

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
        
        # Global timeout
        self.timeout = None
        
    async def execute_pipeline(self, context: PipelineContext, timeout: Optional[float] = None) -> Any:
        """
        Execute a pipeline with concurrent stages.
        
        Args:
            context: Pipeline execution context with configuration
            timeout: Global timeout for pipeline execution in seconds
            
        Returns:
            Final results dict with stage-specific outputs
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        logger.info(f"{context.log_prefix} Starting concurrent pipeline execution")
        
        # Set timeout
        self.timeout = timeout
        if timeout:
            logger.info(f"{context.log_prefix} Pipeline timeout set to {timeout} seconds")
            
        # Create timeout task if needed
        timeout_task = None
        if timeout:
            timeout_task = asyncio.create_task(self._timeout_handler(timeout))
        
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
        finally:
            # Cancel timeout task if it exists
            if timeout_task:
                timeout_task.cancel()
                try:
                    await timeout_task
                except asyncio.CancelledError:
                    pass
    
    async def _timeout_handler(self, timeout_seconds: float):
        """Handle global timeout for the pipeline."""
        try:
            await asyncio.sleep(timeout_seconds)
            logger.warning(f"Pipeline timeout reached after {timeout_seconds} seconds, stopping execution")
            self.stop_event.set()
        except asyncio.CancelledError:
            logger.debug("Timeout handler cancelled")
            raise
    
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
            queues[f"{stage}_output"] = asyncio.Queue(maxsize=100)  # Set a reasonable queue size
        
        return queues
    
    async def _start_stage_tasks(
        self, context: PipelineContext, enabled_stages: List[str]
    ) -> Dict[str, asyncio.Task]:
        """Create and start tasks for each stage."""
        tasks = {}
        
        # Track stage start order for better logging
        stage_order = 0
        
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
            
            # Start the stage and mark its execution order
            context.start_stage(stage_name)
            
            # Create task
            task = asyncio.create_task(
                self._run_stage(
                    stage=stage,
                    stage_name=stage_name,
                    context=context,
                    input_queue=input_queue,
                    output_queue=output_queue,
                    order=stage_order
                ),
                name=f"pipeline_{context.name}_{stage_name}"
            )
            tasks[stage_name] = task
            stage_order += 1
            
            logger.info(f"{context.log_prefix} Started stage worker: {stage_name} (order: {stage_order})")
        
        return tasks
    
    async def _run_stage(
        self,
        stage: PipelineStage,
        stage_name: str,
        context: PipelineContext,
        input_queue: Optional[asyncio.Queue] = None,
        output_queue: Optional[asyncio.Queue] = None,
        order: int = 0
    ) -> Dict[str, Any]:
        """Run a single stage as a worker."""
        stage_results = []
        item_count = 0
        stage_start_time = time.time()
        last_progress_time = stage_start_time

        last_item_count = 0
    
        try:            
            # Special case for ingestion (no input queue)
            if stage_name == "ingestion":
                try:
                    # Check for continuous mode adapter
                    adapter_config = context.config.get("adapter", {})
                    continuous_mode = False
                    if adapter_config.get("type") == "file_watcher":
                        continuous_mode = adapter_config.get("continuous", True)
                    
                    if continuous_mode:
                        logger.info(f"{context.log_prefix} Running ingestion in continuous mode")
                        logger.info(f"{context.log_prefix} Starting continuous ingestion")
                        
                        # First execution starts the continuous adapter
                        # This returns immediately with initial results
                        result = await stage.execute(context)
                        
                        # Process initial results
                        if result:
                            if isinstance(result, list):
                                # For batch results, put each item separately
                                for item in result:
                                    if self.stop_event.is_set():
                                        logger.info(f"{context.log_prefix} Stop event detected in {stage_name}, stopping item processing")
                                        break
                                        
                                    await output_queue.put(item)
                                    stage_results.append(item)
                                    item_count += 1
                            else:
                                # Single result
                                await output_queue.put(result)
                                stage_results.append(result)
                                item_count += 1
                            
                            # Log progress
                            logger.info(f"{context.log_prefix} Ingestion processed {item_count} total items")
                        
                        # Now periodically poll for new results (every 1 second)
                        # This simulates a continuous flow of data
                        while not self.stop_event.is_set():
                            try:
                                # Short timeout to poll for new results
                                new_result = await asyncio.wait_for(stage.execute(context), timeout=1.0)
                                
                                # Process any new results
                                if new_result:
                                    if isinstance(new_result, list):
                                        # For batch results, put each item separately
                                        for item in new_result:
                                            if self.stop_event.is_set():
                                                break
                                                
                                            await output_queue.put(item)
                                            stage_results.append(item)
                                            item_count += 1
                                    else:
                                        # Single result
                                        await output_queue.put(new_result)
                                        stage_results.append(new_result)
                                        item_count += 1
                                    
                                    # Log progress
                                    logger.info(f"{context.log_prefix} Ingestion processed {item_count} total items")
                            except asyncio.TimeoutError:
                                # This is expected - just retry
                                await asyncio.sleep(0.5)  # Brief pause between polls
                            except Exception as e:
                                logger.error(f"{context.log_prefix} Error in continuous ingestion poll: {e}")
                                await asyncio.sleep(1.0)  # Longer pause after error
                        
                        logger.info(f"{context.log_prefix} Continuous ingestion completed, processed {item_count} items")
                    else:
                        # Standard one-time ingestion
                        result = await stage.execute(context)
                        if result:
                            # Put result in output queue
                            if isinstance(result, list):
                                # For batch results, put each item separately
                                for item in result:
                                    if self.stop_event.is_set():
                                        logger.info(f"{context.log_prefix} Stop event detected in {stage_name}, stopping item processing")
                                        break
                                        
                                    await output_queue.put(item)
                                    stage_results.append(item)
                                    item_count += 1
                                    
                                    # Log progress periodically
                                    current_time = time.time()
                                    if current_time - last_progress_time > 30.0:  # No progress for 30 seconds
                                        if item_count == last_item_count:  # No new items processed
                                            logger.warning(f"{context.log_prefix} No progress in {stage_name} for 30s, might be stuck")
                                            
                                            # After 3 no-progress warnings (90 seconds), forcibly terminate
                                            if no_progress_warnings > 2:
                                                logger.error(f"{context.log_prefix} Stage {stage_name} appears stuck, forcing completion")
                                                break  # Exit loop and complete stage
                                                
                                            no_progress_warnings += 1
                                        else:
                                            # Reset if we made progress
                                            last_item_count = item_count
                                            last_progress_time = current_time
                            else:
                                # Single result
                                await output_queue.put(result)
                                stage_results.append(result)
                                item_count = 1
                except Exception as e:
                    logger.error(f"{context.log_prefix} Error in ingestion stage: {e}")
                    context.add_error(stage_name, f"Failed to execute stage: {str(e)}")
                    
                # For continuous mode, we don't signal completion until explicitly stopped
                if continuous_mode:
                    logger.info(f"{context.log_prefix} Ingestion stage ongoing - processed {len(stage_results)} items so far")
                else:
                    # For one-time processing, mark completion
                    logger.info(f"{context.log_prefix} Ingestion completed, sent {len(stage_results)} items to next stage")
                    
                    # Signal the end of this stage's output (ONLY for non-continuous mode)
                    if output_queue:
                        await output_queue.put(None)
            else:
                # For other stages, process items from input queue
                if not input_queue:
                    logger.error(f"{context.log_prefix} No input queue for stage {stage_name}")
                    context.add_error(stage_name, "Missing input queue")
                    if output_queue:
                        await output_queue.put(None)  # Signal end even if error
                    return {
                        "stage": stage_name,
                        "status": "failed",
                        "error": "Missing input queue",
                        "result_count": 0,
                        "results": []
                    }
                
                while not self.stop_event.is_set():
                    try:
                        # Get item from input queue with timeout
                        item = await asyncio.wait_for(input_queue.get(), timeout=10.0)
                        
                        # Check for end-of-queue marker
                        if item is None:
                            logger.info(f"{context.log_prefix} Received end-of-queue marker in {stage_name}")
                            break
                        
                        # Process item
                        try:
                            result = await stage.execute(context, item)
                            
                            # Put result in output queue if we have one
                            if result and output_queue:
                                await output_queue.put(result)
                                stage_results.append(result)
                                item_count += 1
                                
                                # Log progress periodically
                                current_time = time.time()
                                if current_time - last_progress_time > 5.0:
                                    logger.info(f"{context.log_prefix} {stage_name}: Processed {item_count} items so far")
                                    last_progress_time = current_time
                        except Exception as e:
                            logger.error(f"{context.log_prefix} Error processing item in {stage_name}: {e}")
                            context.add_error(stage_name, f"Error processing item: {str(e)}")
                        
                        # Mark item as processed
                        input_queue.task_done()
                        
                    except asyncio.TimeoutError:
                        # Check if we should continue waiting
                        if self.stop_event.is_set():
                            logger.info(f"{context.log_prefix} Stop event detected in {stage_name}, exiting")
                            break
                        # Log that we're still waiting for input
                        logger.debug(f"{context.log_prefix} {stage_name} waiting for input...")
                        continue
                    except Exception as e:
                        logger.error(f"{context.log_prefix} Unexpected error in {stage_name}: {e}")
                        if self.stop_event.is_set():
                            break
                
                # Signal the end of this stage's output
                if output_queue:
                    await output_queue.put(None)
                
                stage_duration = time.time() - stage_start_time
                logger.info(f"{context.log_prefix} Stage {stage_name} completed in {stage_duration:.2f}s, processed {item_count} items")
            
            # Mark stage completion
            context.end_stage(stage_name, stage_results)
            
            return {
                "stage": stage_name,
                "status": "completed",
                "result_count": item_count,
                "duration": time.time() - stage_start_time,
                "results": stage_results
            }
            
        except asyncio.CancelledError:
            logger.info(f"{context.log_prefix} Stage {stage_name} was cancelled")
            
            # Signal end-of-stream to next stage
            if output_queue:
                await output_queue.put(None)
                
            raise
        
        except Exception as e:
            logger.error(f"{context.log_prefix} Error in stage {stage_name}: {str(e)}")
            logger.error(f"{context.log_prefix} Traceback: {traceback.format_exc()}")
            
            # Record error in context
            context.add_error(stage_name, f"Failed to execute stage: {str(e)}")
            
            # Signal end-of-stream to next stage
            if output_queue:
                await output_queue.put(None)
            
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
        
        # Start time for tracking overall duration
        start_time = time.time()
        last_status_time = start_time
        pending_count = len(task_set)
        
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
            
            # Log a status update periodically
            current_time = time.time()
            if current_time - last_status_time > 30.0 and pending:
                pending_count = len(pending)
                elapsed = current_time - start_time
                pending_stages = [name for name, task in tasks.items() if task in pending]
                logger.info(f"{context.log_prefix} Pipeline status: {len(results)}/{len(tasks)} stages completed, "
                           f"{pending_count} pending ({', '.join(pending_stages)}), "
                           f"elapsed: {elapsed:.1f}s")
                last_status_time = current_time
        
        # Return the combined results
        total_duration = time.time() - start_time
        logger.info(f"{context.log_prefix} All pipeline stages completed in {total_duration:.2f}s")
        
        return {
            "status": "completed" if not errors else "completed_with_errors",
            "results": results,
            "errors": errors,
            "duration": total_duration
        }
    
    async def _cancel_all_tasks(self):
        """Cancel all running tasks."""
        for stage_name, task in self.tasks.items():
            if not task.done():
                logger.info(f"Cancelling task for stage: {stage_name}")
                task.cancel()
        
        # Wait for all tasks to be cancelled
        if self.tasks:
            pending_tasks = [task for task in self.tasks.values() if not task.done()]
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
                logger.info(f"Cancelled {len(pending_tasks)} pending tasks")
    
    async def stop(self):
        """Signal all tasks to stop."""
        logger.info("Setting stop event for all pipeline tasks")
        self.stop_event.set()
        await self._cancel_all_tasks()
