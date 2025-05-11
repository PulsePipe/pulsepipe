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

# src/pulsepipe/pipelines/runner.py

"""
Pipeline runner for PulsePipe.

Provides high-level interface for running pipelines.
"""

import os
import asyncio
import json
from typing import Dict, Any, Optional

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import PipelineError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.executor import PipelineExecutor


logger = LogFactory.get_logger(__name__)


class PipelineRunner:
    """
    High-level interface for running PulsePipe pipelines.
    
    This class handles:
    - Setting up execution context
    - Running a single pipeline
    - Coordinating outputs and reporting
    """
    
    def __init__(self):
        """Initialize the pipeline runner."""
        self.executor = PipelineExecutor()
    
    async def run_pipeline(self, config: Dict[str, Any], name: str, **kwargs) -> Dict[str, Any]:
        """
        Run a single pipeline with the given configuration.
        
        Args:
            config: Pipeline configuration
            name: Pipeline name
            **kwargs: Additional options
                concurrent: Whether to run the pipeline with concurrent stages
                
        Returns:
            Dictionary with execution results
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        # Create execution context
        context = PipelineContext(
            name=name,
            config=config,
            output_path=kwargs.get('output_path'),
            summary=kwargs.get('summary', False),
            print_model=kwargs.get('print_model', False),
            pretty=kwargs.get('pretty', True),
            verbose=kwargs.get('verbose', False)
        )
        
        try:
            # Check if concurrent execution is requested
            concurrent = kwargs.get('concurrent', False)
            
            if concurrent:
                # Use concurrent executor
                from pulsepipe.pipelines.concurrent_executor import ConcurrentPipelineExecutor
                executor = ConcurrentPipelineExecutor()
                logger.info(f"{context.log_prefix} Using concurrent pipeline execution")
            else:
                # Use standard executor
                executor = self.executor
                logger.info(f"{context.log_prefix} Using sequential pipeline execution")
            
            # Execute the pipeline
            result = await executor.execute_pipeline(context)
            
            # Get execution summary
            summary = context.get_summary()
            
            # Print summary if requested
            if context.summary:
                logger.info(f"{context.log_prefix} Pipeline summary:")
                for key, value in summary.items():
                    if key != "stage_timings":
                        logger.info(f"{context.log_prefix}   {key}: {value}")
                
                logger.info(f"{context.log_prefix} Stage timings:")
                for stage, timing in summary.get("stage_timings", {}).items():
                    logger.info(f"{context.log_prefix}   {stage}: {timing['duration']:.2f}s")
            
            # Export full model if requested
            if context.print_model and result:
                if context.output_path:
                    context.export_results(result, format="json")
                else:
                    # Print to console
                    if hasattr(result, "model_dump_json"):
                        model_json = result.model_dump_json(indent=2 if context.pretty else None)
                        print(model_json)
                    elif hasattr(result, "__dict__"):
                        print(json.dumps(result.__dict__, indent=2 if context.pretty else None, default=str))
                    else:
                        print(result)
            
            # Return results
            return {
                "result": result,
                "summary": summary,
                "success": True,
                "errors": context.errors,
                "warnings": context.warnings
            }
            
        except Exception as e:
            # Log error
            logger.error(f"{context.log_prefix} Pipeline execution failed: {str(e)}")
            
            # Return error result
            return {
                "result": None,
                "summary": context.get_summary() if hasattr(context, 'get_summary') else {},
                "success": False,
                "errors": context.errors if hasattr(context, 'errors') else [str(e)],
                "warnings": context.warnings if hasattr(context, 'warnings') else []
            }
