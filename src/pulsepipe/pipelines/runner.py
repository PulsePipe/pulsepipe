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

# src/pulsepipe/pipelines/runner.py

"""
Pipeline runner for PulsePipe.

Provides high-level interface for running pipelines.
"""

import os
import asyncio
import json
from typing import Dict, List, Any, Optional, Union

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.errors import PipelineError, ConfigurationError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.executor import PipelineExecutor


logger = LogFactory.get_logger(__name__)


class PipelineRunner:
    """
    High-level interface for running PulsePipe pipelines.
    
    This class handles:
    - Loading pipeline configurations
    - Setting up execution context
    - Running single or multiple pipelines
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
            # Execute the pipeline
            result = await self.executor.execute_pipeline(context)
            
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
    
    async def run_multiple_pipelines(self, config_path: str, pipeline_names: Optional[List[str]] = None, 
                                  run_all: bool = False, **kwargs) -> List[Dict[str, Any]]:
        """
        Run multiple pipelines from a configuration file.
        
        Args:
            config_path: Path to pipeline configuration file
            pipeline_names: List of specific pipelines to run (optional)
            run_all: Whether to run all pipelines including inactive ones
            **kwargs: Additional options
            
        Returns:
            List of dictionaries with execution results
            
        Raises:
            ConfigurationError: If pipeline configuration is invalid
        """
        # Load pipeline configuration
        try:
            config = load_config(config_path)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load pipeline configuration file: {config_path}",
                cause=e
            )
        
        pipelines = config.get('pipelines', [])
        if not pipelines:
            raise ConfigurationError(
                f"No pipelines found in {config_path}",
                details={"pipeline_config_path": config_path}
            )
        
        # Determine which pipelines to run
        target_pipelines = []
        if pipeline_names:
            target_pipelines = [p for p in pipelines if p.get('name') in pipeline_names]
            if not target_pipelines:
                raise ConfigurationError(
                    f"No matching pipelines found for names: {', '.join(pipeline_names)}",
                    details={"available_pipelines": [p.get('name') for p in pipelines]}
                )
        elif run_all:
            target_pipelines = pipelines
        else:
            target_pipelines = [p for p in pipelines if p.get('active', True)]
        
        logger.info(f"Running {len(target_pipelines)} pipeline(s)")
        
        # Execute pipelines (sequentially for now)
        results = []
        for pipeline_config in target_pipelines:
            pipeline_name = pipeline_config.get('name', 'unnamed')
            
            # Prepare output path
            output_path = kwargs.get('output_path')
            if output_path:
                base, ext = os.path.splitext(output_path)
                pipeline_output = f"{base}_{pipeline_name}{ext}"
            else:
                pipeline_output = None
            
            # Run the pipeline
            logger.info(f"Starting pipeline: {pipeline_name}")
            
            # Create a modified copy of kwargs without 'output_path'
            pipeline_kwargs = {k: v for k, v in kwargs.items() if k != 'output_path'}

            # Now use the pipeline-specific output_path and the modified kwargs
            result = await self.run_pipeline(
                config=pipeline_config,
                name=pipeline_name,
                output_path=pipeline_output,
                **pipeline_kwargs
            )
            
            # Store result
            results.append({
                "name": pipeline_name,
                "result": result
            })
        
        # Return all results
        return results
