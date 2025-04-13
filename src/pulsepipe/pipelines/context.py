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

# src/pulsepipe/pipelines/context.py

"""
Pipeline execution context for PulsePipe.

Manages state and configuration during pipeline execution.
"""

import uuid
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import ConfigurationError

logger = LogFactory.get_logger(__name__)

class PipelineContext:
    """
    Context object that maintains state during pipeline execution.
    
    This class tracks:
    - Pipeline configuration
    - Intermediate results between stages
    - Output paths and formats
    - Execution metadata (timing, etc.)
    """
    
    def __init__(self, 
                 name: str,
                 config: Dict[str, Any],
                 output_path: Optional[str] = None,
                 summary: bool = False,
                 print_model: bool = False,
                 pretty: bool = True,
                 verbose: bool = False):
        """
        Initialize a pipeline context.
        
        Args:
            name: Pipeline name
            config: Pipeline configuration dictionary
            output_path: Path to write output files (if any)
            summary: Whether to generate execution summary
            print_model: Whether to print the model data
            pretty: Whether to use pretty formatting for output
            verbose: Whether to include verbose information
        """
        self.pipeline_id = str(uuid.uuid4())
        self.name = name
        self.config = config
        self.output_path = output_path
        self.summary = summary
        self.print_model = print_model
        self.pretty = pretty
        self.verbose = verbose
        
        # Execution tracking
        self.start_time = datetime.now()
        self.end_time = None
        self.stage_timings = {}
        self.executed_stages = []
        
        # Stage data and results
        self.input_data = None
        self.ingested_data = None
        self.deidentified_data = None
        self.chunked_data = None
        self.embedded_data = None
        self.vectorstore_data = None
        
        # Errors and warnings
        self.errors = []
        self.warnings = []
        
        # Initialize the logger prefix for consistent logging
        self.log_prefix = f"[{self.name}:{self.pipeline_id[:8]}]"
        
        logger.info(f"{self.log_prefix} Pipeline context initialized")
    
    def start_stage(self, stage_name: str) -> None:
        """Mark the start of a pipeline stage for timing purposes."""
        logger.info(f"{self.log_prefix} Starting stage: {stage_name}")
        self.stage_timings[stage_name] = {
            'start': datetime.now(),
            'end': None,
            'duration': None
        }
    
    def end_stage(self, stage_name: str, result: Any = None) -> None:
        """
        Mark the end of a pipeline stage and store its result.
        
        Args:
            stage_name: Name of the stage that completed
            result: Result data from the stage execution
        """
        end_time = datetime.now()
        
        if stage_name in self.stage_timings:
            self.stage_timings[stage_name]['end'] = end_time
            start_time = self.stage_timings[stage_name]['start']
            duration = (end_time - start_time).total_seconds()
            self.stage_timings[stage_name]['duration'] = duration
            
            logger.info(f"{self.log_prefix} Completed stage: {stage_name} in {duration:.2f}s")
        else:
            logger.warning(f"{self.log_prefix} Ended untracked stage: {stage_name}")
        
        # Store the result in the appropriate attribute
        # This is critical for stage-to-stage data flow
        if stage_name == "ingestion":
            self.ingested_data = result
            logger.info(f"{self.log_prefix} Stored ingestion result of type: {type(result).__name__}")
            if isinstance(result, list):
                logger.info(f"{self.log_prefix} Ingested {len(result)} items")
        elif stage_name == "deid":
            self.deidentified_data = result
            logger.info(f"{self.log_prefix} Stored deid result of type: {type(result).__name__}")
        elif stage_name == "chunking":
            self.chunked_data = result
            logger.info(f"{self.log_prefix} Stored chunking result: {len(result) if isinstance(result, list) else type(result).__name__}")
        elif stage_name == "embedding":
            self.embedded_data = result
            logger.info(f"{self.log_prefix} Stored embedding result")
        elif stage_name == "vectorstore":
            self.vectorstore_data = result
            logger.info(f"{self.log_prefix} Stored vectorstore result")
        
        # Record that this stage was executed
        self.executed_stages.append(stage_name)
    
    def add_error(self, stage: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Record an error that occurred during pipeline execution.
        
        Args:
            stage: Pipeline stage where the error occurred
            message: Error message
            details: Additional error details (optional)
        """
        error = {
            'stage': stage,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        self.errors.append(error)
        logger.error(f"{self.log_prefix} Error in {stage}: {message}")
    
    def add_warning(self, stage: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a warning that occurred during pipeline execution.
        
        Args:
            stage: Pipeline stage where the warning occurred
            message: Warning message
            details: Additional warning details (optional)
        """
        warning = {
            'stage': stage,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        self.warnings.append(warning)
        logger.warning(f"{self.log_prefix} Warning in {stage}: {message}")
    

    def get_stage_config(self, stage_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific pipeline stage.
        
        Args:
            stage_name: Name of the stage to get configuration for
            
        Returns:
            Configuration dictionary for the specified stage
        """
        # Look for direct stage config
        if stage_name in self.config:
            logger.info(f"{self.log_prefix} Found direct config for stage: {stage_name}")
            return self.config[stage_name]
        
        # For vectorstore, also check the top level configuration
        if stage_name == "vectorstore" and "vectorstore" in self.config:
            logger.info(f"{self.log_prefix} Found vectorstore config at top level")
            return self.config["vectorstore"]
        
        # Check common alternate names
        alternate_names = {
            "ingestion": ["ingester", "ingest"],
            "deid": ["deidentify", "deidentification", "de-id"],
            "chunking": ["chunker", "chunk"],
            "embedding": ["embedder", "embed"],
            "vectorstore": ["vector_store", "vector-store"]
        }
        
        if stage_name in alternate_names:
            for alt_name in alternate_names[stage_name]:
                if alt_name in self.config:
                    logger.info(f"{self.log_prefix} Found config for stage {stage_name} under alternate name: {alt_name}")
                    return self.config[alt_name]
        
        logger.warning(f"{self.log_prefix} No configuration found for stage: {stage_name}")
        # Return empty config if not found
        return {}
    
    def is_stage_enabled(self, stage_name: str) -> bool:
        """
        Check if a pipeline stage is enabled in the configuration.
        
        Args:
            stage_name: Name of the stage to check
            
        Returns:
            True if the stage is enabled, False otherwise
        """
        config = self.get_stage_config(stage_name)
        
        # If config exists and 'enabled' is explicitly False, stage is disabled
        if config and 'enabled' in config:
            enabled = config['enabled']
            logger.info(f"{self.log_prefix} Stage {stage_name} explicitly enabled: {enabled}")
            return enabled
        
        # Special case: chunking is enabled by default if chunker config exists
        # This is important for backward compatibility
        if stage_name == "chunking" and "chunker" in self.config:
            logger.info(f"{self.log_prefix} Stage {stage_name} enabled (chunker config exists)")
            return True
            
        # If stage is in config, it's considered enabled
        if config:
            logger.info(f"{self.log_prefix} Stage {stage_name} implicitly enabled (config exists)")
            return True
            
        # Special case: ingestion is always enabled
        if stage_name == "ingestion":
            logger.info(f"{self.log_prefix} Stage {stage_name} is always enabled")
            return True
            
        logger.info(f"{self.log_prefix} Stage {stage_name} is disabled (no config)")
        return False
    
    def get_output_path_for_stage(self, stage_name: str, suffix: str = None) -> Optional[str]:
        """
        Get the output file path for a specific stage.
        
        Args:
            stage_name: Pipeline stage
            suffix: Optional suffix to add to the filename
            
        Returns:
            Output path string or None if no output path is set
        """
        if not self.output_path:
            return None
            
        base, ext = os.path.splitext(self.output_path)
        
        # Compose filename with stage name and optional suffix
        if suffix:
            return f"{base}_{stage_name}_{suffix}{ext}"
        return f"{base}_{stage_name}{ext}"
    
    def export_results(self, data: Any, stage: str = None, format: str = None) -> None:
        """
        Export data to a file.
        
        Args:
            data: Data to export
            stage: Pipeline stage that generated the data
            format: Output format (e.g., "json", "jsonl")
        """
        if not self.output_path:
            logger.info(f"{self.log_prefix} No output path specified, skipping export")
            return
            
        output_path = self.output_path
        if stage:
            base, ext = os.path.splitext(self.output_path)
            output_path = f"{base}_{stage}{ext}"
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            
            # Determine export format
            format = format or "json"
            
            if format == "jsonl":
                # Export as JSONL (one JSON object per line)
                if isinstance(data, list):
                    with open(output_path, 'w') as f:
                        for item in data:
                            if hasattr(item, 'model_dump'):
                                # Pydantic model
                                f.write(json.dumps(item.model_dump()) + '\n')
                            elif isinstance(item, dict):
                                # Dictionary
                                f.write(json.dumps(item) + '\n')
                            else:
                                # Convert to string
                                f.write(json.dumps(str(item)) + '\n')
                else:
                    logger.warning(f"{self.log_prefix} Data is not a list, but JSONL format was requested")
                    # Create a single-line JSONL file
                    with open(output_path, 'w') as f:
                        if hasattr(data, 'model_dump'):
                            f.write(json.dumps(data.model_dump()) + '\n')
                        elif isinstance(data, dict):
                            f.write(json.dumps(data) + '\n')
                        else:
                            f.write(json.dumps(str(data)) + '\n')
            
            elif format == "json":
                # Export as formatted JSON
                indent = 2 if self.pretty else None
                with open(output_path, 'w') as f:
                    if hasattr(data, 'model_dump_json'):
                        # Pydantic model with direct JSON serialization
                        f.write(data.model_dump_json(indent=indent))
                    elif hasattr(data, 'model_dump'):
                        # Pydantic model
                        json.dump(data.model_dump(), f, indent=indent)
                    else:
                        # Try to JSON serialize the data
                        json.dump(data, f, indent=indent, default=str)
            
            else:
                # Default to string representation
                with open(output_path, 'w') as f:
                    f.write(str(data))
            
            logger.info(f"{self.log_prefix} Exported data to {output_path}")
            
        except Exception as e:
            error_msg = f"Failed to export data to {output_path}: {str(e)}"
            self.add_error("export", error_msg)
            logger.error(f"{self.log_prefix} {error_msg}")
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Generate a summary of the pipeline execution.
        
        Returns:
            Dictionary with summary information
        """
        # Set end time if not already set
        if not self.end_time:
            self.end_time = datetime.now()
            
        # Calculate total duration
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        # Count results if available
        result_counts = {}
        
        if self.ingested_data:
            if isinstance(self.ingested_data, list):
                result_counts["ingested"] = len(self.ingested_data)
            else:
                result_counts["ingested"] = 1
        
        if self.chunked_data:
            if isinstance(self.chunked_data, list):
                result_counts["chunked"] = len(self.chunked_data)
            else:
                result_counts["chunked"] = 1
        
        # Build summary
        return {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "total_duration": total_duration,
            "executed_stages": self.executed_stages,
            "stage_timings": {
                stage: {
                    'duration': timing['duration']
                } for stage, timing in self.stage_timings.items() if timing['duration'] is not None
            },
            "result_counts": result_counts,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings)
        }
