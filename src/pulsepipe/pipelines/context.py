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

# src/pulsepipe/pipelines/context.py

"""
Pipeline execution context for PulsePipe.

Manages state and configuration during pipeline execution.
"""

import uuid
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import ConfigurationError
from pulsepipe.utils.config_loader import load_config
from pulsepipe.config.data_intelligence_config import load_data_intelligence_config
from pulsepipe.persistence.factory import get_tracking_repository
from pulsepipe.audit.audit_logger import AuditLogger
from pulsepipe.audit.ingestion_tracker import IngestionTracker

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
        
        # Initialize data intelligence and audit logging
        self.audit_logger = None
        self.tracking_repository = None
        self.stage_trackers = {}  # Store stage-specific trackers
        self._init_data_intelligence()
        
        logger.info(f"{self.log_prefix} Pipeline context initialized")
    
    def _init_data_intelligence(self) -> None:
        """Initialize data intelligence and audit logging components."""
        try:
            # Load data intelligence configuration from the pipeline config
            logger.debug(f"{self.log_prefix} Loading data intelligence config from pipeline config...")
            di_config = load_data_intelligence_config(self.config)
            logger.debug(f"{self.log_prefix} Data intelligence config loaded: enabled={di_config.enabled}")
            
            # Check if audit trail is enabled
            audit_enabled = di_config.is_feature_enabled('audit_trail')
            logger.debug(f"{self.log_prefix} Audit trail feature enabled check: {audit_enabled}")
            
            if audit_enabled:
                logger.info(f"{self.log_prefix} Initializing audit logging (enabled)")
                
                # Initialize database connection and tracking repository
                logger.debug(f"{self.log_prefix} Creating tracking repository...")
                self.tracking_repository = get_tracking_repository(self.config)
                logger.debug(f"{self.log_prefix} Tracking repository created successfully")
                
                # Initialize database schema
                if hasattr(self.tracking_repository.conn, 'init_schema'):
                    logger.debug(f"{self.log_prefix} Initializing database schema...")
                    self.tracking_repository.conn.init_schema()
                    logger.debug(f"{self.log_prefix} Database schema initialized")
                
                # Start pipeline run tracking
                logger.debug(f"{self.log_prefix} Starting pipeline run tracking...")
                self.tracking_repository.start_pipeline_run(
                    run_id=self.pipeline_id,
                    name=self.name,
                    config_snapshot=self.config
                )
                logger.debug(f"{self.log_prefix} Pipeline run tracking started")
                
                # Initialize audit logger
                logger.debug(f"{self.log_prefix} Creating audit logger...")
                self.audit_logger = AuditLogger(
                    pipeline_run_id=self.pipeline_id,
                    config=di_config,
                    repository=self.tracking_repository
                )
                logger.debug(f"{self.log_prefix} Audit logger created")
                
                # Store data intelligence config for later use by stage trackers
                self.data_intelligence_config = di_config
                logger.debug(f"{self.log_prefix} Data intelligence config stored for stage trackers")
                
                # Log pipeline started event
                logger.debug(f"{self.log_prefix} Logging pipeline started event...")
                self.audit_logger.log_pipeline_started(self.name, self.config)
                logger.debug(f"{self.log_prefix} Pipeline started event logged")
                
                logger.info(f"{self.log_prefix} Data intelligence initialized successfully")
            else:
                logger.info(f"{self.log_prefix} Audit logging disabled in configuration")
                
        except Exception as e:
            logger.warning(f"{self.log_prefix} Failed to initialize data intelligence: {e}")
            # Continue without audit logging rather than failing the pipeline
            self.audit_logger = None
            self.tracking_repository = None
            self.data_intelligence_config = None
    
    def start_stage(self, stage_name: str) -> None:
        """Mark the start of a pipeline stage for timing purposes."""
        logger.info(f"{self.log_prefix} Starting stage: {stage_name}")
        self.stage_timings[stage_name] = {
            'start': datetime.now(),
            'end': None,
            'duration': None
        }
        
        # Log stage started event to audit trail
        if self.audit_logger:
            self.audit_logger.log_stage_started(stage_name)
        
        # Create stage-specific ingestion tracker for ingestion stages
        if stage_name == "ingestion" and self.data_intelligence_config and self.tracking_repository:
            if self.data_intelligence_config.is_feature_enabled('ingestion_tracking'):
                try:
                    self.stage_trackers[stage_name] = IngestionTracker(
                        pipeline_run_id=self.pipeline_id,
                        stage_name=stage_name,
                        config=self.data_intelligence_config,
                        repository=self.tracking_repository
                    )
                    logger.debug(f"{self.log_prefix} Created ingestion tracker for stage: {stage_name}")
                except Exception as e:
                    logger.warning(f"{self.log_prefix} Failed to create ingestion tracker for {stage_name}: {e}")
    
    def get_ingestion_tracker(self, stage_name: str) -> Optional[IngestionTracker]:
        """
        Get the ingestion tracker for a specific stage.
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            IngestionTracker instance or None if not available
        """
        return self.stage_trackers.get(stage_name)
    
    def end_stage(self, stage_name: str, result: Any = None) -> None:
        """
        Mark the end of a pipeline stage and store its result.
        
        Args:
            stage_name: Name of the stage that completed
            result: Result data from the stage execution
        """
        end_time = datetime.now()
        duration = 0.0  # Default duration for untracked stages
        
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
        
        # Log stage completed event to audit trail
        if self.audit_logger:
            self.audit_logger.log_stage_completed(stage_name, duration)
    
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
        
        # Log error event to audit trail
        if self.audit_logger:
            self.audit_logger.log_error(stage, message, details)
    
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
        
        # Log warning event to audit trail
        if self.audit_logger:
            self.audit_logger.log_warning(stage, message, details)
    

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
    
    def export_results(self, data, output_type=None, format=None):
        """
        Export pipeline results to the configured output path.
        
        Args:
            data: The data to export
            output_type: Type of output (ingestion, chunking, etc.)
            format: Output format (json, csv, etc.)
        """
        if not self.output_path:
            return
            
        # Import PlatformPath
        from pulsepipe.utils.path_normalizer import PlatformPath
        
        # Determine output path
        if output_type:
            base_name = os.path.splitext(self.output_path)[0]
            ext = f".{format}" if format else os.path.splitext(self.output_path)[1]
            suffix = f"_{output_type}" if output_type else ""
            output_path = f"{base_name}{suffix}{ext}"
        else:
            output_path = self.output_path
            
        # Normalize path for the current platform
        platform_path = PlatformPath()
        normalized_path = platform_path.normalize_path(output_path)
        
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(os.path.abspath(normalized_path))
        os.makedirs(dir_path, exist_ok=True)
        
        # Write data to file
        with open(normalized_path, "w", encoding='utf-8') as f:
            if hasattr(data, 'model_dump_json'):
                # If it's a Pydantic model, use its JSON serialization
                f.write(data.model_dump_json(indent=2))
            else:
                # Otherwise use standard JSON serialization
                json.dump(data, f, indent=2)


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
        
        # Log pipeline completion to audit trail
        if self.audit_logger:
            if self.errors:
                self.audit_logger.log_pipeline_failed(self.name, f"Pipeline completed with {len(self.errors)} errors")
            else:
                self.audit_logger.log_pipeline_completed(self.name, total_duration)
        
        # Complete pipeline run tracking
        if self.tracking_repository:
            status = "failed" if self.errors else "completed"
            error_message = f"Pipeline completed with {len(self.errors)} errors" if self.errors else None
            self.tracking_repository.complete_pipeline_run(
                run_id=self.pipeline_id,
                status=status,
                error_message=error_message
            )
        
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