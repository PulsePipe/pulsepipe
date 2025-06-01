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

# src/pulsepipe/pipelines/stages/ingestion.py

"""
Ingestion stage for PulsePipe pipeline.

Handles the acquisition and parsing of input data using appropriate adapters and ingesters.
"""

import asyncio
from typing import Any, Dict, Optional, Union, List

from pulsepipe.utils.errors import AdapterError, IngesterError, IngestionEngineError, ConfigurationError
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage


class IngestionStage(PipelineStage):
    """
    Pipeline stage that manages data ingestion using adapters and ingesters.
    
    This stage:
    - Creates adapter and ingester instances based on configuration
    - Runs the ingestion engine
    - Captures ingestion results
    """
    
    def __init__(self):
        """Initialize the ingestion stage."""
        super().__init__("ingestion")
    
    async def execute(self, context: PipelineContext, input_data: Any = None) -> Any:
        """
        Execute the ingestion process.
        
        Args:
            context: Pipeline execution context
            input_data: Data from previous stage (unused for ingestion)
            
        Returns:
            Ingested data (clinical or operational content)
            
        Raises:
            ConfigurationError: If adapter or ingester configuration is missing
            AdapterError: If there's an error with the adapter
            IngesterError: If there's an error with the ingester
            IngestionEngineError: If there's an error in the ingestion engine
        """
        # Get adapter and ingester configs
        adapter_config = context.config.get("adapter")
        ingester_config = context.config.get("ingester")
        
        if not adapter_config:
            raise ConfigurationError(
                "Missing adapter configuration",
                details={"pipeline": context.name}
            )
            
        if not ingester_config:
            raise ConfigurationError(
                "Missing ingester configuration",
                details={"pipeline": context.name}
            )
        
        self.logger.info(f"{context.log_prefix} Creating adapter: {adapter_config.get('type', 'unknown')}")
        self.logger.info(f"{context.log_prefix} Creating ingester: {ingester_config.get('type', 'unknown')}")
        
        try:
            # Check if we want a non-continuous processing mode
            single_scan = context.config.get("single_scan", False)
            
            # Create adapter with appropriate flags and full config for unified bookmark store
            adapter = create_adapter(adapter_config, single_scan=single_scan, full_config=context.config)
            
            # Create ingester
            ingester = create_ingester(ingester_config)
            
            # Create ingestion engine
            engine = IngestionEngine(adapter, ingester)
            
            # Determine timeout based on configuration
            timeout = None  # Default: no timeout for continuous adapters
            if adapter_config.get("type") == "file_watcher":
                continuous = adapter_config.get("continuous", True)
                if not continuous:
                    # Use timeout for one-time processing
                    timeout = context.config.get("timeout", 30.0)
                # For continuous mode, use None (no timeout) to keep the pipeline running

            self.logger.info(f"{context.log_prefix} Running ingestion engine" + 
                           (f" with timeout: {timeout}s" if timeout else " without timeout"))
            
            # Run the ingestion engine
            result = await engine.run(timeout=timeout)
        
            # Handle continuous mode empty results differently
            adapter_config = context.config.get("adapter", {})
            continuous_mode = False
            if adapter_config.get("type") == "file_watcher":
                continuous_mode = adapter_config.get("continuous", True)
                
            if not result:
                if continuous_mode:
                    self.logger.debug(f"{context.log_prefix} No data ingested yet in continuous mode")
                    return None  # Return None but don't signal completion
                else:
                    self.logger.warning(f"{context.log_prefix} No data was ingested, signaling completion anyway")
                    return None  # Signal end-of-stream to next stage
        
            # Check for and handle processing errors
            if hasattr(engine, 'processing_errors') and engine.processing_errors:
                error_count = len(engine.processing_errors)
                if error_count > 0:
                    self.logger.warning(f"{context.log_prefix} Ingestion completed with {error_count} errors")
                    
                    # Add errors to context
                    for err in engine.processing_errors[:10]:  # Limit to first 10 errors
                        context.add_error("ingestion", 
                                         err.get("message", "Unknown error"),
                                         details=err)
            
            # Log summary of results and record ingestion statistics
            ingestion_tracker = context.get_ingestion_tracker("ingestion")
            
            if isinstance(result, list):
                self.logger.info(f"{context.log_prefix} Ingested {len(result)} items")
                
                # Record ingestion statistics for each item
                if ingestion_tracker:
                    with ingestion_tracker.track_batch(f"ingestion_batch_{context.pipeline_id[:8]}") as batch:
                        for i, item in enumerate(result):
                            record_id = self._extract_record_id(item)
                            record_type = type(item).__name__
                            ingestion_tracker.record_success(
                                record_id=record_id,
                                record_type=record_type,
                                processing_time_ms=10,  # Approximate since we don't track individual timing
                                data_source="pipeline_ingestion"
                            )
                
                # Update pipeline run totals
                if context.tracking_repository:
                    context.tracking_repository.update_pipeline_run_counts(
                        run_id=context.pipeline_id,
                        total=len(result),
                        successful=len(result),
                        failed=0,
                        skipped=0
                    )
                        
            elif result is not None:
                self.logger.info(f"{context.log_prefix} Ingested 1 item of type: {type(result).__name__}")
                
                # Record single item ingestion statistics
                if ingestion_tracker:
                    with ingestion_tracker.track_batch(f"ingestion_batch_{context.pipeline_id[:8]}") as batch:
                        record_id = self._extract_record_id(result)
                        record_type = type(result).__name__
                        ingestion_tracker.record_success(
                            record_id=record_id,
                            record_type=record_type,
                            processing_time_ms=10,
                            data_source="pipeline_ingestion"
                        )
                
                # Update pipeline run totals
                if context.tracking_repository:
                    context.tracking_repository.update_pipeline_run_counts(
                        run_id=context.pipeline_id,
                        total=1,
                        successful=1,
                        failed=0,
                        skipped=0
                    )
            else:
                self.logger.warning(f"{context.log_prefix} No data was ingested")
            
            # Return the result
            return result
            
        except AdapterError as e:
            # Re-raise adapter errors
            raise
        except IngesterError as e:
            # Re-raise ingester errors
            raise
        except Exception as e:
            # Wrap other exceptions
            raise IngestionEngineError(
                f"Unexpected error in ingestion engine: {str(e)}",
                cause=e
            )
    
    def _extract_record_id(self, item: Any) -> Optional[str]:
        """
        Extract a record ID from an ingested item.
        
        Args:
            item: The ingested item
            
        Returns:
            Record ID if available, otherwise None
        """
        if hasattr(item, 'patient') and hasattr(item.patient, 'id'):
            return item.patient.id
        elif hasattr(item, 'id'):
            return str(item.id)
        elif hasattr(item, 'patient_id'):
            return item.patient_id
        else:
            return None
