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

# src/pulsepipe/pipelines/stages/chunking.py

"""
Chunking stage for PulsePipe pipeline.

Breaks down canonical model data into smaller, content-specific chunks
that can be embedded and stored in vector databases.
"""

from typing import Any, Dict, List, Union, Optional
import json
import os

from pulsepipe.utils.errors import ChunkerError, ConfigurationError
from pulsepipe.pipelines.chunkers.clinical_chunker import ClinicalSectionChunker
from pulsepipe.pipelines.chunkers.operational_chunker import OperationalEntityChunker
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage


class ChunkingStage(PipelineStage):
    """
    Pipeline stage that breaks down normalized data models into chunks.
    
    This stage handles:
    - Determining the content type (clinical or operational)
    - Selecting an appropriate chunker
    - Running the chunking process
    - Exporting chunks in requested formats
    """
    
    def __init__(self):
        """Initialize the chunking stage."""
        super().__init__("chunking")
        
        # Initialize chunkers
        self.clinical_chunker = ClinicalSectionChunker()
        self.operational_chunker = OperationalEntityChunker()
    
    async def execute(self, context: PipelineContext, 
                     input_data: Union[PulseClinicalContent, PulseOperationalContent, List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute the chunking process on input data.
        
        Args:
            context: Pipeline execution context
            input_data: Normalized data to chunk (clinical or operational content)
            
        Returns:
            List of chunks as dictionaries
            
        Raises:
            ChunkerError: If chunking fails
            ConfigurationError: If chunking configuration is invalid
        """
        import time
        
        # Get chunking tracker
        chunking_tracker = context.get_ingestion_tracker("chunking")
        stage_start_time = time.time()

        # Get chunker configuration
        config = self.get_stage_config(context)
        if not config:
            self.logger.warning(f"{context.log_prefix} Chunking stage is enabled but no configuration provided, using defaults")
            config = {"type": "auto", "include_metadata": True}
        
        # Check if we have input data (from previous stage or from context)
        if input_data is None:
            # Try to get data from context
            input_data = context.ingested_data
            
            # Add debug logging to help diagnose issues
            self.logger.info(f"{context.log_prefix} Input data type from context: {type(input_data)}")
            if isinstance(input_data, list):
                self.logger.info(f"{context.log_prefix} List contains {len(input_data)} items")
                if input_data:
                    self.logger.info(f"{context.log_prefix} First item type: {type(input_data[0])}")
            
            if input_data is None:
                self.logger.error(f"{context.log_prefix} No input data available for chunking!")
                raise ChunkerError(
                    "No input data available for chunking",
                    details={
                        "pipeline": context.name,
                        "executed_stages": context.executed_stages
                    }
                )
        
        chunker_type = config.get("type", "auto")
        export_format = config.get("export_chunks_to")
        include_metadata = config.get("include_metadata", True)
        
        self.logger.info(f"{context.log_prefix} Chunking data with type: {chunker_type}")
        
        all_chunks = []
        
        processing_stats = {
            "total_items": 0,
            "successful_chunks": 0,
            "failed_items": 0,
            "processing_errors": []
        }

        try:
            # Process input based on type (single item or list)
            if isinstance(input_data, list):

                processing_stats["total_items"] = len(input_data)
                # Process a batch of items
                for i, item in enumerate(input_data):
                    self.logger.info(f"{context.log_prefix} Processing batch item {i+1} of type {type(item).__name__}")
                    item_start_time = time.time()
                    try:
                        chunks = self._chunk_item(item, chunker_type, include_metadata)
                        if chunks:
                            all_chunks.extend(chunks)
                            processing_stats["successful_chunks"] += len(chunks)
                            if chunking_tracker:
                                # Record success for each chunk
                                for chunk in chunks:
                                    processing_time_ms = int((time.time() - item_start_time) * 1000)
                                    chunking_tracker.record_success(
                                        record_id=chunk.get("id", f"chunk_{i}"),
                                        record_type=chunk.get("type", "unknown_chunk"),
                                        processing_time_ms=processing_time_ms,
                                        data_source="chunking_stage",
                                        metadata={
                                            "original_item_type": type(item).__name__,
                                            "chunk_size": len(str(chunk.get("content", ""))),
                                            "chunker_type": chunker_type
                                        }
                                    )
                        else:
                            processing_stats["failed_items"] += 1
                            
                    except Exception as e:
                        processing_stats["failed_items"] += 1
                        processing_stats["processing_errors"].append(str(e))
            else:
                # Process a single item
                try:
                    self.logger.info(f"{context.log_prefix} Processing single item of type {type(input_data).__name__}")
                    chunks = self._chunk_item(input_data, chunker_type, include_metadata)
                    processing_stats["total_items"] = 1
                    item_start_time = time.time()
                    if chunks:
                        all_chunks.extend(chunks)
                        self.logger.info(f"{context.log_prefix} Chunked into {len(chunks)} sections")
                        # Record success
                        if chunking_tracker:
                            processing_time_ms = int((time.time() - item_start_time) * 1000)
                            chunking_tracker.record_success(
                                record_id=self._extract_record_id(input_data),
                                record_type=type(input_data).__name__,
                                processing_time_ms=processing_time_ms,
                                data_source="chunking_stage",
                                metadata={
                                    "chunks_created": len(chunks),
                                    "chunker_type": chunker_type
                                }
                            )
                    else:
                        self.logger.warning(f"{context.log_prefix} No chunks generated for input data")
                        processing_stats["failed_items"] = 1
        
                except Exception as e:
                        processing_stats["failed_items"] = 1
                        processing_stats["processing_errors"].append(str(e))
                        
                        # Record failure
                        if chunking_tracker:
                            processing_time_ms = int((time.time() - item_start_time) * 1000)
                            chunking_tracker.record_failure(
                                record_id=self._extract_record_id(input_data),
                                record_type=type(input_data).__name__,
                                processing_time_ms=processing_time_ms,
                                error_message=str(e),
                                data_source="chunking_stage"
                            )
                        
                        # Re-raise as ChunkerError for single item failures
                        raise ChunkerError(
                            f"Error chunking single item: {str(e)}",
                            details={"item_type": type(input_data).__name__}
                        )
    
            # Export chunks if requested format is specified
            if export_format and all_chunks:
                self.logger.info(f"{context.log_prefix} Exporting {len(all_chunks)} chunks to {export_format}")
                
                if export_format.lower() == "jsonl":
                    # If we have a specific output path in context, use it
                    chunks_output_path = context.get_output_path_for_stage("chunks", "jsonl")
                    
                    # If no output path specified, use a default
                    if not chunks_output_path:
                        output_dir = os.path.join(os.getcwd(), "output")
                        os.makedirs(output_dir, exist_ok=True)
                        chunks_output_path = os.path.join(output_dir, f"{context.name}_chunks.jsonl")
                    
                    try:
                        with open(chunks_output_path, "w") as f:
                            for chunk in all_chunks:
                                f.write(json.dumps(chunk) + "\n")
                        self.logger.info(f"{context.log_prefix} Chunked output written to {chunks_output_path}")
                    except Exception as e:
                        context.add_error("chunking", f"Failed to write chunks to {chunks_output_path}: {str(e)}")
                
                elif export_format.lower() == "json":
                    chunks_output_path = context.get_output_path_for_stage("chunks", "json")
                    
                    # If no output path specified, use a default
                    if not chunks_output_path:
                        output_dir = os.path.join(os.getcwd(), "output")
                        os.makedirs(output_dir, exist_ok=True)
                        chunks_output_path = os.path.join(output_dir, f"{context.name}_chunks.json")
                    
                    try:
                        with open(chunks_output_path, "w") as f:
                            json.dump(all_chunks, f, indent=2 if context.pretty else None)
                        self.logger.info(f"{context.log_prefix} Chunked output written to {chunks_output_path}")
                    except Exception as e:
                        context.add_error("chunking", f"Failed to write chunks to {chunks_output_path}: {str(e)}")
                
                else:
                    context.add_warning("chunking", f"Unsupported export format: {export_format}")

            # Update pipeline run totals
            if context.tracking_repository:
                context.tracking_repository.update_pipeline_run_counts(
                    run_id=context.pipeline_id,
                    total=processing_stats["total_items"],
                    successful=processing_stats["successful_chunks"],
                    failed=processing_stats["failed_items"],
                    skipped=0
                )
            total_time_ms = int((time.time() - stage_start_time) * 1000)
            # Always log the result summary
            self.logger.info(f"{context.log_prefix} Chunking complete: {processing_stats['successful_chunks']} chunks from {processing_stats['total_items']} items in {total_time_ms}ms")
            return all_chunks
            
        except Exception as e:
            self.logger.error(f"{context.log_prefix} Error during chunking: {str(e)}")
            # Record stage-level failure
            if chunking_tracker:
                total_time_ms = int((time.time() - stage_start_time) * 1000)
                chunking_tracker.record_failure(
                    record_id=f"chunking_stage_{context.pipeline_id[:8]}",
                    record_type="ChunkingStage",
                    processing_time_ms=total_time_ms,
                    error_message=str(e),
                    data_source="chunking_stage"
                )
            raise ChunkerError(
                f"Error during chunking: {str(e)}",
                details={"chunker_type": chunker_type}
            )
    
    def _chunk_item(self, item: Any, chunker_type: str, include_metadata: bool) -> Optional[List[Dict[str, Any]]]:
        """
        Chunk a single item using the appropriate chunker.
        
        Args:
            item: Item to chunk
            chunker_type: Type of chunker to use ('auto', 'clinical', 'operational')
            include_metadata: Whether to include metadata in chunks
            
        Returns:
            List of chunks or None if item couldn't be chunked
        """
        # Select chunker based on item type
        chunker = None
        if chunker_type == "auto":
            # Auto-detect based on the input type
            if isinstance(item, PulseClinicalContent):
                self.logger.info(f"Auto-detected clinical content type: {type(item).__name__}")
                chunker = ClinicalSectionChunker(include_metadata=include_metadata)
            elif isinstance(item, PulseOperationalContent):
                self.logger.info(f"Auto-detected operational content type: {type(item).__name__}")
                chunker = OperationalEntityChunker(include_metadata=include_metadata)
            else:
                self.logger.warning(f"Unable to auto-detect chunker for type: {type(item).__name__}")
                return None
                
        elif chunker_type == "clinical":
            chunker = ClinicalSectionChunker(include_metadata=include_metadata)
        elif chunker_type == "operational":
            chunker = OperationalEntityChunker(include_metadata=include_metadata)
        else:
            self.logger.warning(f"Unknown chunker type: {chunker_type}")
            return None
        
        # Apply chunking
        try:
            chunks = chunker.chunk(item)
            self.logger.info(f"Chunking successful: {len(chunks)} chunks generated")
            return chunks
        except Exception as e:
            self.logger.error(f"Error chunking item of type {type(item).__name__}: {str(e)}")
            return None
