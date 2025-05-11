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

# src/pulsepipe/pipelines/stages/embedding.py

"""
Embedding stage for PulsePipe pipeline.

Creates vector embeddings from chunked healthcare data 
to enable semantic search and retrieval.
"""

from typing import Any, Dict, List
import asyncio
import json
import os

from pulsepipe.utils.errors import EmbedderError, ConfigurationError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage
from pulsepipe.pipelines.embedders import EMBEDDER_REGISTRY

class EmbeddingStage(PipelineStage):
    """
    Pipeline stage that creates embeddings for chunked data.
    
    This stage handles:
    - Selecting an appropriate embedder based on data type
    - Running the embedding process on chunks
    - Standardizing embedder outputs
    """
    
    def __init__(self):
        """Initialize the embedding stage."""
        super().__init__("embedding")
    
    async def execute(self, context: PipelineContext, chunked_data: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute the embedding process on input chunks.
        
        Args:
            context: Pipeline execution context
            chunked_data: Chunks to embed (from previous stage)
            
        Returns:
            List of chunks with embeddings added
            
        Raises:
            EmbedderError: If embedding fails
            ConfigurationError: If embedding configuration is invalid
        """
        # Get embedder configuration
        config = self.get_stage_config(context)
        if not config:
            self.logger.warning(f"{context.log_prefix} Embedding stage is enabled but no configuration provided, using defaults")
            config = {"type": "clinical", "model_name": "all-MiniLM-L6-v2"}
        
        # Check if we have input data (from previous stage or from context)
        if chunked_data is None:
            # Try to get data from context
            chunked_data = context.chunked_data
            
            if chunked_data is None:
                self.logger.error(f"{context.log_prefix} No chunked data available for embedding!")
                raise EmbedderError(
                    "No chunked data available for embedding",
                    details={
                        "pipeline": context.name,
                        "executed_stages": context.executed_stages
                    }
                )
        
        embedder_type = config.get("type", "clinical")
        
        self.logger.info(f"{context.log_prefix} Embedding data with type: {embedder_type}")
        
        try:
            # Select embedder type
            if embedder_type not in EMBEDDER_REGISTRY:
                self.logger.warning(f"{context.log_prefix} Unknown embedder type: {embedder_type}, falling back to clinical")
                embedder_type = "clinical"
            
            # Create embedder instance
            embedder_class = EMBEDDER_REGISTRY[embedder_type]
            embedder = embedder_class(config)
            
            self.logger.info(f"{context.log_prefix} Using embedder: {embedder.name} ({embedder_type})")
            
            # Process chunks
            result_chunks = []
            chunk_count = len(chunked_data)
            
            if chunk_count == 0:
                self.logger.warning(f"{context.log_prefix} No chunks to embed")
                return []
            
            self.logger.info(f"{context.log_prefix} Embedding {chunk_count} chunks")
            
            # Process chunks in batches to avoid memory issues
            batch_size = 20  # Can be tuned based on model size and available memory
            
            for i in range(0, chunk_count, batch_size):
                batch = chunked_data[i:i+batch_size]
                self.logger.info(f"{context.log_prefix} Processing batch {i//batch_size + 1}/{(chunk_count-1)//batch_size + 1} ({len(batch)} chunks)")
                
                # Process each chunk in the batch
                batch_results = []
                for chunk in batch:
                    try:
                        # Embed chunk
                        embedded_chunk = embedder.embed_chunk(chunk)
                        batch_results.append(embedded_chunk)
                    except Exception as e:
                        self.logger.error(f"{context.log_prefix} Error embedding chunk: {str(e)}")
                        # Continue with other chunks
                
                result_chunks.extend(batch_results)
                self.logger.info(f"{context.log_prefix} Completed batch {i//batch_size + 1}")
            
            # Export embeddings if requested
            export_format = config.get("export_embeddings_to")
            if export_format and result_chunks:
                self.logger.info(f"{context.log_prefix} Exporting {len(result_chunks)} embeddings to {export_format}")
                
                if export_format.lower() == "jsonl":
                    embeddings_output_path = context.get_output_path_for_stage("embeddings", "jsonl")
                    
                    # If no output path specified, use a default
                    if not embeddings_output_path:
                        output_dir = os.path.join(os.getcwd(), "output")
                        os.makedirs(output_dir, exist_ok=True)
                        embeddings_output_path = os.path.join(output_dir, f"{context.name}_embeddings.jsonl")
                    
                    try:
                        with open(embeddings_output_path, "w") as f:
                            for chunk in result_chunks:
                                f.write(json.dumps(chunk) + "\n")
                        self.logger.info(f"{context.log_prefix} Embeddings written to {embeddings_output_path}")
                    except Exception as e:
                        context.add_error("embedding", f"Failed to write embeddings to {embeddings_output_path}: {str(e)}")
                
                elif export_format.lower() == "json":
                    embeddings_output_path = context.get_output_path_for_stage("embeddings", "json")
                    
                    # If no output path specified, use a default
                    if not embeddings_output_path:
                        output_dir = os.path.join(os.getcwd(), "output")
                        os.makedirs(output_dir, exist_ok=True)
                        embeddings_output_path = os.path.join(output_dir, f"{context.name}_embeddings.json")
                    
                    try:
                        with open(embeddings_output_path, "w") as f:
                            json.dump(result_chunks, f, indent=2 if context.pretty else None)
                        self.logger.info(f"{context.log_prefix} Embeddings written to {embeddings_output_path}")
                    except Exception as e:
                        context.add_error("embedding", f"Failed to write embeddings to {embeddings_output_path}: {str(e)}")
                
                else:
                    context.add_warning("embedding", f"Unsupported export format: {export_format}")
            
            self.logger.info(f"{context.log_prefix} Embedding complete. Processed {len(result_chunks)} chunks")
            return result_chunks
            
        except Exception as e:
            self.logger.error(f"{context.log_prefix} Error during embedding: {str(e)}")
            raise EmbedderError(
                f"Error during embedding: {str(e)}",
                details={"embedder_type": embedder_type}
            )
