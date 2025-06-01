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

# src/pulsepipe/pipelines/stages/vectorstore.py

"""
VectorStore stage for PulsePipe pipeline.

Uploads embedded healthcare data chunks to vector databases 
to enable semantic search and retrieval.
"""

from typing import Any, Dict, List, Union, Optional
import json
import os
import uuid

from pulsepipe.utils.errors import VectorStoreError, ConfigurationError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage
from pulsepipe.pipelines.vectorstore import (
    VectorStore, WeaviateVectorStore, QdrantVectorStore,
    VectorStoreConnectionError
)

class VectorStoreStage(PipelineStage):
    """
    Pipeline stage that loads embeddings into a vector database.
    
    This stage handles:
    - Connecting to the chosen vector database
    - Creating/updating collections or indices
    - Uploading embedded chunks
    """
    
    def __init__(self):
        """Initialize the vectorstore stage."""
        super().__init__("vectorstore")
        
        # Registry of supported vector stores
        self.vectorstore_registry = {
            "weaviate": WeaviateVectorStore,
            "qdrant": QdrantVectorStore,
        }
    
    async def execute(self, context: PipelineContext, embedded_chunks: List[Dict[str, Any]] = None) -> Any:
        """
        Execute the vector store loading process.
        
        Args:
            context: Pipeline execution context
            embedded_chunks: Embedded chunks (from previous stage)
            
        Returns:
            Summary information about the upload operation
            
        Raises:
            VectorStoreError: If vector store operation fails
            ConfigurationError: If vector store configuration is invalid
        """
        import time
        
        # Get vectorstore tracker
        vectorstore_tracker = context.get_vector_db_tracker("vectorstore")
        stage_start_time = time.time()
        
        # Get vectorstore configuration
        config = self.get_stage_config(context)
        
        # Also check the top-level vectorstore config (common pattern in the codebase)
        if not config and "vectorstore" in context.config:
            config = context.config["vectorstore"]
        
        if not config:
            self.logger.warning(f"{context.log_prefix} VectorStore stage is enabled but no configuration provided")
            raise ConfigurationError(
                "Missing vectorstore configuration",
                details={"pipeline": context.name}
            )
        
        # Check if vectorstore is explicitly disabled
        if config.get("enabled") is False:
            self.logger.info(f"{context.log_prefix} VectorStore is disabled in configuration, skipping")
            return {"status": "skipped", "reason": "disabled in configuration"}
        
        # Check if we have input data (from previous stage or from context)
        if embedded_chunks is None:
            # Try to get data from context
            embedded_chunks = context.embedded_data
            
            if embedded_chunks is None:
                self.logger.error(f"{context.log_prefix} No embedded chunks available for vectorstore!")
                raise VectorStoreError(
                    "No embedded chunks available for vectorstore",
                    details={
                        "pipeline": context.name,
                        "executed_stages": context.executed_stages
                    }
                )
        
        engine = config.get("engine", "weaviate").lower()
        host = config.get("host", "http://localhost")
        port = config.get("port", 8080)
        namespace_prefix = config.get("namespace_prefix", "pulsepipe")
        
        self.logger.info(f"{context.log_prefix} Using vector store: {engine} at {host}")
        
        processing_stats = {
            "total_chunks": len(embedded_chunks),
            "successful_uploads": 0,
            "failed_uploads": 0,
            "collections_created": 0,
            "processing_errors": []
        }
        
        try:
            # Validate configuration
            if engine not in self.vectorstore_registry:
                available_engines = list(self.vectorstore_registry.keys())
                raise ConfigurationError(
                    f"Unsupported vector store engine: {engine}",
                    details={
                        "available_engines": available_engines
                    }
                )
            
            # Create vectorstore instance
            vectorstore_class = self.vectorstore_registry[engine]
            vectorstore = vectorstore_class(url=host)
            
            # Process and upload chunks
            result_summary = await self._upload_chunks(
                vectorstore=vectorstore,
                chunks=embedded_chunks,
                namespace_prefix=namespace_prefix,
                context=context,
                processing_stats=processing_stats,
                vectorstore_tracker=vectorstore_tracker
            )
            
            # Update pipeline run totals
            if context.tracking_repository:
                context.tracking_repository.update_pipeline_run_counts(
                    run_id=context.pipeline_id,
                    total=processing_stats["total_chunks"],
                    successful=processing_stats["successful_uploads"],
                    failed=processing_stats["failed_uploads"],
                    skipped=0
                )
            
            total_time_ms = int((time.time() - stage_start_time) * 1000)
            self.logger.info(f"{context.log_prefix} Vector store upload complete: {result_summary}")
            
            # Log audit event if audit logger is available
            if context.audit_logger:
                context.audit_logger.log_stage_completed(
                    "vectorstore",
                    details={
                        "engine": engine,
                        "host": host,
                        "namespace_prefix": namespace_prefix,
                        "total_chunks": processing_stats["total_chunks"],
                        "successful_uploads": processing_stats["successful_uploads"],
                        "failed_uploads": processing_stats["failed_uploads"],
                        "collections_created": processing_stats["collections_created"],
                        "processing_time_ms": total_time_ms
                    }
                )
            
            return result_summary
            
        except VectorStoreConnectionError as e:
            # Record stage-level failure if tracker is available
            if vectorstore_tracker:
                from pulsepipe.audit.vector_db_tracker import VectorDbStage
                total_time_ms = int((time.time() - stage_start_time) * 1000)
                vectorstore_tracker.record_failure(
                    record_id=f"vectorstore_stage_{context.pipeline_id[:8]}",
                    error=e,
                    stage=VectorDbStage.CONNECTION,
                    source_id=context.pipeline_id,
                    content_type="vectorstore_stage",
                    processing_time_ms=total_time_ms,
                    vector_store_type=engine,
                    metadata={
                        "error_type": "connection_error",
                        "host": host,
                        "port": port
                    }
                )
            
            # Log audit event if audit logger is available
            if context.audit_logger:
                context.audit_logger.log_stage_failed("vectorstore", e, details={
                    "engine": engine,
                    "host": host,
                    "port": port,
                    "error_type": "connection_error"
                })
            
            self.logger.error(f"{context.log_prefix} Failed to connect to vector store: {str(e)}")
            raise VectorStoreError(
                f"Failed to connect to {engine} at {host}:{port}",
                details={
                    "engine": engine,
                    "host": host,
                    "port": port
                },
                cause=e
            )
        except Exception as e:
            # Record stage-level failure if tracker is available
            if vectorstore_tracker:
                from pulsepipe.audit.vector_db_tracker import VectorDbStage
                total_time_ms = int((time.time() - stage_start_time) * 1000)
                vectorstore_tracker.record_failure(
                    record_id=f"vectorstore_stage_{context.pipeline_id[:8]}",
                    error=e,
                    stage=VectorDbStage.VECTOR_INSERTION,
                    source_id=context.pipeline_id,
                    content_type="vectorstore_stage",
                    processing_time_ms=total_time_ms,
                    vector_store_type=engine,
                    metadata={
                        "error_type": "general_error"
                    }
                )
            
            # Log audit event if audit logger is available
            if context.audit_logger:
                context.audit_logger.log_stage_failed("vectorstore", e, details={"engine": engine})
            
            self.logger.error(f"{context.log_prefix} Error during vector store operation: {str(e)}")
            raise VectorStoreError(
                f"Error during vector store operation: {str(e)}",
                details={"engine": engine}
            )
    
    async def _upload_chunks(self, 
                            vectorstore: VectorStore,
                            chunks: List[Dict[str, Any]],
                            namespace_prefix: str,
                            context: PipelineContext,
                            processing_stats: Dict[str, Any] = None,
                            vectorstore_tracker: Optional[Any] = None) -> Dict[str, Any]:
        """
        Upload chunks to the vector store.
        
        Args:
            vectorstore: Vector store instance
            chunks: Embedded chunks to upload
            namespace_prefix: Prefix for collection/index names
            context: Pipeline context
            processing_stats: Statistics tracking dictionary
            vectorstore_tracker: Tracker for vectorstore operations
            
        Returns:
            Summary information about the upload operation
        """
        import time
        
        if processing_stats is None:
            processing_stats = {
                "total_chunks": len(chunks),
                "successful_uploads": 0,
                "failed_uploads": 0,
                "collections_created": 0,
                "processing_errors": []
            }
        
        # Group chunks by type
        groups = {}
        for chunk in chunks:
            chunk_type = chunk.get("type", "unknown")
            if chunk_type not in groups:
                groups[chunk_type] = []
            groups[chunk_type].append(chunk)
        
        self.logger.info(f"{context.log_prefix} Uploading {len(chunks)} chunks in {len(groups)} groups")
        
        # Upload each group as a separate collection
        upload_results = {}
        
        for chunk_type, type_chunks in groups.items():
            namespace = f"{namespace_prefix}_{chunk_type}"
            collection_start_time = time.time()
            
            # Add upload IDs if not present
            for i, chunk in enumerate(type_chunks):
                if "id" not in chunk:
                    # Generate a deterministic ID based on content
                    chunk_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{chunk_type}_{i}_{str(chunk.get('content', ''))}"))
                    chunk["id"] = chunk_id
            
            try:
                self.logger.info(f"{context.log_prefix} Uploading {len(type_chunks)} chunks to {namespace}")
                vectorstore.upsert(namespace, type_chunks)
                
                processing_stats["successful_uploads"] += len(type_chunks)
                processing_stats["collections_created"] += 1
                
                upload_results[chunk_type] = {
                    "success": True,
                    "count": len(type_chunks)
                }
                
                # Record success for collection upload if tracker is available
                if vectorstore_tracker:
                    processing_time_ms = int((time.time() - collection_start_time) * 1000)
                    vectorstore_tracker.record_success(
                        record_id=f"collection_{namespace}",
                        source_id=context.pipeline_id,
                        content_type=chunk_type,
                        processing_time_ms=processing_time_ms,
                        vector_count=len(type_chunks),
                        index_name=namespace,
                        collection_name=namespace,
                        vector_store_type=vectorstore.__class__.__name__,
                        metadata={
                            "namespace": namespace,
                            "chunk_type": chunk_type,
                            "chunk_count": len(type_chunks),
                            "vectorstore_engine": vectorstore.__class__.__name__
                        }
                    )
                
                # Log audit event for collection upload if audit logger is available
                if context.audit_logger:
                    context.audit_logger.log_record_processed(
                        stage_name="vectorstore",
                        record_id=f"collection_{namespace}",
                        record_type="VectorStoreCollection",
                        processing_time_ms=int((time.time() - collection_start_time) * 1000),
                        details={
                            "namespace": namespace,
                            "chunk_type": chunk_type,
                            "chunk_count": len(type_chunks),
                            "vectorstore_engine": vectorstore.__class__.__name__
                        }
                    )
                
                # Log individual chunk uploads for detailed tracking
                for chunk in type_chunks:
                    chunk_id = chunk.get("id", "unknown")
                    
                    # Log audit event for each chunk if audit logger is available
                    if context.audit_logger:
                        context.audit_logger.log_record_processed(
                            stage_name="vectorstore",
                            record_id=chunk_id,
                            record_type=f"VectorStoreChunk_{chunk_type}",
                            processing_time_ms=1,  # Approximate since we don't track individual timing
                            details={
                                "namespace": namespace,
                                "chunk_type": chunk_type,
                                "has_embedding": "embedding" in chunk,
                                "vectorstore_engine": vectorstore.__class__.__name__
                            }
                        )
                
            except Exception as e:
                processing_stats["failed_uploads"] += len(type_chunks)
                processing_stats["processing_errors"].append(str(e))
                
                self.logger.error(f"{context.log_prefix} Error uploading to {namespace}: {str(e)}")
                upload_results[chunk_type] = {
                    "success": False,
                    "error": str(e),
                    "count": 0
                }
                
                # Record failure for collection upload if tracker is available
                if vectorstore_tracker:
                    from pulsepipe.audit.vector_db_tracker import VectorDbStage
                    processing_time_ms = int((time.time() - collection_start_time) * 1000)
                    vectorstore_tracker.record_failure(
                        record_id=f"collection_{namespace}",
                        error=e,
                        stage=VectorDbStage.VECTOR_INSERTION,
                        source_id=context.pipeline_id,
                        content_type=chunk_type,
                        processing_time_ms=processing_time_ms,
                        vector_store_type=vectorstore.__class__.__name__,
                        metadata={
                            "namespace": namespace,
                            "chunk_type": chunk_type,
                            "chunk_count": len(type_chunks)
                        }
                    )
                
                # Log audit event for collection upload failure if audit logger is available
                if context.audit_logger:
                    context.audit_logger.log_record_failed(
                        stage_name="vectorstore",
                        record_id=f"collection_{namespace}",
                        error=e,
                        details={
                            "namespace": namespace,
                            "chunk_type": chunk_type,
                            "chunk_count": len(type_chunks),
                            "vectorstore_engine": vectorstore.__class__.__name__
                        }
                    )
        
        return {
            "total_uploaded": processing_stats["successful_uploads"],
            "total_chunks": len(chunks),
            "collections": list(upload_results.keys()),
            "details": upload_results
        }
