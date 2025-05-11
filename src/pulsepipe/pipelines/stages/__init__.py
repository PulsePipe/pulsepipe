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

# src/pulsepipe/pipelines/stages/__init__.py

"""
Pipeline stages for PulsePipe.

Each stage represents a discrete step in the processing pipeline.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.errors import PulsePipeError
from pulsepipe.pipelines.context import PipelineContext


class PipelineStage(ABC):
    """
    Base class for all pipeline stages.
    
    Pipeline stages represent a discrete processing step in the data pipeline,
    such as ingestion, de-identification, chunking, embedding, etc.
    """
    
    def __init__(self, name: str):
        """
        Initialize a pipeline stage.
        
        Args:
            name: Stage name
        """
        self.name = name
        self.logger = LogFactory.get_logger(f"pipeline.stage.{name}")
    
    @abstractmethod
    async def execute(self, context: PipelineContext, input_data: Any = None) -> Any:
        """
        Execute the pipeline stage.
        
        Args:
            context: Pipeline execution context
            input_data: Input data for the stage (optional)
            
        Returns:
            Stage execution result
            
        Raises:
            PulsePipeError: If stage execution fails
        """
        pass
    
    def get_stage_config(self, context: PipelineContext) -> Dict[str, Any]:
        """
        Get configuration for this stage from the pipeline context.
        
        Args:
            context: Pipeline execution context
            
        Returns:
            Stage-specific configuration
        """
        return context.get_stage_config(self.name)
    
    def is_enabled(self, context: PipelineContext) -> bool:
        """
        Check if this stage is enabled in the pipeline configuration.
        
        Args:
            context: Pipeline execution context
            
        Returns:
            True if stage is enabled, False otherwise
        """
        return context.is_stage_enabled(self.name)


# Import specific stage implementations
from .ingestion import IngestionStage
from .chunking import ChunkingStage
from .deid import DeidentificationStage
from .embedding import EmbeddingStage
from .vectorstore import VectorStoreStage

# Make stages available at package level
__all__ = [
    "PipelineStage",
    "IngestionStage",
    "ChunkingStage",
    "DeidentificationStage", 
    "EmbeddingStage",
    "VectorStoreStage"
]
