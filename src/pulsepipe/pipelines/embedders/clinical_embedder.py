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

# src/pulsepipe/pipelines/embedders/clinical_embedder.py

from typing import List, Dict, Any
from .base_embedder import Embedder
from pulsepipe.utils.log_factory import LogFactory

class ClinicalEmbedder(Embedder):
    """
    Clinical embedder that uses BioClinicalBERT or other clinical-domain models
    to create embeddings specifically optimized for clinical text.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing ClinicalEmbedder")
        
        self.config = config or {}
        self.model_name = self.config.get("model_name", "emilyalsentzer/Bio_ClinicalBERT")
        self.normalize = self.config.get("normalize", True)
        
        # Initialize the model
        from sentence_transformers import SentenceTransformer
        self.logger.info(f"Loading clinical embedding model: {self.model_name}")
        self.model = SentenceTransformer(self.model_name)
        self.name = "ClinicalEmbedder"
        
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for the given texts using a clinical domain model.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors (as lists of floats)
        """
        self.logger.info(f"Embedding {len(texts)} clinical text chunks")
        
        # Handle batching for large input
        batch_size = 32  # Reasonable default batch size
        if len(texts) > batch_size:
            self.logger.info(f"Processing in batches of {batch_size}")
            
        # Convert to numpy array and then to list for consistent return type
        embeddings = self.model.encode(texts, 
                                      normalize_embeddings=self.normalize,
                                      batch_size=batch_size)
        
        self.logger.info(f"Generated {len(embeddings)} embeddings of dimension {self.dimension}")
        return embeddings.tolist()
    
    def embed_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """
        Embed a clinical chunk and add the embedding to the chunk.
        
        Args:
            chunk: A dictionary containing at least a 'content' key with text to embed
            
        Returns:
            The chunk with an added 'embedding' key
        """
        if "content" not in chunk:
            self.logger.warning("Chunk missing 'content' field, skipping embedding")
            return chunk
        content_value = chunk["content"]
        if content_value == "":
            self.logger.info("Empty content string, using space character for embedding")
            content_value = " "

        # Extract the text to embed from the chunk
        if isinstance(chunk["content"], str):
            text = chunk["content"]
        elif isinstance(chunk["content"], list) and chunk["content"]:
            # For clinical content model chunks, create a concatenated representation
            text = " ".join(str(item) for item in chunk["content"])
        else:
            text = str(chunk["content"])
        
        # Generate the embedding
        embedding = self.model.encode(text, normalize_embeddings=self.normalize)
        
        # Add the embedding to the chunk
        result = chunk.copy()
        result["embedding"] = embedding.tolist()
        result["embedding_model"] = self.model_name
        result["embedding_dim"] = self.dimension
        
        return result
        
    @property
    def dimension(self) -> int:
        """Return the dimension of the embedding vectors"""
        return self.model.get_sentence_embedding_dimension()
