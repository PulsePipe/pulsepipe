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

"""
Tests for audit logging in de-identification, embedding, and vectorstore stages.

Verifies that audit logging is properly implemented and records appropriate
audit events for the remaining pipeline stages.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from pulsepipe.pipelines.stages.deid import DeidentificationStage
from pulsepipe.pipelines.stages.embedding import EmbeddingStage
from pulsepipe.pipelines.stages.vectorstore import VectorStoreStage
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.audit.audit_logger import AuditLogger, EventType, AuditLevel
from pulsepipe.audit.ingestion_tracker import IngestionTracker
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.patient import PatientInfo
from pulsepipe.models.note import Note


class TestDeidStageAuditLogging:
    """Tests for audit logging in the de-identification stage."""
    
    @pytest.fixture
    def mock_context_with_audit(self):
        """Create a mock context with audit logging enabled."""
        context = MagicMock(spec=PipelineContext)
        context.log_prefix = "[test_deid]"
        context.name = "test_pipeline"
        context.pipeline_id = "test-pipeline-123"
        context.executed_stages = ["ingestion"]
        
        # Mock audit logger
        audit_logger = MagicMock(spec=AuditLogger)
        context.audit_logger = audit_logger
        
        # Mock ingestion tracker
        ingestion_tracker = MagicMock(spec=IngestionTracker)
        context.get_ingestion_tracker = MagicMock(return_value=ingestion_tracker)
        
        # Mock tracking repository
        tracking_repository = MagicMock()
        context.tracking_repository = tracking_repository
        
        return context
    
    @pytest.fixture
    def deid_stage(self):
        """Create a de-identification stage instance."""
        return DeidentificationStage()
    
    @pytest.fixture
    def sample_clinical_content(self):
        """Create sample clinical content for testing."""
        patient = PatientInfo(
            id="patient-123",
            gender="female",
            dob_year=1980,
            over_90=False,
            geographic_area="New York, NY",
            preferences=None
        )
        
        note = Note(
            note_type_code="PN",
            text="Patient visit on 2023-05-15",
            patient_id="patient-123"
        )
        
        return PulseClinicalContent(
            patient=patient,
            encounter=None,
            notes=[note]
        )
    
    @pytest.mark.asyncio
    async def test_deid_stage_logs_successful_processing(self, deid_stage, mock_context_with_audit, sample_clinical_content):
        """Test that successful de-identification logs appropriate audit events."""
        # Set up the stage configuration
        deid_stage.get_stage_config = MagicMock(return_value={
            "method": "safe_harbor",
            "patient_id_strategy": "hash"
        })
        
        # Execute the stage
        result = await deid_stage.execute(mock_context_with_audit, sample_clinical_content)
        
        # Verify audit logging was called
        audit_logger = mock_context_with_audit.audit_logger
        audit_logger.log_record_processed.assert_called()
        
        # Check the audit log call details
        call_args = audit_logger.log_record_processed.call_args
        assert call_args[1]["stage_name"] == "deid"
        assert call_args[1]["record_id"] == "patient-123"
        assert call_args[1]["record_type"] == "PulseClinicalContent"
        assert "deid_method" in call_args[1]["details"]
        
        # Verify tracking repository was called
        tracking_repo = mock_context_with_audit.tracking_repository
        tracking_repo.update_pipeline_run_counts.assert_called_with(
            run_id="test-pipeline-123",
            total=1,
            successful=1,
            failed=0,
            skipped=0
        )
    
    @pytest.mark.asyncio
    async def test_deid_stage_logs_batch_processing(self, deid_stage, mock_context_with_audit, sample_clinical_content):
        """Test that batch de-identification logs audit events for each item."""
        # Set up the stage configuration
        deid_stage.get_stage_config = MagicMock(return_value={
            "method": "safe_harbor",
            "patient_id_strategy": "hash"
        })
        
        # Create a batch of items
        batch_data = [sample_clinical_content, sample_clinical_content]
        
        # Execute the stage
        result = await deid_stage.execute(mock_context_with_audit, batch_data)
        
        # Verify audit logging was called for each item
        audit_logger = mock_context_with_audit.audit_logger
        assert audit_logger.log_record_processed.call_count == 2
        
        # Verify tracking repository was called with correct totals
        tracking_repo = mock_context_with_audit.tracking_repository
        tracking_repo.update_pipeline_run_counts.assert_called_with(
            run_id="test-pipeline-123",
            total=2,
            successful=2,
            failed=0,
            skipped=0
        )
    
    @pytest.mark.asyncio
    async def test_deid_stage_logs_failure(self, deid_stage, mock_context_with_audit):
        """Test that de-identification failures are properly logged."""
        # Set up the stage configuration
        deid_stage.get_stage_config = MagicMock(return_value={
            "method": "safe_harbor"
        })
        
        # Mock the _deid_item method to raise an exception
        deid_stage._deid_item = MagicMock(side_effect=Exception("Test error"))
        
        # Create test data
        test_data = {"id": "test-123"}
        
        # Verify that the stage raises the expected exception
        with pytest.raises(Exception):
            await deid_stage.execute(mock_context_with_audit, test_data)
        
        # Verify failure audit logging was called
        audit_logger = mock_context_with_audit.audit_logger
        audit_logger.log_record_failed.assert_called()
        
        # Check the failure audit log call details
        call_args = audit_logger.log_record_failed.call_args
        assert call_args[1]["stage_name"] == "deid"


class TestEmbeddingStageAuditLogging:
    """Tests for audit logging in the embedding stage."""
    
    @pytest.fixture
    def mock_context_with_audit(self):
        """Create a mock context with audit logging enabled."""
        context = MagicMock(spec=PipelineContext)
        context.log_prefix = "[test_embedding]"
        context.name = "test_pipeline"
        context.pipeline_id = "test-pipeline-123"
        context.executed_stages = ["ingestion", "chunking"]
        
        # Mock audit logger
        audit_logger = MagicMock(spec=AuditLogger)
        context.audit_logger = audit_logger
        
        # Mock ingestion tracker
        ingestion_tracker = MagicMock(spec=IngestionTracker)
        context.get_ingestion_tracker = MagicMock(return_value=ingestion_tracker)
        
        # Mock tracking repository
        tracking_repository = MagicMock()
        context.tracking_repository = tracking_repository
        
        return context
    
    @pytest.fixture
    def embedding_stage(self):
        """Create an embedding stage instance."""
        return EmbeddingStage()
    
    @pytest.fixture
    def sample_chunks(self):
        """Create sample chunks for testing."""
        return [
            {
                "id": "chunk-1",
                "type": "clinical",
                "content": "Patient has diabetes",
                "metadata": {"source": "note"}
            },
            {
                "id": "chunk-2", 
                "type": "clinical",
                "content": "Blood pressure is normal",
                "metadata": {"source": "note"}
            }
        ]
    
    @pytest.mark.asyncio
    async def test_embedding_stage_logs_successful_processing(self, embedding_stage, mock_context_with_audit, sample_chunks):
        """Test that successful embedding logs appropriate audit events."""
        # Mock the embedder registry and embedder
        mock_embedder = MagicMock()
        mock_embedder.name = "TestEmbedder"
        mock_embedder.embed_chunk = MagicMock(side_effect=lambda chunk: {**chunk, "embedding": [0.1, 0.2, 0.3]})
        
        with patch('pulsepipe.pipelines.stages.embedding.EMBEDDER_REGISTRY', {"clinical": MagicMock(return_value=mock_embedder)}):
            # Set up the stage configuration
            embedding_stage.get_stage_config = MagicMock(return_value={
                "type": "clinical",
                "model_name": "test-model"
            })
            
            # Execute the stage
            result = await embedding_stage.execute(mock_context_with_audit, sample_chunks)
            
            # Verify audit logging was called for each chunk
            audit_logger = mock_context_with_audit.audit_logger
            assert audit_logger.log_record_processed.call_count == 2
            
            # Check the audit log call details
            call_args_list = audit_logger.log_record_processed.call_args_list
            assert call_args_list[0][1]["stage_name"] == "embedding"
            assert call_args_list[0][1]["record_id"] == "chunk-1"
            assert call_args_list[0][1]["record_type"] == "clinical"
            assert "embedder_type" in call_args_list[0][1]["details"]
            assert "embedder_name" in call_args_list[0][1]["details"]
            
            # Verify tracking repository was called
            tracking_repo = mock_context_with_audit.tracking_repository
            tracking_repo.update_pipeline_run_counts.assert_called_with(
                run_id="test-pipeline-123",
                total=2,
                successful=2,
                failed=0,
                skipped=0
            )
    
    @pytest.mark.asyncio
    async def test_embedding_stage_logs_chunk_failure(self, embedding_stage, mock_context_with_audit, sample_chunks):
        """Test that embedding failures for individual chunks are properly logged."""
        # Mock the embedder to fail on the second chunk
        mock_embedder = MagicMock()
        mock_embedder.name = "TestEmbedder"
        def mock_embed_chunk(chunk):
            if chunk["id"] == "chunk-2":
                raise Exception("Embedding failed")
            return {**chunk, "embedding": [0.1, 0.2, 0.3]}
        mock_embedder.embed_chunk = MagicMock(side_effect=mock_embed_chunk)
        
        with patch('pulsepipe.pipelines.stages.embedding.EMBEDDER_REGISTRY', {"clinical": MagicMock(return_value=mock_embedder)}):
            # Set up the stage configuration
            embedding_stage.get_stage_config = MagicMock(return_value={
                "type": "clinical",
                "model_name": "test-model"
            })
            
            # Execute the stage
            result = await embedding_stage.execute(mock_context_with_audit, sample_chunks)
            
            # Verify both success and failure audit logging were called
            audit_logger = mock_context_with_audit.audit_logger
            audit_logger.log_record_processed.assert_called()  # For chunk-1
            audit_logger.log_record_failed.assert_called()     # For chunk-2
            
            # Verify tracking repository was called with correct counts
            tracking_repo = mock_context_with_audit.tracking_repository
            tracking_repo.update_pipeline_run_counts.assert_called_with(
                run_id="test-pipeline-123",
                total=2,
                successful=1,
                failed=1,
                skipped=0
            )


class TestVectorStoreStageAuditLogging:
    """Tests for audit logging in the vector store stage."""
    
    @pytest.fixture
    def mock_context_with_audit(self):
        """Create a mock context with audit logging enabled."""
        context = MagicMock(spec=PipelineContext)
        context.log_prefix = "[test_vectorstore]"
        context.name = "test_pipeline"
        context.pipeline_id = "test-pipeline-123"
        context.executed_stages = ["ingestion", "chunking", "embedding"]
        
        # Mock audit logger
        audit_logger = MagicMock(spec=AuditLogger)
        context.audit_logger = audit_logger
        
        # Mock ingestion tracker
        ingestion_tracker = MagicMock(spec=IngestionTracker)
        context.get_ingestion_tracker = MagicMock(return_value=ingestion_tracker)
        
        # Mock tracking repository
        tracking_repository = MagicMock()
        context.tracking_repository = tracking_repository
        
        return context
    
    @pytest.fixture
    def vectorstore_stage(self):
        """Create a vector store stage instance."""
        return VectorStoreStage()
    
    @pytest.fixture
    def sample_embedded_chunks(self):
        """Create sample embedded chunks for testing."""
        return [
            {
                "id": "chunk-1",
                "type": "clinical",
                "content": "Patient has diabetes",
                "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
            },
            {
                "id": "chunk-2",
                "type": "operational", 
                "content": "Billing code 123.45",
                "embedding": [0.6, 0.7, 0.8, 0.9, 1.0]
            }
        ]
    
    @pytest.mark.asyncio
    async def test_vectorstore_stage_logs_successful_upload(self, vectorstore_stage, mock_context_with_audit, sample_embedded_chunks):
        """Test that successful vector store upload logs appropriate audit events."""
        # Mock the vector store
        mock_vectorstore = MagicMock()
        mock_vectorstore.__class__.__name__ = "MockVectorStore"
        mock_vectorstore.upsert = MagicMock()
        
        # Mock the vector store registry
        vectorstore_stage.vectorstore_registry = {
            "weaviate": MagicMock(return_value=mock_vectorstore)
        }
        
        # Set up the stage configuration
        vectorstore_stage.get_stage_config = MagicMock(return_value={
            "engine": "weaviate",
            "host": "http://localhost:8080",
            "namespace_prefix": "test_pulse"
        })
        
        # Execute the stage
        result = await vectorstore_stage.execute(mock_context_with_audit, sample_embedded_chunks)
        
        # Verify audit logging was called for stage completion
        audit_logger = mock_context_with_audit.audit_logger
        audit_logger.log_stage_completed.assert_called()
        
        # Check the stage completion audit log details
        call_args = audit_logger.log_stage_completed.call_args
        assert call_args[0][0] == "vectorstore"
        assert "engine" in call_args[1]["details"]
        assert "total_chunks" in call_args[1]["details"]
        assert "successful_uploads" in call_args[1]["details"]
        
        # Verify individual chunk and collection logging
        audit_logger.log_record_processed.assert_called()
        
        # Verify tracking repository was called
        tracking_repo = mock_context_with_audit.tracking_repository
        tracking_repo.update_pipeline_run_counts.assert_called()
    
    @pytest.mark.asyncio
    async def test_vectorstore_stage_logs_connection_failure(self, vectorstore_stage, mock_context_with_audit, sample_embedded_chunks):
        """Test that vector store connection failures are properly logged."""
        from pulsepipe.pipelines.vectorstore import VectorStoreConnectionError
        
        # Mock the vector store to raise connection error
        mock_vectorstore_class = MagicMock(side_effect=VectorStoreConnectionError("Connection failed", "localhost", 8080))
        
        vectorstore_stage.vectorstore_registry = {
            "weaviate": mock_vectorstore_class
        }
        
        # Set up the stage configuration
        vectorstore_stage.get_stage_config = MagicMock(return_value={
            "engine": "weaviate",
            "host": "http://localhost:8080"
        })
        
        # Verify that the stage raises the expected exception
        with pytest.raises(Exception):
            await vectorstore_stage.execute(mock_context_with_audit, sample_embedded_chunks)
        
        # Verify failure audit logging was called
        audit_logger = mock_context_with_audit.audit_logger
        audit_logger.log_stage_failed.assert_called()
        
        # Check the failure audit log details
        call_args = audit_logger.log_stage_failed.call_args
        assert call_args[0][0] == "vectorstore"
        assert "engine" in call_args[1]["details"]
        assert "error_type" in call_args[1]["details"]
    
    @pytest.mark.asyncio
    async def test_vectorstore_stage_logs_upload_failure(self, vectorstore_stage, mock_context_with_audit, sample_embedded_chunks):
        """Test that vector store upload failures are properly logged."""
        # Mock the vector store to fail on upsert
        mock_vectorstore = MagicMock()
        mock_vectorstore.__class__.__name__ = "MockVectorStore"
        mock_vectorstore.upsert = MagicMock(side_effect=Exception("Upload failed"))
        
        vectorstore_stage.vectorstore_registry = {
            "weaviate": MagicMock(return_value=mock_vectorstore)
        }
        
        # Set up the stage configuration
        vectorstore_stage.get_stage_config = MagicMock(return_value={
            "engine": "weaviate",
            "host": "http://localhost:8080",
            "namespace_prefix": "test_pulse"
        })
        
        # Execute the stage
        result = await vectorstore_stage.execute(mock_context_with_audit, sample_embedded_chunks)
        
        # Verify failure audit logging was called for collection uploads
        audit_logger = mock_context_with_audit.audit_logger
        audit_logger.log_record_failed.assert_called()
        
        # Verify tracking repository was called with correct failure counts
        tracking_repo = mock_context_with_audit.tracking_repository
        tracking_repo.update_pipeline_run_counts.assert_called()
        
        # Get the call args to check failure counts
        call_args = tracking_repo.update_pipeline_run_counts.call_args
        assert call_args[1]["failed"] > 0


if __name__ == "__main__":
    pytest.main([__file__])