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

# tests/test_quality_integration.py

"""
Unit tests for the quality assessment integration components.

Tests the QualityAssessmentService and QualityAssessmentPipeline classes
that integrate the quality scoring engine with the persistence layer.
"""

import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List
from unittest.mock import Mock, MagicMock

from pulsepipe.pipelines.quality.integration import (
    QualityAssessmentService,
    QualityAssessmentPipeline
)
from pulsepipe.pipelines.quality.scoring_engine import QualityScore
from pulsepipe.persistence import QualityMetric


@pytest.fixture
def mock_repository():
    """Create a mock tracking repository."""
    repository = Mock()
    repository.record_quality_metric.return_value = 1
    repository.get_quality_summary.return_value = {
        'total_records': 100,
        'avg_completeness_score': 0.85,
        'avg_consistency_score': 0.90,
        'avg_validity_score': 0.88,
        'avg_accuracy_score': 0.92,
        'avg_overall_score': 0.89,
        'min_overall_score': 0.60,
        'max_overall_score': 0.98
    }
    return repository


@pytest.fixture
def sample_patient_data():
    """Sample patient data for testing."""
    return {
        "id": "patient_001",
        "name": "John Doe",
        "birth_date": "1985-03-15",
        "age": 39,
        "gender": "male",
        "email": "john.doe@example.com",
        "phone": "555-123-4567"
    }


@pytest.fixture
def incomplete_patient_data():
    """Incomplete patient data for testing."""
    return {
        "id": "patient_002",
        "name": "",
        "birth_date": None,
        "email": "invalid-email"
    }


class TestQualityAssessmentService:
    """Test QualityAssessmentService class."""
    
    def test_init_service(self, mock_repository):
        """Test initializing the quality assessment service."""
        service = QualityAssessmentService(mock_repository)
        
        assert service.repository == mock_repository
        assert service.auto_persist is True
        assert service.sampling_rate == 1.0
        assert service.batch_size == 100
        assert len(service.pending_scores) == 0
    
    def test_init_service_with_config(self, mock_repository):
        """Test initializing with custom configuration."""
        config = {
            'auto_persist': False,
            'sampling_rate': 0.5,
            'batch_size': 50,
            'weights': {
                'completeness': 0.4,
                'consistency': 0.3,
                'validity': 0.1,
                'accuracy': 0.1,
                'outlier_detection': 0.05,
                'data_usage': 0.05
            }
        }
        
        service = QualityAssessmentService(mock_repository, config)
        
        assert service.auto_persist is False
        assert service.sampling_rate == 0.5
        assert service.batch_size == 50
        assert service.scorer.weights['completeness'] == 0.4
    
    def test_assess_record_with_persist(self, mock_repository, sample_patient_data):
        """Test assessing a single record with persistence."""
        service = QualityAssessmentService(mock_repository)
        
        quality_score = service.assess_record(
            pipeline_run_id="test_pipeline_123",
            data=sample_patient_data,
            record_type="Patient",
            record_id="patient_001",
            persist=True
        )
        
        assert quality_score.record_id == "patient_001"
        assert quality_score.record_type == "Patient"
        assert quality_score.overall_score > 0
        
        # Should have called repository to persist
        mock_repository.record_quality_metric.assert_called_once()
        
        # Check the persisted metric
        call_args = mock_repository.record_quality_metric.call_args[0]
        metric = call_args[0]
        assert isinstance(metric, QualityMetric)
        assert metric.pipeline_run_id == "test_pipeline_123"
        assert metric.record_id == "patient_001"
        assert metric.record_type == "Patient"
    
    def test_assess_record_without_persist(self, mock_repository, sample_patient_data):
        """Test assessing a record without persistence."""
        service = QualityAssessmentService(mock_repository)
        
        quality_score = service.assess_record(
            pipeline_run_id="test_pipeline_123",
            data=sample_patient_data,
            record_type="Patient",
            persist=False
        )
        
        assert quality_score.record_id == sample_patient_data["id"]
        assert len(service.pending_scores) == 1
        
        # Should not have called repository
        mock_repository.record_quality_metric.assert_not_called()
    
    def test_assess_record_with_sampling(self, mock_repository, sample_patient_data):
        """Test record assessment with sampling."""
        config = {'sampling_rate': 0.5}  # 50% sampling
        service = QualityAssessmentService(mock_repository, config)
        
        # Assess multiple records to test sampling
        scores = []
        for i in range(10):
            data = sample_patient_data.copy()
            data["id"] = f"patient_{i:03d}"
            
            score = service.assess_record(
                pipeline_run_id="test_pipeline_123",
                data=data,
                record_type="Patient"
            )
            scores.append(score)
        
        # Should have a mix of real scores and placeholder scores
        real_scores = [s for s in scores if not s.metadata.get('placeholder', False)]
        placeholder_scores = [s for s in scores if s.metadata.get('placeholder', False)]
        
        # Due to randomness, we can't guarantee exact counts, but there should be some of each
        assert len(real_scores) + len(placeholder_scores) == 10
        assert len(placeholder_scores) > 0  # Should have some placeholders
    
    def test_assess_batch(self, mock_repository):
        """Test assessing a batch of records."""
        service = QualityAssessmentService(mock_repository)
        
        records = [
            {"id": "p1", "name": "John", "age": 30},
            {"id": "p2", "name": "Jane", "age": 25},
            {"id": "p3", "name": "Bob", "age": 35}
        ]
        
        scores = service.assess_batch(
            pipeline_run_id="test_pipeline_123",
            records=records,
            record_type="Patient",
            persist=True
        )
        
        assert len(scores) == 3
        assert all(isinstance(score, QualityScore) for score in scores)
        assert scores[0].record_id == "p1"
        assert scores[1].record_id == "p2"
        assert scores[2].record_id == "p3"
        
        # Should have persisted all scores
        assert mock_repository.record_quality_metric.call_count == 3
    
    def test_assess_batch_with_sampling(self, mock_repository):
        """Test batch assessment with sampling."""
        config = {'sampling_rate': 0.5}
        service = QualityAssessmentService(mock_repository, config)
        
        records = [{"id": f"p{i}", "name": f"Patient{i}"} for i in range(20)]
        
        scores = service.assess_batch(
            pipeline_run_id="test_pipeline_123",
            records=records,
            record_type="Patient"
        )
        
        assert len(scores) == 20
        
        # Check sampling metadata
        sampled_scores = [s for s in scores if s.metadata.get('sampled', False)]
        non_sampled_scores = [s for s in scores if not s.metadata.get('sampled', True)]
        
        # Should have both sampled and non-sampled
        assert len(sampled_scores) > 0
        assert len(non_sampled_scores) > 0
    
    def test_get_quality_summary(self, mock_repository):
        """Test getting quality summary."""
        service = QualityAssessmentService(mock_repository)
        
        summary = service.get_quality_summary("test_pipeline_123")
        
        assert summary['total_records'] == 100
        assert summary['avg_overall_score'] == 0.89
        
        mock_repository.get_quality_summary.assert_called_once_with("test_pipeline_123")
    
    def test_get_quality_trends(self, mock_repository):
        """Test getting quality trends."""
        service = QualityAssessmentService(mock_repository)
        
        trends = service.get_quality_trends("Patient", 30)
        
        assert trends['period_days'] == 30
        assert 'start_date' in trends
        assert 'end_date' in trends
        assert 'summary' in trends
        assert trends['trend_analysis'] == "Not yet implemented"
    
    def test_flush_pending(self, mock_repository, sample_patient_data):
        """Test flushing pending scores."""
        service = QualityAssessmentService(mock_repository)
        
        # Add some pending scores
        service.assess_record(
            pipeline_run_id="test_pipeline_123",
            data=sample_patient_data,
            record_type="Patient",
            persist=False
        )
        
        assert len(service.pending_scores) == 1
        
        # Flush pending
        service.flush_pending("test_pipeline_123")
        
        assert len(service.pending_scores) == 0
        mock_repository.record_quality_metric.assert_called_once()
    
    def test_batch_auto_flush(self, mock_repository):
        """Test automatic batch flushing when batch size is reached."""
        config = {'batch_size': 3, 'auto_persist': False}
        service = QualityAssessmentService(mock_repository, config)
        
        # Add records up to batch size
        for i in range(3):
            service.assess_record(
                pipeline_run_id="test_pipeline_123",
                data={"id": f"patient_{i}", "name": f"Patient {i}"},
                record_type="Patient",
                persist=False
            )
        
        # Should have auto-flushed
        assert len(service.pending_scores) == 0
        assert mock_repository.record_quality_metric.call_count == 3
    
    def test_create_placeholder_score(self, mock_repository):
        """Test creating placeholder scores for non-sampled records."""
        service = QualityAssessmentService(mock_repository)
        
        placeholder = service._create_placeholder_score("test_id", "Patient")
        
        assert placeholder.record_id == "test_id"
        assert placeholder.record_type == "Patient"
        assert placeholder.overall_score == 0.0
        assert placeholder.metadata['placeholder'] is True
        assert placeholder.metadata['sampled'] is False
        assert len(placeholder.issues) == 0
    
    def test_persist_quality_score_error_handling(self, mock_repository, sample_patient_data):
        """Test error handling during persistence."""
        # Make repository raise an exception
        mock_repository.record_quality_metric.side_effect = Exception("Database error")
        
        service = QualityAssessmentService(mock_repository)
        
        # Should not raise exception, just log error
        quality_score = service.assess_record(
            pipeline_run_id="test_pipeline_123",
            data=sample_patient_data,
            record_type="Patient",
            persist=True
        )
        
        assert quality_score.record_id == sample_patient_data["id"]
        # Exception should be caught and logged, not raised


class TestQualityAssessmentPipeline:
    """Test QualityAssessmentPipeline class."""
    
    def test_init_pipeline(self, mock_repository):
        """Test initializing the quality assessment pipeline."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        assert pipeline.service == service
        assert pipeline.processed_records == 0
        assert pipeline.quality_stats['total_assessed'] == 0
    
    @pytest.mark.asyncio
    async def test_process_record(self, mock_repository, sample_patient_data):
        """Test processing a single record through the pipeline."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        enhanced_data = await pipeline.process_record(
            pipeline_run_id="test_pipeline_123",
            data=sample_patient_data,
            record_type="Patient",
            record_id="patient_001"
        )
        
        # Should return enhanced data with quality metadata
        assert enhanced_data["id"] == "patient_001"
        assert enhanced_data["name"] == "John Doe"
        assert "_quality" in enhanced_data
        
        quality_meta = enhanced_data["_quality"]
        assert "quality_score" in quality_meta
        assert "quality_grade" in quality_meta
        assert "has_issues" in quality_meta
        assert "issue_count" in quality_meta
        assert "high_severity_issues" in quality_meta
        
        # Should update pipeline stats
        assert pipeline.processed_records == 1
        assert pipeline.quality_stats['total_assessed'] == 1
    
    @pytest.mark.asyncio
    async def test_process_batch(self, mock_repository):
        """Test processing a batch of records through the pipeline."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        records = [
            {"id": "p1", "name": "John", "age": 30},
            {"id": "p2", "name": "Jane", "age": 25},
            {"id": "p3", "name": "Bob", "age": 35}
        ]
        
        enhanced_records = await pipeline.process_batch(
            pipeline_run_id="test_pipeline_123",
            records=records,
            record_type="Patient"
        )
        
        assert len(enhanced_records) == 3
        
        # All records should have quality metadata
        for record in enhanced_records:
            assert "_quality" in record
            quality_meta = record["_quality"]
            assert "quality_score" in quality_meta
            assert "quality_grade" in quality_meta
            assert "critical_issues" in quality_meta
        
        # Should update pipeline stats
        assert pipeline.processed_records == 3
        assert pipeline.quality_stats['total_assessed'] == 3
    
    def test_get_pipeline_stats(self, mock_repository, sample_patient_data):
        """Test getting pipeline statistics."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        # Process some records to generate stats
        for i in range(5):
            data = sample_patient_data.copy()
            data["id"] = f"patient_{i}"
            quality_score = service.assess_record(
                pipeline_run_id="test_pipeline_123",
                data=data,
                record_type="Patient"
            )
            pipeline._update_stats(quality_score)
        
        stats = pipeline.get_pipeline_stats()
        
        assert stats['processed_records'] == 5
        assert stats['quality_stats']['total_assessed'] == 5
        assert 'quality_rate' in stats
        assert stats['quality_rate'] >= 0
    
    def test_update_stats(self, mock_repository):
        """Test updating internal statistics."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        # Create mock quality scores
        high_quality_score = QualityScore(
            record_id="high_quality",
            record_type="Patient",
            completeness_score=0.9,
            consistency_score=0.9,
            validity_score=0.9,
            accuracy_score=0.9,
            outlier_score=0.9,
            data_usage_score=0.9,
            overall_score=0.9,
            issues=[],
            missing_fields=[],
            invalid_fields=[],
            outlier_fields=[],
            unused_fields=[]
        )
        
        low_quality_score = QualityScore(
            record_id="low_quality",
            record_type="Patient",
            completeness_score=0.3,
            consistency_score=0.3,
            validity_score=0.3,
            accuracy_score=0.3,
            outlier_score=0.3,
            data_usage_score=0.3,
            overall_score=0.3,
            issues=[],
            missing_fields=[],
            invalid_fields=[],
            outlier_fields=[],
            unused_fields=[]
        )
        
        # Update stats
        pipeline._update_stats(high_quality_score)
        pipeline._update_stats(low_quality_score)
        
        assert pipeline.processed_records == 2
        assert pipeline.quality_stats['total_assessed'] == 2
        assert pipeline.quality_stats['high_quality_records'] == 1
        assert pipeline.quality_stats['low_quality_records'] == 1
        assert pipeline.quality_stats['avg_quality_score'] == 0.6  # (0.9 + 0.3) / 2
    
    def test_update_stats_with_placeholder(self, mock_repository):
        """Test that placeholder scores are skipped in statistics."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        placeholder_score = service._create_placeholder_score("test", "Patient")
        
        # Should not update stats for placeholder
        initial_processed = pipeline.processed_records
        pipeline._update_stats(placeholder_score)
        
        assert pipeline.processed_records == initial_processed
        assert pipeline.quality_stats['total_assessed'] == 0
    
    def test_get_quality_grade(self, mock_repository):
        """Test quality grade assignment."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        assert pipeline._get_quality_grade(0.95) == 'A'
        assert pipeline._get_quality_grade(0.85) == 'B'
        assert pipeline._get_quality_grade(0.75) == 'C'
        assert pipeline._get_quality_grade(0.65) == 'D'
        assert pipeline._get_quality_grade(0.45) == 'F'
    
    @pytest.mark.asyncio
    async def test_process_record_with_usage_context(self, mock_repository, sample_patient_data):
        """Test processing record with usage context."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        usage_context = {
            "validation": ["id", "name"],
            "processing": ["id", "age"]
        }
        
        enhanced_data = await pipeline.process_record(
            pipeline_run_id="test_pipeline_123",
            data=sample_patient_data,
            record_type="Patient",
            usage_context=usage_context
        )
        
        assert "_quality" in enhanced_data
        # Should have processed successfully with usage context
        assert enhanced_data["_quality"]["quality_score"] > 0
    
    def test_quality_rate_calculation(self, mock_repository):
        """Test quality rate calculation in pipeline stats."""
        service = QualityAssessmentService(mock_repository)
        pipeline = QualityAssessmentPipeline(service)
        
        # Add mix of high and low quality scores
        high_scores = [
            QualityScore("h1", "Patient", 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, [], [], [], [], []),
            QualityScore("h2", "Patient", 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, [], [], [], [], [])
        ]
        
        low_scores = [
            QualityScore("l1", "Patient", 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, [], [], [], [], []),
            QualityScore("l2", "Patient", 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, [], [], [], [], [])
        ]
        
        for score in high_scores + low_scores:
            pipeline._update_stats(score)
        
        stats = pipeline.get_pipeline_stats()
        
        # 2 high quality out of 4 total = 50%
        assert stats['quality_rate'] == 50.0


if __name__ == "__main__":
    pytest.main([__file__])