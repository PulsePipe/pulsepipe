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

# tests/test_audit_reporter.py

"""
Unit tests for audit reporting system.

Tests report generation, data aggregation,
export functionality, and recommendation generation.
"""

import pytest
import json
import csv
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
from pathlib import Path

from pulsepipe.audit.audit_reporter import (
    AuditReporter,
    AuditReport,
    ProcessingSummary,
    QualitySummary
)
from pulsepipe.persistence import PipelineRunSummary


class TestProcessingSummary:
    """Test ProcessingSummary dataclass."""
    
    def test_basic_creation(self):
        """Test basic ProcessingSummary creation."""
        summary = ProcessingSummary(
            total_records=1000,
            successful_records=950,
            failed_records=30,
            skipped_records=20,
            success_rate=95.0,
            failure_rate=3.0,
            skip_rate=2.0,
            avg_processing_time_ms=150.5,
            total_bytes_processed=1048576
        )
        
        assert summary.total_records == 1000
        assert summary.successful_records == 950
        assert summary.failed_records == 30
        assert summary.skipped_records == 20
        assert summary.success_rate == 95.0
        assert summary.failure_rate == 3.0
        assert summary.skip_rate == 2.0
        assert summary.avg_processing_time_ms == 150.5
        assert summary.total_bytes_processed == 1048576
    
    def test_from_ingestion_summary(self):
        """Test creation from ingestion summary data."""
        ingestion_data = {
            'total_records': 500,
            'successful_records': 450,
            'failed_records': 40,
            'skipped_records': 10,
            'avg_processing_time_ms': 120.0,
            'total_bytes_processed': 524288
        }
        
        summary = ProcessingSummary.from_ingestion_summary(ingestion_data)
        
        assert summary.total_records == 500
        assert summary.successful_records == 450
        assert summary.failed_records == 40
        assert summary.skipped_records == 10
        assert summary.success_rate == 90.0  # 450/500 * 100
        assert summary.failure_rate == 8.0   # 40/500 * 100
        assert summary.skip_rate == 2.0      # 10/500 * 100
        assert summary.avg_processing_time_ms == 120.0
        assert summary.total_bytes_processed == 524288
    
    def test_from_ingestion_summary_zero_records(self):
        """Test creation with zero total records."""
        ingestion_data = {
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'skipped_records': 0,
            'avg_processing_time_ms': 0,
            'total_bytes_processed': 0
        }
        
        summary = ProcessingSummary.from_ingestion_summary(ingestion_data)
        
        assert summary.success_rate == 0
        assert summary.failure_rate == 0
        assert summary.skip_rate == 0
    
    def test_from_ingestion_summary_missing_fields(self):
        """Test creation with missing fields in data."""
        ingestion_data = {
            'total_records': 100,
            'successful_records': 90
            # Missing other fields
        }
        
        summary = ProcessingSummary.from_ingestion_summary(ingestion_data)
        
        assert summary.total_records == 100
        assert summary.successful_records == 90
        assert summary.failed_records == 0  # Default
        assert summary.skipped_records == 0  # Default
        assert summary.success_rate == 90.0
        assert summary.failure_rate == 0
        assert summary.skip_rate == 0


class TestQualitySummary:
    """Test QualitySummary dataclass."""
    
    def test_basic_creation(self):
        """Test basic QualitySummary creation."""
        summary = QualitySummary(
            total_records=1000,
            avg_completeness_score=0.95,
            avg_consistency_score=0.88,
            avg_validity_score=0.92,
            avg_accuracy_score=0.90,
            avg_overall_score=0.91,
            min_overall_score=0.65,
            max_overall_score=0.98
        )
        
        assert summary.total_records == 1000
        assert summary.avg_completeness_score == 0.95
        assert summary.avg_consistency_score == 0.88
        assert summary.avg_validity_score == 0.92
        assert summary.avg_accuracy_score == 0.90
        assert summary.avg_overall_score == 0.91
        assert summary.min_overall_score == 0.65
        assert summary.max_overall_score == 0.98
    
    def test_from_quality_summary(self):
        """Test creation from quality summary data."""
        quality_data = {
            'total_records': 500,
            'avg_completeness_score': 0.9234567,
            'avg_consistency_score': None,  # Should handle None
            'avg_validity_score': 0.8876543,
            'avg_accuracy_score': 0.9123456,
            'avg_overall_score': 0.9055555,
            'min_overall_score': 0.65,
            'max_overall_score': 0.98
        }
        
        summary = QualitySummary.from_quality_summary(quality_data)
        
        assert summary.total_records == 500
        assert summary.avg_completeness_score == 0.923  # Rounded to 3 places
        assert summary.avg_consistency_score == 0.0     # None converted to 0
        assert summary.avg_validity_score == 0.888      # Rounded
        assert summary.avg_accuracy_score == 0.912      # Rounded
        assert summary.avg_overall_score == 0.906       # Rounded
        assert summary.min_overall_score == 0.65
        assert summary.max_overall_score == 0.98
    
    def test_from_quality_summary_empty(self):
        """Test creation from empty quality data."""
        quality_data = {}
        
        summary = QualitySummary.from_quality_summary(quality_data)
        
        assert summary.total_records == 0
        assert summary.avg_completeness_score == 0.0
        assert summary.avg_consistency_score == 0.0
        assert summary.avg_overall_score == 0.0


class TestAuditReport:
    """Test AuditReport dataclass."""
    
    def test_basic_creation(self):
        """Test basic AuditReport creation."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        report = AuditReport(
            report_id="test_report_123",
            generated_at=datetime.now(),
            report_type="pipeline_run",
            pipeline_run_id="run_123",
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={"validation_error": 3, "parse_error": 2},
            pipeline_runs=[],
            recommendations=["Fix validation rules"],
            metadata={"test": "data"}
        )
        
        assert report.report_id == "test_report_123"
        assert report.report_type == "pipeline_run"
        assert report.pipeline_run_id == "run_123"
        assert report.processing_summary == processing_summary
        assert report.error_breakdown == {"validation_error": 3, "parse_error": 2}
        assert report.recommendations == ["Fix validation rules"]
        assert report.metadata == {"test": "data"}
    
    def test_to_dict(self):
        """Test to_dict conversion."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        report = AuditReport(
            report_id="test_report_123",
            generated_at=datetime.now(),
            report_type="test",
            pipeline_run_id=None,
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={},
            pipeline_runs=[],
            recommendations=[],
            metadata={}
        )
        
        report_dict = report.to_dict()
        
        assert isinstance(report_dict, dict)
        assert report_dict['report_id'] == "test_report_123"
        assert report_dict['report_type'] == "test"
        assert 'processing_summary' in report_dict
        assert isinstance(report_dict['processing_summary'], dict)
    
    def test_to_json(self):
        """Test to_json conversion."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        report = AuditReport(
            report_id="test_report_123",
            generated_at=datetime.now(),
            report_type="test",
            pipeline_run_id=None,
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={},
            pipeline_runs=[],
            recommendations=[],
            metadata={}
        )
        
        json_str = report.to_json()
        
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed['report_id'] == "test_report_123"
        assert parsed['report_type'] == "test"


class TestAuditReporter:
    """Test AuditReporter class."""
    
    @pytest.fixture
    def mock_repository(self):
        """Create mock tracking repository."""
        repository = Mock()
        repository.get_pipeline_run.return_value = None
        repository.get_ingestion_summary.return_value = {
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'skipped_records': 0,
            'error_breakdown': {},
            'avg_processing_time_ms': 0,
            'total_bytes_processed': 0
        }
        repository.get_quality_summary.return_value = {
            'total_records': 0,
            'avg_completeness_score': None,
            'avg_consistency_score': None,
            'avg_validity_score': None,
            'avg_accuracy_score': None,
            'avg_overall_score': None,
            'min_overall_score': None,
            'max_overall_score': None
        }
        repository.get_recent_pipeline_runs.return_value = []
        return repository
    
    @pytest.fixture
    def reporter(self, mock_repository):
        """Create AuditReporter instance."""
        return AuditReporter(mock_repository)
    
    def test_initialization(self, mock_repository):
        """Test AuditReporter initialization."""
        reporter = AuditReporter(mock_repository)
        assert reporter.repository == mock_repository
    
    def test_generate_pipeline_report_not_found(self, reporter, mock_repository):
        """Test generate_pipeline_report with non-existent run."""
        mock_repository.get_pipeline_run.return_value = None
        
        with pytest.raises(ValueError, match="Pipeline run not found"):
            reporter.generate_pipeline_report("nonexistent_run")
    
    def test_generate_pipeline_report_success(self, reporter, mock_repository):
        """Test successful pipeline report generation."""
        # Setup mock data
        pipeline_run = PipelineRunSummary(
            id="test_run_123",
            name="test_pipeline",
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="completed",
            total_records=1000,
            successful_records=950,
            failed_records=50,
            skipped_records=0
        )
        
        ingestion_summary = {
            'total_records': 1000,
            'successful_records': 950,
            'failed_records': 50,
            'skipped_records': 0,
            'error_breakdown': {"validation_error": 30, "parse_error": 20},
            'avg_processing_time_ms': 150.0,
            'total_bytes_processed': 1048576
        }
        
        quality_summary = {
            'total_records': 1000,
            'avg_completeness_score': 0.95,
            'avg_consistency_score': 0.88,
            'avg_validity_score': 0.92,
            'avg_accuracy_score': 0.90,
            'avg_overall_score': 0.91,
            'min_overall_score': 0.65,
            'max_overall_score': 0.98
        }
        
        mock_repository.get_pipeline_run.return_value = pipeline_run
        mock_repository.get_ingestion_summary.return_value = ingestion_summary
        mock_repository.get_quality_summary.return_value = quality_summary
        
        # Generate report
        report = reporter.generate_pipeline_report("test_run_123")
        
        # Verify report
        assert isinstance(report, AuditReport)
        assert report.report_type == "pipeline_run"
        assert report.pipeline_run_id == "test_run_123"
        assert report.processing_summary.total_records == 1000
        assert report.processing_summary.success_rate == 95.0
        assert report.quality_summary.avg_overall_score == 0.91
        assert report.error_breakdown == {"validation_error": 30, "parse_error": 20}
        assert len(report.pipeline_runs) == 1
        assert report.pipeline_runs[0] == pipeline_run
        assert len(report.recommendations) > 0
        assert report.metadata["pipeline_name"] == "test_pipeline"
        assert report.metadata["pipeline_status"] == "completed"
    
    def test_generate_summary_report(self, reporter, mock_repository):
        """Test summary report generation."""
        # Setup mock data
        pipeline_runs = [
            PipelineRunSummary(
                id="run_1", name="pipeline_1", started_at=datetime.now() - timedelta(days=1),
                completed_at=datetime.now() - timedelta(days=1), status="completed",
                total_records=500, successful_records=480, failed_records=20, skipped_records=0
            ),
            PipelineRunSummary(
                id="run_2", name="pipeline_2", started_at=datetime.now() - timedelta(days=2),
                completed_at=datetime.now() - timedelta(days=2), status="failed",
                total_records=300, successful_records=0, failed_records=300, skipped_records=0
            )
        ]
        
        ingestion_summary = {
            'total_records': 800,
            'successful_records': 480,
            'failed_records': 320,
            'skipped_records': 0,
            'error_breakdown': {"network_error": 200, "validation_error": 120},
            'avg_processing_time_ms': 200.0,
            'total_bytes_processed': 2097152
        }
        
        mock_repository.get_recent_pipeline_runs.return_value = pipeline_runs
        mock_repository.get_ingestion_summary.return_value = ingestion_summary
        
        # Generate report
        report = reporter.generate_summary_report()
        
        # Verify report
        assert report.report_type == "summary"
        assert report.pipeline_run_id is None
        assert report.time_range is not None
        assert report.processing_summary.total_records == 800
        assert report.processing_summary.failed_records == 320
        assert len(report.pipeline_runs) == 2
        assert len(report.recommendations) > 0
        assert report.metadata["total_pipeline_runs"] == 2
        assert report.metadata["successful_runs"] == 1
        assert report.metadata["failed_runs"] == 1
    
    def test_generate_summary_report_with_date_range(self, reporter, mock_repository):
        """Test summary report with specific date range."""
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        # Generate report with date range
        report = reporter.generate_summary_report(start_date, end_date, limit=5)
        
        # Verify repository was called with correct parameters
        mock_repository.get_ingestion_summary.assert_called_with(
            start_date=start_date, end_date=end_date
        )
        
        assert report.time_range["start_date"] == start_date
        assert report.time_range["end_date"] == end_date
        assert report.metadata["date_range_days"] == 30
    
    def test_generate_failure_report(self, reporter, mock_repository):
        """Test failure report generation."""
        # Setup mock data with failures
        failed_runs = [
            PipelineRunSummary(
                id="failed_run_1", name="failed_pipeline", started_at=datetime.now(),
                completed_at=datetime.now(), status="failed",
                total_records=100, successful_records=0, failed_records=100, skipped_records=0,
                error_message="Network connection failed"
            )
        ]
        
        ingestion_summary = {
            'total_records': 200,
            'successful_records': 0,
            'failed_records': 200,
            'skipped_records': 0,
            'error_breakdown': {"network_error": 150, "system_error": 50},
            'avg_processing_time_ms': 50.0,
            'total_bytes_processed': 0
        }
        
        mock_repository.get_recent_pipeline_runs.return_value = failed_runs
        mock_repository.get_ingestion_summary.return_value = ingestion_summary
        
        # Generate report
        report = reporter.generate_failure_report()
        
        # Verify report
        assert report.report_type == "failures"
        assert report.processing_summary.failed_records == 200
        assert report.processing_summary.failure_rate == 100.0
        assert report.quality_summary is None  # Not relevant for failure report
        assert report.error_breakdown == {"network_error": 150, "system_error": 50}
        assert len(report.pipeline_runs) == 1
        assert report.pipeline_runs[0].status == "failed"
        assert len(report.recommendations) > 0
        assert report.metadata["total_failed_records"] == 200
        assert report.metadata["total_failed_runs"] == 1
        assert report.metadata["most_common_error"] == "network_error"
    
    def test_export_report_json(self, reporter, tmp_path):
        """Test exporting report to JSON."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        report = AuditReport(
            report_id="test_export",
            generated_at=datetime.now(),
            report_type="test",
            pipeline_run_id=None,
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={"test_error": 5},
            pipeline_runs=[],
            recommendations=["Test recommendation"],
            metadata={"test": "metadata"}
        )
        
        export_path = tmp_path / "test_export_report_json.json"
        reporter.export_report(report, str(export_path), "json")
        
        assert export_path.exists()
        
        # Verify content
        with open(export_path) as f:
            data = json.load(f)
        
        assert data["report_id"] == "test_export"
        assert data["report_type"] == "test"
        assert data["processing_summary"]["total_records"] == 100
        assert data["error_breakdown"]["test_error"] == 5
    
    def test_export_report_csv(self, reporter, tmp_path):
        """Test exporting report to CSV."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        pipeline_runs = [
            PipelineRunSummary(
                id="run_1", name="test_pipeline", started_at=datetime.now(),
                completed_at=datetime.now(), status="completed",
                total_records=100, successful_records=95, failed_records=5, skipped_records=0
            )
        ]
        
        report = AuditReport(
            report_id="test_csv_export",
            generated_at=datetime.now(),
            report_type="test",
            pipeline_run_id=None,
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={"validation_error": 3, "parse_error": 2},
            pipeline_runs=pipeline_runs,
            recommendations=["Fix validation"],
            metadata={}
        )
        
        export_path = tmp_path / "test_export_report_csv.csv"
        reporter.export_report(report, str(export_path), "csv")
        
        assert export_path.exists()
        
        # Verify CSV content
        with open(export_path) as f:
            content = f.read()
        
        assert "test_csv_export" in content
        assert "Processing Summary" in content
        assert "Total Records" in content
        assert "Error Breakdown" in content
        assert "Pipeline Runs" in content
    
    def test_export_report_html(self, reporter, tmp_path):
        """Test exporting report to HTML."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        report = AuditReport(
            report_id="test_html_export",
            generated_at=datetime.now(),
            report_type="test",
            pipeline_run_id=None,
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={"validation_error": 3, "parse_error": 2},
            pipeline_runs=[],
            recommendations=["Fix validation", "Improve parsing"],
            metadata={}
        )
        
        export_path = tmp_path / "test_export_report_html.html"
        reporter.export_report(report, str(export_path), "html")
        
        assert export_path.exists()
        
        # Verify HTML content
        with open(export_path) as f:
            content = f.read()
        
        assert "<!DOCTYPE html>" in content
        assert "PulsePipe Audit Report" in content
        assert "test_html_export" in content
        assert "Processing Summary" in content
        assert "95.0%" in content  # Success rate (with decimal)
        assert "Error Breakdown" in content
        assert "validation_error" in content
        assert "Recommendations" in content
        assert "Fix validation" in content
    
    def test_export_report_invalid_format(self, reporter, tmp_path):
        """Test export with invalid format."""
        processing_summary = ProcessingSummary(
            total_records=100, successful_records=95, failed_records=5,
            skipped_records=0, success_rate=95.0, failure_rate=5.0,
            skip_rate=0.0, avg_processing_time_ms=100.0, total_bytes_processed=1024
        )
        
        report = AuditReport(
            report_id="test",
            generated_at=datetime.now(),
            report_type="test",
            pipeline_run_id=None,
            time_range=None,
            processing_summary=processing_summary,
            quality_summary=None,
            error_breakdown={},
            pipeline_runs=[],
            recommendations=[],
            metadata={}
        )
        
        export_path = tmp_path / "test_export_report_invalid_format.xml"
        
        with pytest.raises(ValueError, match="Unsupported export format"):
            reporter.export_report(report, str(export_path), "xml")
    
    def test_recommendation_generation_high_failure_rate(self, reporter, mock_repository):
        """Test recommendation generation for high failure rate."""
        pipeline_run = PipelineRunSummary(
            id="test_run", name="test_pipeline", started_at=datetime.now(),
            completed_at=datetime.now(), status="completed",
            total_records=1000, successful_records=800, failed_records=200, skipped_records=0
        )
        
        ingestion_summary = {
            'total_records': 1000,
            'successful_records': 800,
            'failed_records': 200,
            'skipped_records': 0,
            'error_breakdown': {"validation_error": 200},
            'avg_processing_time_ms': 150.0,
            'total_bytes_processed': 1048576
        }
        
        mock_repository.get_pipeline_run.return_value = pipeline_run
        mock_repository.get_ingestion_summary.return_value = ingestion_summary
        
        report = reporter.generate_pipeline_report("test_run")
        
        # Should generate recommendation for high failure rate (20%)
        failure_recommendations = [r for r in report.recommendations if "failure rate" in r.lower()]
        assert len(failure_recommendations) > 0
    
    def test_recommendation_generation_slow_processing(self, reporter, mock_repository):
        """Test recommendation generation for slow processing."""
        pipeline_run = PipelineRunSummary(
            id="test_run", name="test_pipeline", started_at=datetime.now(),
            completed_at=datetime.now(), status="completed",
            total_records=1000, successful_records=1000, failed_records=0, skipped_records=0
        )
        
        ingestion_summary = {
            'total_records': 1000,
            'successful_records': 1000,
            'failed_records': 0,
            'skipped_records': 0,
            'error_breakdown': {},
            'avg_processing_time_ms': 2000.0,  # High processing time
            'total_bytes_processed': 1048576
        }
        
        mock_repository.get_pipeline_run.return_value = pipeline_run
        mock_repository.get_ingestion_summary.return_value = ingestion_summary
        
        report = reporter.generate_pipeline_report("test_run")
        
        # Should generate recommendation for slow processing
        perf_recommendations = [r for r in report.recommendations if "processing time" in r.lower()]
        assert len(perf_recommendations) > 0
    
    def test_recommendation_generation_poor_quality(self, reporter, mock_repository):
        """Test recommendation generation for poor data quality."""
        pipeline_run = PipelineRunSummary(
            id="test_run", name="test_pipeline", started_at=datetime.now(),
            completed_at=datetime.now(), status="completed",
            total_records=1000, successful_records=1000, failed_records=0, skipped_records=0
        )
        
        ingestion_summary = {
            'total_records': 1000,
            'successful_records': 1000,
            'failed_records': 0,
            'skipped_records': 0,
            'error_breakdown': {},
            'avg_processing_time_ms': 150.0,
            'total_bytes_processed': 1048576
        }
        
        quality_summary = {
            'total_records': 1000,
            'avg_completeness_score': 0.5,
            'avg_consistency_score': 0.6,
            'avg_validity_score': 0.7,
            'avg_accuracy_score': 0.65,
            'avg_overall_score': 0.6,  # Poor quality score
            'min_overall_score': 0.3,
            'max_overall_score': 0.8
        }
        
        mock_repository.get_pipeline_run.return_value = pipeline_run
        mock_repository.get_ingestion_summary.return_value = ingestion_summary
        mock_repository.get_quality_summary.return_value = quality_summary
        
        report = reporter.generate_pipeline_report("test_run")
        
        # Should generate recommendation for poor quality
        quality_recommendations = [r for r in report.recommendations if "quality" in r.lower()]
        assert len(quality_recommendations) > 0