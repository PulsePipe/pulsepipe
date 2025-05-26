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

# tests/test_performance_collector.py

"""
Unit tests for performance metrics collection system.

Tests centralized collection, aggregation, and export functionality.
"""

import pytest
import json
import csv
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pulsepipe.pipelines.performance.collector import (
    MetricsCollector,
    get_global_collector,
    reset_global_collector
)
from pulsepipe.pipelines.performance.tracker import (
    PerformanceTracker,
    PipelineMetrics,
    StepMetrics
)


class TestMetricsCollector:
    """Test MetricsCollector class."""
    
    def test_initialization(self):
        """Test MetricsCollector initialization."""
        collector = MetricsCollector(max_pipelines=500, max_steps_per_pipeline=50)
        
        assert collector.max_pipelines == 500
        assert collector.max_steps_per_pipeline == 50
        assert len(collector.get_active_pipelines()) == 0
        assert len(collector.get_recent_pipelines()) == 0
    
    def test_create_tracker(self):
        """Test creating a new tracker."""
        collector = MetricsCollector()
        
        tracker = collector.create_tracker("pipeline_1", "Test Pipeline 1")
        
        assert isinstance(tracker, PerformanceTracker)
        assert tracker.pipeline_metrics.pipeline_id == "pipeline_1"
        assert tracker.pipeline_metrics.pipeline_name == "Test Pipeline 1"
        assert "pipeline_1" in collector.get_active_pipelines()
    
    def test_create_duplicate_tracker(self):
        """Test creating tracker with duplicate ID."""
        collector = MetricsCollector()
        
        tracker1 = collector.create_tracker("pipeline_1", "Test Pipeline 1")
        tracker2 = collector.create_tracker("pipeline_1", "Test Pipeline 1 Duplicate")
        
        # Should return the same tracker
        assert tracker1 is tracker2
        assert len(collector.get_active_pipelines()) == 1
    
    def test_get_tracker(self):
        """Test getting an existing tracker."""
        collector = MetricsCollector()
        
        tracker = collector.create_tracker("pipeline_1", "Test Pipeline 1")
        retrieved_tracker = collector.get_tracker("pipeline_1")
        
        assert retrieved_tracker is tracker
        
        # Test non-existent tracker
        assert collector.get_tracker("nonexistent") is None
    
    def test_finish_pipeline(self):
        """Test finishing a pipeline and collecting metrics."""
        collector = MetricsCollector()
        
        tracker = collector.create_tracker("pipeline_1", "Test Pipeline 1")
        tracker.start_step("step_1")
        tracker.finish_step(records_processed=100, success_count=100)
        
        metadata = {"test": "metadata"}
        metrics = collector.finish_pipeline("pipeline_1", metadata)
        
        assert isinstance(metrics, PipelineMetrics)
        assert metrics.pipeline_id == "pipeline_1"
        assert metrics.metadata["test"] == "metadata"
        assert "pipeline_1" not in collector.get_active_pipelines()
        assert len(collector.get_recent_pipelines()) == 1
        assert collector.get_pipeline_metrics("pipeline_1") == metrics
    
    def test_finish_nonexistent_pipeline(self):
        """Test finishing non-existent pipeline."""
        collector = MetricsCollector()
        
        result = collector.finish_pipeline("nonexistent")
        
        assert result is None
    
    def test_get_recent_pipelines(self):
        """Test getting recent pipelines with limit."""
        collector = MetricsCollector()
        
        # Create and finish multiple pipelines
        for i in range(5):
            tracker = collector.create_tracker(f"pipeline_{i}", f"Pipeline {i}")
            tracker.start_step("step_1")
            tracker.finish_step(records_processed=10)
            collector.finish_pipeline(f"pipeline_{i}")
        
        # Test default limit
        recent = collector.get_recent_pipelines()
        assert len(recent) == 5
        
        # Test custom limit
        recent_limited = collector.get_recent_pipelines(limit=3)
        assert len(recent_limited) == 3
        
        # Should be most recent (last 3)
        assert recent_limited[0].pipeline_id == "pipeline_2"
        assert recent_limited[1].pipeline_id == "pipeline_3"
        assert recent_limited[2].pipeline_id == "pipeline_4"
    
    def test_get_aggregated_metrics_empty(self):
        """Test aggregated metrics with no pipelines."""
        collector = MetricsCollector()
        
        metrics = collector.get_aggregated_metrics()
        
        assert metrics['summary']['total_pipelines'] == 0
        assert metrics['summary']['total_records_processed'] == 0
        assert metrics['pipeline_metrics']['avg_duration_ms'] == 0
    
    def test_get_aggregated_metrics_with_data(self):
        """Test aggregated metrics with pipeline data."""
        collector = MetricsCollector()
        
        # Create pipelines with different characteristics
        for i in range(3):
            tracker = collector.create_tracker(f"pipeline_{i}", f"Pipeline {i}")
            
            # Add steps with different performance
            tracker.start_step("step_1")
            tracker.finish_step(records_processed=100, success_count=100)
            
            tracker.start_step("step_2")
            tracker.finish_step(records_processed=50, success_count=45, failure_count=5)
            
            collector.finish_pipeline(f"pipeline_{i}")
        
        metrics = collector.get_aggregated_metrics()
        
        assert metrics['summary']['total_pipelines'] == 3
        assert metrics['summary']['total_records_processed'] == 450  # (100+50) * 3
        assert metrics['pipeline_metrics']['avg_duration_ms'] > 0
        assert metrics['step_metrics']['total_steps'] == 6  # 2 steps * 3 pipelines
        assert 'step_1' in metrics['step_metrics']['step_types']
        assert 'step_2' in metrics['step_metrics']['step_types']
    
    def test_get_aggregated_metrics_with_time_window(self):
        """Test aggregated metrics with time window filtering."""
        collector = MetricsCollector()
        
        # Create a pipeline
        tracker = collector.create_tracker("recent_pipeline", "Recent Pipeline")
        tracker.start_step("step_1")
        tracker.finish_step(records_processed=100)
        collector.finish_pipeline("recent_pipeline")
        
        # Test with very short time window (should include the pipeline)
        recent_metrics = collector.get_aggregated_metrics(timedelta(minutes=1))
        assert recent_metrics['summary']['total_pipelines'] == 1
        
        # Test with very long time window (should include the pipeline)
        old_metrics = collector.get_aggregated_metrics(timedelta(days=1))
        assert old_metrics['summary']['total_pipelines'] == 1
    
    def test_export_json(self):
        """Test exporting metrics to JSON."""
        collector = MetricsCollector()
        
        # Create a pipeline
        tracker = collector.create_tracker("test_pipeline", "Test Pipeline")
        tracker.start_step("test_step")
        tracker.finish_step(records_processed=100, success_count=100)
        collector.finish_pipeline("test_pipeline")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            export_path = f.name
        
        try:
            collector.export_metrics(export_path, 'json')
            
            # Verify the file was created and contains valid JSON
            with open(export_path) as f:
                data = json.load(f)
            
            assert 'export_metadata' in data
            assert 'aggregated_metrics' in data
            assert 'pipelines' in data
            assert len(data['pipelines']) == 1
            assert data['pipelines'][0]['pipeline_id'] == "test_pipeline"
            
        finally:
            Path(export_path).unlink(missing_ok=True)
    
    def test_export_csv(self):
        """Test exporting metrics to CSV."""
        collector = MetricsCollector()
        
        # Create pipelines
        for i in range(2):
            tracker = collector.create_tracker(f"pipeline_{i}", f"Pipeline {i}")
            tracker.start_step("step_1")
            tracker.finish_step(records_processed=50 * (i + 1))
            collector.finish_pipeline(f"pipeline_{i}")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            export_path = f.name
        
        try:
            collector.export_metrics(export_path, 'csv')
            
            # Verify the file was created and contains valid CSV
            with open(export_path) as f:
                csv_reader = csv.DictReader(f)
                rows = list(csv_reader)
            
            assert len(rows) == 2
            assert rows[0]['pipeline_id'] == "pipeline_0"
            assert rows[1]['pipeline_id'] == "pipeline_1"
            assert int(rows[0]['total_records_processed']) == 50
            assert int(rows[1]['total_records_processed']) == 100
            
        finally:
            Path(export_path).unlink(missing_ok=True)
    
    def test_export_invalid_format(self):
        """Test export with invalid format."""
        collector = MetricsCollector()
        
        with tempfile.NamedTemporaryFile() as f:
            with pytest.raises(ValueError, match="Unsupported export format"):
                collector.export_metrics(f.name, 'xml')
    
    def test_clear_old_metrics(self):
        """Test clearing old metrics."""
        collector = MetricsCollector()
        
        # Create some pipelines
        for i in range(3):
            tracker = collector.create_tracker(f"pipeline_{i}", f"Pipeline {i}")
            tracker.start_step("step_1")
            tracker.finish_step(records_processed=100)
            collector.finish_pipeline(f"pipeline_{i}")
        
        assert len(collector.get_recent_pipelines()) == 3
        
        # Clear metrics older than 1 hour (should clear nothing)
        cleared_count = collector.clear_old_metrics(timedelta(hours=1))
        assert cleared_count == 0
        assert len(collector.get_recent_pipelines()) == 3
        
        # Clear metrics older than 0 seconds (should clear everything)
        cleared_count = collector.clear_old_metrics(timedelta(seconds=0))
        assert cleared_count == 3
        assert len(collector.get_recent_pipelines()) == 0
    
    def test_get_statistics(self):
        """Test getting collector statistics."""
        collector = MetricsCollector(max_pipelines=100, max_steps_per_pipeline=50)
        
        # Create some active and completed pipelines
        tracker1 = collector.create_tracker("active_1", "Active 1")
        tracker2 = collector.create_tracker("active_2", "Active 2")
        
        tracker3 = collector.create_tracker("completed_1", "Completed 1")
        tracker3.start_step("step_1")
        tracker3.finish_step(records_processed=100)
        collector.finish_pipeline("completed_1")
        
        stats = collector.get_statistics()
        
        assert stats['active_trackers'] == 2
        assert stats['completed_pipelines'] == 1
        assert stats['stored_metrics'] == 1
        assert stats['memory_usage']['max_pipelines'] == 100
        assert stats['memory_usage']['max_steps_per_pipeline'] == 50
    
    def test_pipeline_limit(self):
        """Test that collector respects pipeline limits."""
        collector = MetricsCollector(max_pipelines=3)
        
        # Create more pipelines than the limit
        for i in range(5):
            tracker = collector.create_tracker(f"pipeline_{i}", f"Pipeline {i}")
            tracker.start_step("step_1")
            tracker.finish_step(records_processed=100)
            collector.finish_pipeline(f"pipeline_{i}")
        
        # Should only keep the most recent 3
        recent_pipelines = collector.get_recent_pipelines()
        assert len(recent_pipelines) == 3
        assert recent_pipelines[0].pipeline_id == "pipeline_2"
        assert recent_pipelines[1].pipeline_id == "pipeline_3"
        assert recent_pipelines[2].pipeline_id == "pipeline_4"
    
    def test_update_aggregates(self):
        """Test that aggregates are updated correctly."""
        collector = MetricsCollector()
        
        # Create a pipeline with specific metrics
        tracker = collector.create_tracker("test_pipeline", "Test Pipeline")
        tracker.start_step("slow_step")
        tracker.finish_step(records_processed=100, success_count=100)
        metrics = collector.finish_pipeline("test_pipeline")
        
        # Check that aggregates contain the step data
        assert len(collector._pipeline_aggregates['duration_ms']) == 1
        assert collector._pipeline_aggregates['duration_ms'][0] == metrics.total_duration_ms
        
        # Check step aggregates
        step_key = "slow_step_duration_ms"
        assert step_key in collector._step_aggregates
        assert len(collector._step_aggregates[step_key]) == 1
    
    def test_identify_common_bottlenecks(self):
        """Test identification of common bottlenecks."""
        collector = MetricsCollector()
        
        # Create pipelines with consistent bottlenecks
        for i in range(3):
            pipeline = PipelineMetrics(
                pipeline_id=f"pipeline_{i}",
                pipeline_name=f"Pipeline {i}",
                start_time=datetime.now()
            )
            
            # All pipelines have "slow_step" as a bottleneck
            pipeline.bottlenecks = ["slow_step: takes too long", "other_issue"]
            collector._completed_pipelines.append(pipeline)
        
        common_bottlenecks = collector._identify_common_bottlenecks(list(collector._completed_pipelines))
        
        assert len(common_bottlenecks) > 0
        assert common_bottlenecks[0]['step_name'] == "slow_step"
        assert common_bottlenecks[0]['occurrence_count'] == 3
        assert common_bottlenecks[0]['percentage'] == 100.0  # 3/3 * 100
    
    def test_count_step_types(self):
        """Test counting step types."""
        collector = MetricsCollector()
        
        steps = [
            StepMetrics("ingestion", datetime.now()),
            StepMetrics("processing", datetime.now()),
            StepMetrics("ingestion", datetime.now()),
            StepMetrics("export", datetime.now()),
            StepMetrics("processing", datetime.now())
        ]
        
        step_counts = collector._count_step_types(steps)
        
        assert step_counts["ingestion"] == 2
        assert step_counts["processing"] == 2
        assert step_counts["export"] == 1
    
    def test_thread_safety(self):
        """Test basic thread safety of collector operations."""
        import threading
        
        collector = MetricsCollector()
        results = []
        
        def worker(worker_id):
            try:
                tracker = collector.create_tracker(f"pipeline_{worker_id}", f"Pipeline {worker_id}")
                tracker.start_step("step_1")
                tracker.finish_step(records_processed=10)
                metrics = collector.finish_pipeline(f"pipeline_{worker_id}")
                results.append(metrics is not None)
            except Exception:
                results.append(False)
        
        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All operations should have succeeded
        assert len(results) == 3
        assert all(results)
        assert len(collector.get_recent_pipelines()) == 3


class TestGlobalCollector:
    """Test global collector functionality."""
    
    def test_get_global_collector(self):
        """Test getting global collector instance."""
        reset_global_collector()  # Ensure clean state
        
        collector1 = get_global_collector()
        collector2 = get_global_collector()
        
        # Should return the same instance
        assert collector1 is collector2
        assert isinstance(collector1, MetricsCollector)
    
    def test_reset_global_collector(self):
        """Test resetting global collector."""
        collector1 = get_global_collector()
        
        # Add some data
        tracker = collector1.create_tracker("test", "test")
        
        reset_global_collector()
        collector2 = get_global_collector()
        
        # Should be a new instance
        assert collector1 is not collector2
        assert len(collector2.get_active_pipelines()) == 0
    
    def test_global_collector_persistence(self):
        """Test that global collector persists data between calls."""
        reset_global_collector()
        
        collector = get_global_collector()
        tracker = collector.create_tracker("persistent_test", "Persistent Test")
        tracker.start_step("step_1")
        tracker.finish_step(records_processed=100)
        collector.finish_pipeline("persistent_test")
        
        # Get collector again
        collector2 = get_global_collector()
        
        # Should have the same data
        assert collector is collector2
        assert len(collector2.get_recent_pipelines()) == 1
        assert collector2.get_recent_pipelines()[0].pipeline_id == "persistent_test"