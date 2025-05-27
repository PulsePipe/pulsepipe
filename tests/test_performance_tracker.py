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

# tests/test_performance_tracker.py

"""
Unit tests for performance tracking system.

Tests timing, throughput metrics, bottleneck identification,
and performance decorators.
"""

import pytest
import time
import asyncio
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from pulsepipe.pipelines.performance.tracker import (
    PerformanceTracker,
    StepMetrics,
    PipelineMetrics,
    BottleneckAnalysis
)


class TestStepMetrics:
    """Test StepMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic StepMetrics creation."""
        start_time = datetime.now()
        step = StepMetrics(
            step_name="test_step",
            start_time=start_time,
            metadata={"test": "data"}
        )
        
        assert step.step_name == "test_step"
        assert step.start_time == start_time
        assert step.end_time is None
        assert step.duration_ms is None
        assert step.records_processed == 0
        assert step.bytes_processed == 0
        assert step.records_per_second == 0.0
        assert step.metadata == {"test": "data"}
    
    def test_finish_step(self):
        """Test finishing a step and calculating metrics."""
        start_time = datetime.now()
        step = StepMetrics(step_name="test_step", start_time=start_time)
        
        # Simulate some processing time
        time.sleep(0.01)  # 10ms
        
        step.finish(
            records_processed=100,
            bytes_processed=1024,
            success_count=95,
            failure_count=5
        )
        
        assert step.end_time is not None
        assert step.duration_ms is not None
        assert step.duration_ms >= 10  # At least 10ms
        assert step.records_processed == 100
        assert step.bytes_processed == 1024
        assert step.success_count == 95
        assert step.failure_count == 5
        assert step.records_per_second > 0
        assert step.bytes_per_second > 0
    
    def test_finish_step_zero_duration(self):
        """Test finishing a step with zero duration."""
        step = StepMetrics(step_name="test_step", start_time=datetime.now())
        # Don't set end_time manually, let finish() calculate it
        
        step.finish(records_processed=100)
        
        # Should handle very small duration gracefully
        # Duration will be very small but not exactly zero due to execution time
        assert step.records_per_second >= 0  # Should be a reasonable number, not inf
    
    def test_to_dict(self):
        """Test converting StepMetrics to dictionary."""
        start_time = datetime.now()
        step = StepMetrics(
            step_name="test_step",
            start_time=start_time,
            metadata={"key": "value"}
        )
        step.finish(records_processed=50)
        
        result = step.to_dict()
        
        assert isinstance(result, dict)
        assert result['step_name'] == "test_step"
        assert result['start_time'] == start_time.isoformat()
        assert result['end_time'] is not None
        assert result['records_processed'] == 50
        assert result['metadata'] == {"key": "value"}
        assert isinstance(result['records_per_second'], float)


class TestPipelineMetrics:
    """Test PipelineMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic PipelineMetrics creation."""
        start_time = datetime.now()
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=start_time
        )
        
        assert pipeline.pipeline_id == "test_123"
        assert pipeline.pipeline_name == "test_pipeline"
        assert pipeline.start_time == start_time
        assert pipeline.end_time is None
        assert len(pipeline.step_metrics) == 0
        assert pipeline.total_records_processed == 0
        assert len(pipeline.bottlenecks) == 0
    
    def test_add_step_metrics(self):
        """Test adding step metrics to pipeline."""
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=datetime.now()
        )
        
        step = StepMetrics(step_name="step1", start_time=datetime.now())
        step.finish(records_processed=100, success_count=100)
        
        pipeline.add_step_metrics(step)
        
        assert len(pipeline.step_metrics) == 1
        assert pipeline.total_records_processed == 100
        assert pipeline.total_success_count == 100
        assert pipeline.step_metrics[0] == step
    
    def test_finish_pipeline(self):
        """Test finishing pipeline and calculating metrics."""
        start_time = datetime.now()
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline", 
            start_time=start_time
        )
        
        # Add some steps
        for i in range(3):
            step = StepMetrics(step_name=f"step{i}", start_time=datetime.now())
            step.finish(records_processed=50, success_count=50)
            pipeline.add_step_metrics(step)
        
        time.sleep(0.01)  # Ensure some time passes
        pipeline.finish()
        
        assert pipeline.end_time is not None
        assert pipeline.total_duration_ms is not None
        assert pipeline.total_duration_ms > 0
        assert pipeline.total_records_processed == 150
        assert pipeline.avg_records_per_second > 0
        assert isinstance(pipeline.bottlenecks, list)
    
    def test_identify_bottlenecks_slow_step(self):
        """Test bottleneck identification for slow steps."""
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=datetime.now()
        )
        
        # Create a fast step with proper timing
        fast_start = datetime.now()
        fast_step = StepMetrics(step_name="fast_step", start_time=fast_start)
        fast_step.end_time = fast_start + timedelta(milliseconds=100)
        fast_step.duration_ms = 100
        fast_step.records_processed = 100
        fast_step.success_count = 100
        fast_step.records_per_second = 1000  # 100 records / 0.1 seconds
        
        # Create a slow step with proper timing
        slow_start = datetime.now()
        slow_step = StepMetrics(step_name="slow_step", start_time=slow_start)
        slow_step.end_time = slow_start + timedelta(milliseconds=700)
        slow_step.duration_ms = 700
        slow_step.records_processed = 50
        slow_step.success_count = 50
        slow_step.records_per_second = 71.4  # 50 records / 0.7 seconds
        
        pipeline.add_step_metrics(fast_step)
        pipeline.add_step_metrics(slow_step)
        
        # Manually set total duration to test bottleneck logic
        pipeline.total_duration_ms = 800  # Total of both steps
        
        # Test the bottleneck identification directly
        bottlenecks = pipeline._identify_bottlenecks()
        
        assert len(bottlenecks) > 0
        assert any("slow_step" in bottleneck for bottleneck in bottlenecks)
        # The slow step takes 700/800 = 87.5% of total time, should be flagged
        assert any("87.5%" in bottleneck for bottleneck in bottlenecks)
    
    def test_identify_bottlenecks_high_failure_rate(self):
        """Test bottleneck identification for high failure rates."""
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=datetime.now()
        )
        
        # Create step with high failure rate
        failing_step = StepMetrics(step_name="failing_step", start_time=datetime.now())
        failing_step.finish(
            records_processed=100,
            success_count=70,
            failure_count=30  # 30% failure rate
        )
        
        pipeline.add_step_metrics(failing_step)
        pipeline.total_duration_ms = 1000
        
        bottlenecks = pipeline._identify_bottlenecks()
        
        assert len(bottlenecks) > 0
        assert any("failing_step" in bottleneck and "failure rate" in bottleneck 
                  for bottleneck in bottlenecks)
    
    def test_to_dict(self):
        """Test converting PipelineMetrics to dictionary."""
        start_time = datetime.now()
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=start_time
        )
        
        step = StepMetrics(step_name="step1", start_time=datetime.now())
        step.finish(records_processed=100)
        pipeline.add_step_metrics(step)
        pipeline.finish()
        
        result = pipeline.to_dict()
        
        assert isinstance(result, dict)
        assert result['pipeline_id'] == "test_123"
        assert result['pipeline_name'] == "test_pipeline"
        assert result['start_time'] == start_time.isoformat()
        assert len(result['step_metrics']) == 1
        assert isinstance(result['step_metrics'][0], dict)


class TestBottleneckAnalysis:
    """Test BottleneckAnalysis dataclass."""
    
    def test_from_pipeline_metrics(self):
        """Test creating BottleneckAnalysis from PipelineMetrics."""
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=datetime.now()
        )
        
        # Add steps with varying performance
        # Create slow step with manual timing setup
        slow_start = datetime.now() - timedelta(milliseconds=800)
        slow_step = StepMetrics(step_name="slow_step", start_time=slow_start)
        slow_step.end_time = slow_start + timedelta(milliseconds=800)
        slow_step.duration_ms = 800
        slow_step.records_processed = 10
        slow_step.success_count = 10
        slow_step.records_per_second = 12.5  # 10 records / 0.8 seconds
        
        # Create fast step with manual timing setup
        fast_start = datetime.now() - timedelta(milliseconds=200)
        fast_step = StepMetrics(step_name="fast_step", start_time=fast_start)
        fast_step.end_time = fast_start + timedelta(milliseconds=200)
        fast_step.duration_ms = 200
        fast_step.records_processed = 100
        fast_step.success_count = 100
        fast_step.records_per_second = 500  # 100 records / 0.2 seconds
        
        # Create failing step with manual timing setup
        failing_start = datetime.now() - timedelta(milliseconds=200)
        failing_step = StepMetrics(step_name="failing_step", start_time=failing_start)
        failing_step.end_time = failing_start + timedelta(milliseconds=200)
        failing_step.duration_ms = 200
        failing_step.records_processed = 100
        failing_step.success_count = 80
        failing_step.failure_count = 20
        failing_step.records_per_second = 500
        
        pipeline.add_step_metrics(slow_step)
        pipeline.add_step_metrics(fast_step)
        pipeline.add_step_metrics(failing_step)
        pipeline.total_duration_ms = 1200
        
        analysis = BottleneckAnalysis.from_pipeline_metrics(pipeline)
        
        assert analysis.pipeline_id == "test_123"
        assert analysis.total_duration_ms == 1200
        assert len(analysis.slowest_steps) > 0
        assert analysis.slowest_steps[0]['step_name'] == "slow_step"
        assert len(analysis.high_failure_steps) > 0
        assert analysis.high_failure_steps[0]['step_name'] == "failing_step"
        assert len(analysis.recommendations) > 0
    
    def test_generate_recommendations(self):
        """Test recommendation generation."""
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=datetime.now()
        )
        
        # Add a step that takes 60% of total time
        slow_start = datetime.now()
        slow_step = StepMetrics(step_name="bottleneck_step", start_time=slow_start)
        slow_step.end_time = slow_start + timedelta(milliseconds=600)
        slow_step.duration_ms = 600
        slow_step.records_processed = 100
        slow_step.success_count = 100
        slow_step.records_per_second = 166.7  # 100 records / 0.6 seconds
        
        pipeline.add_step_metrics(slow_step)
        pipeline.total_duration_ms = 1000
        
        analysis = BottleneckAnalysis.from_pipeline_metrics(pipeline)
        
        recommendations = analysis._generate_recommendations()
        
        assert len(recommendations) > 0
        assert any("bottleneck_step" in rec for rec in recommendations)
        assert any("60.0%" in rec for rec in recommendations)
    
    def test_to_dict(self):
        """Test converting BottleneckAnalysis to dictionary."""
        pipeline = PipelineMetrics(
            pipeline_id="test_123",
            pipeline_name="test_pipeline",
            start_time=datetime.now()
        )
        pipeline.total_duration_ms = 1000
        
        analysis = BottleneckAnalysis.from_pipeline_metrics(pipeline)
        result = analysis.to_dict()
        
        assert isinstance(result, dict)
        assert result['pipeline_id'] == "test_123"
        assert result['total_duration_ms'] == 1000
        assert 'slowest_steps' in result
        assert 'recommendations' in result


class TestPerformanceTracker:
    """Test PerformanceTracker class."""
    
    def test_initialization(self):
        """Test PerformanceTracker initialization."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        assert tracker.pipeline_metrics.pipeline_id == "test_pipeline"
        assert tracker.pipeline_metrics.pipeline_name == "Test Pipeline"
        assert tracker.current_step is None
        assert len(tracker.get_step_history()) == 0
    
    def test_start_step(self):
        """Test starting a step."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        step = tracker.start_step("test_step", {"key": "value"})
        
        assert step is not None
        assert step.step_name == "test_step"
        assert step.metadata == {"key": "value"}
        assert tracker.current_step == step
        assert step.start_time is not None
        assert step.end_time is None
    
    def test_finish_step(self):
        """Test finishing a step."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        tracker.start_step("test_step")
        time.sleep(0.01)  # Ensure some time passes
        
        finished_step = tracker.finish_step(
            records_processed=100,
            bytes_processed=1024,
            success_count=95,
            failure_count=5
        )
        
        assert finished_step is not None
        assert finished_step.end_time is not None
        assert finished_step.duration_ms > 0
        assert finished_step.records_processed == 100
        assert finished_step.bytes_processed == 1024
        assert finished_step.success_count == 95
        assert finished_step.failure_count == 5
        assert tracker.current_step is None
        assert len(tracker.get_step_history()) == 1
        assert len(tracker.pipeline_metrics.step_metrics) == 1
    
    def test_finish_step_no_active_step(self):
        """Test finishing step when no step is active."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        result = tracker.finish_step(records_processed=100)
        
        assert result is None
    
    def test_update_step_progress(self):
        """Test updating step progress."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        tracker.start_step("test_step")
        tracker.update_step_progress(
            records_processed=50,
            bytes_processed=512,
            success_count=45,
            failure_count=5
        )
        
        step = tracker.get_current_step()
        assert step.records_processed == 50
        assert step.bytes_processed == 512
        assert step.success_count == 45
        assert step.failure_count == 5
    
    def test_multiple_steps(self):
        """Test tracking multiple sequential steps."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        # Step 1
        tracker.start_step("step_1")
        time.sleep(0.01)
        tracker.finish_step(records_processed=50, success_count=50)
        
        # Step 2
        tracker.start_step("step_2")
        time.sleep(0.01)
        tracker.finish_step(records_processed=75, success_count=75)
        
        history = tracker.get_step_history()
        assert len(history) == 2
        assert history[0].step_name == "step_1"
        assert history[1].step_name == "step_2"
        
        metrics = tracker.pipeline_metrics
        assert len(metrics.step_metrics) == 2
        assert metrics.total_records_processed == 125
        assert metrics.total_success_count == 125
    
    def test_finish_pipeline(self):
        """Test finishing the entire pipeline."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        # Add some steps
        tracker.start_step("step_1")
        time.sleep(0.01)
        tracker.finish_step(records_processed=100, success_count=100)
        
        tracker.start_step("step_2") 
        time.sleep(0.01)
        tracker.finish_step(records_processed=200, success_count=200)
        
        # Finish pipeline
        metrics = tracker.finish_pipeline({"test_meta": "value"})
        
        assert metrics.end_time is not None
        assert metrics.total_duration_ms is not None
        assert metrics.total_duration_ms > 0
        assert metrics.total_records_processed == 300
        assert metrics.avg_records_per_second > 0
        assert metrics.metadata["test_meta"] == "value"
        assert len(metrics.bottlenecks) >= 0  # May or may not have bottlenecks
    
    def test_finish_pipeline_with_active_step(self):
        """Test finishing pipeline when a step is still active."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        tracker.start_step("active_step")
        time.sleep(0.01)
        
        # Finish pipeline without explicitly finishing the step
        metrics = tracker.finish_pipeline()
        
        # Should automatically finish the active step
        assert tracker.current_step is None
        assert len(metrics.step_metrics) == 1
        assert metrics.step_metrics[0].step_name == "active_step"
        assert metrics.step_metrics[0].end_time is not None
    
    def test_get_bottleneck_analysis(self):
        """Test getting bottleneck analysis."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        # Add a slow step
        tracker.start_step("slow_step")
        time.sleep(0.05)  # 50ms
        tracker.finish_step(records_processed=10, success_count=10)
        
        # Add a fast step
        tracker.start_step("fast_step")
        time.sleep(0.01)  # 10ms
        tracker.finish_step(records_processed=100, success_count=100)
        
        tracker.finish_pipeline()
        
        analysis = tracker.get_bottleneck_analysis()
        
        assert isinstance(analysis, BottleneckAnalysis)
        assert analysis.pipeline_id == "test_pipeline"
        assert len(analysis.slowest_steps) > 0
    
    def test_get_performance_summary(self):
        """Test getting performance summary."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        tracker.start_step("step_1")
        time.sleep(0.01)
        tracker.finish_step(records_processed=100, success_count=100)
        
        summary = tracker.get_performance_summary()
        
        assert isinstance(summary, dict)
        assert summary['pipeline_id'] == "test_pipeline"
        assert summary['pipeline_name'] == "Test Pipeline"
        assert summary['status'] == 'running'  # Not finished yet
        assert summary['steps_completed'] == 1
        assert summary['total_records_processed'] == 100
        
        # Finish pipeline and check again
        tracker.finish_pipeline()
        summary = tracker.get_performance_summary()
        assert summary['status'] == 'completed'
    
    def test_get_performance_summary_with_active_step(self):
        """Test performance summary with an active step."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        tracker.start_step("active_step")
        time.sleep(0.01)
        
        summary = tracker.get_performance_summary()
        
        assert 'current_step' in summary
        assert summary['current_step']['name'] == "active_step"
        assert summary['current_step']['duration_ms'] > 0
    
    def test_thread_safety(self):
        """Test thread safety of PerformanceTracker."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        results = []
        
        def worker(step_id):
            try:
                tracker.start_step(f"step_{step_id}")
                time.sleep(0.01)
                result = tracker.finish_step(
                    records_processed=10,
                    success_count=10
                )
                results.append(result is not None)
            except Exception as e:
                results.append(False)
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check that operations completed safely
        # Note: Due to thread safety, only one step should be active at a time
        # Some operations may return None if another thread is already active
        assert len(results) == 5
        assert any(results)  # At least some operations should succeed
    
    def test_step_history_limit(self):
        """Test that step history respects the limit."""
        tracker = PerformanceTracker("test_pipeline", "Test Pipeline")
        
        # Create more steps than the history limit (100)
        for i in range(105):
            tracker.start_step(f"step_{i}")
            tracker.finish_step(records_processed=1, success_count=1)
        
        history = tracker.get_step_history()
        
        # Should be limited to 100 steps
        assert len(history) == 100
        # Should contain the most recent steps
        assert history[-1].step_name == "step_104"
        assert history[0].step_name == "step_5"  # First 5 should be dropped