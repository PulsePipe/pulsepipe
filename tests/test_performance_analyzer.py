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

# tests/test_performance_analyzer.py

"""
Unit tests for performance analysis tools.

Tests bottleneck identification, trend analysis, and optimization recommendations.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock

from pulsepipe.pipelines.performance.analyzer import (
    PerformanceAnalyzer,
    TrendAnalysis,
    OptimizationRecommendation
)
from pulsepipe.pipelines.performance.tracker import (
    PipelineMetrics,
    StepMetrics,
    BottleneckAnalysis
)
from pulsepipe.pipelines.performance.collector import MetricsCollector


class TestTrendAnalysis:
    """Test TrendAnalysis dataclass."""
    
    def test_basic_creation(self):
        """Test basic TrendAnalysis creation."""
        trend = TrendAnalysis(
            metric_name="avg_duration_ms",
            time_period="7 days",
            trend_direction="improving",
            trend_strength=0.3,
            current_value=1500.0,
            previous_value=2000.0,
            change_percentage=-25.0,
            data_points=10,
            confidence=0.8
        )
        
        assert trend.metric_name == "avg_duration_ms"
        assert trend.trend_direction == "improving"
        assert trend.trend_strength == 0.3
        assert trend.current_value == 1500.0
        assert trend.previous_value == 2000.0
        assert trend.change_percentage == -25.0
        assert trend.confidence == 0.8
    
    def test_to_dict(self):
        """Test converting TrendAnalysis to dictionary."""
        trend = TrendAnalysis(
            metric_name="throughput",
            time_period="1 day",
            trend_direction="stable",
            trend_strength=0.1,
            current_value=100.0,
            previous_value=95.0,
            change_percentage=5.3,
            data_points=5,
            confidence=0.6
        )
        
        result = trend.to_dict()
        
        assert isinstance(result, dict)
        assert result['metric_name'] == "throughput"
        assert result['trend_direction'] == "stable"
        assert result['change_percentage'] == 5.3
        assert result['data_points'] == 5


class TestOptimizationRecommendation:
    """Test OptimizationRecommendation dataclass."""
    
    def test_basic_creation(self):
        """Test basic OptimizationRecommendation creation."""
        recommendation = OptimizationRecommendation(
            priority="high",
            category="bottleneck",
            title="Optimize slow step",
            description="Step X is taking 80% of execution time",
            impact_estimate="high",
            effort_estimate="medium",
            affected_steps=["step_x"],
            metrics_evidence={"percentage": 80}
        )
        
        assert recommendation.priority == "high"
        assert recommendation.category == "bottleneck"
        assert recommendation.title == "Optimize slow step"
        assert recommendation.impact_estimate == "high"
        assert recommendation.effort_estimate == "medium"
        assert len(recommendation.affected_steps) == 1
        assert recommendation.metrics_evidence["percentage"] == 80
    
    def test_to_dict(self):
        """Test converting OptimizationRecommendation to dictionary."""
        recommendation = OptimizationRecommendation(
            priority="medium",
            category="throughput",
            title="Increase batch size",
            description="Current batch size is too small",
            impact_estimate="medium",
            effort_estimate="low"
        )
        
        result = recommendation.to_dict()
        
        assert isinstance(result, dict)
        assert result['priority'] == "medium"
        assert result['category'] == "throughput"
        assert result['title'] == "Increase batch size"
        assert result['affected_steps'] == []
        assert result['metrics_evidence'] == {}


class TestPerformanceAnalyzer:
    """Test PerformanceAnalyzer class."""
    
    def test_initialization_no_collector(self):
        """Test analyzer initialization without collector."""
        analyzer = PerformanceAnalyzer()
        assert analyzer.collector is None
    
    def test_initialization_with_collector(self):
        """Test analyzer initialization with collector."""
        collector = MetricsCollector()
        analyzer = PerformanceAnalyzer(collector)
        assert analyzer.collector is collector
    
    def test_analyze_bottlenecks(self):
        """Test bottleneck analysis for specific pipeline."""
        analyzer = PerformanceAnalyzer()
        
        # Create pipeline with bottlenecks
        pipeline = PipelineMetrics(
            pipeline_id="test_pipeline",
            pipeline_name="Test Pipeline",
            start_time=datetime.now()
        )
        
        # Add slow step
        slow_step = StepMetrics("slow_step", datetime.now())
        slow_step.duration_ms = 800
        slow_step.records_processed = 100
        slow_step.success_count = 100
        slow_step.records_per_second = 125
        
        pipeline.add_step_metrics(slow_step)
        pipeline.total_duration_ms = 1000
        
        analysis = analyzer.analyze_bottlenecks(pipeline)
        
        assert isinstance(analysis, BottleneckAnalysis)
        assert analysis.pipeline_id == "test_pipeline"
        assert analysis.total_duration_ms == 1000
    
    def test_analyze_trends_no_collector(self):
        """Test trend analysis without collector raises error."""
        analyzer = PerformanceAnalyzer()
        
        with pytest.raises(ValueError, match="MetricsCollector required"):
            analyzer.analyze_trends([timedelta(days=7)])
    
    def test_analyze_trends_with_collector(self):
        """Test trend analysis with mock collector."""
        mock_collector = Mock()
        analyzer = PerformanceAnalyzer(mock_collector)
        
        # Mock aggregated metrics for different time periods
        current_metrics = {
            'pipeline_metrics': {'avg_duration_ms': 1500, 'avg_throughput': 100},
            'summary': {'total_records_processed': 1000, 'total_pipelines': 5},
            'bottlenecks': {'bottleneck_rate': 0.2}
        }
        
        previous_metrics = {
            'pipeline_metrics': {'avg_duration_ms': 2000, 'avg_throughput': 80},
            'summary': {'total_records_processed': 800, 'total_pipelines': 4},
            'bottlenecks': {'bottleneck_rate': 0.3}
        }
        
        # Mock collector to return different metrics for different time periods
        def mock_get_aggregated_metrics(time_period):
            if time_period == timedelta(days=7):
                return current_metrics
            else:  # timedelta(days=14)
                return previous_metrics
        
        mock_collector.get_aggregated_metrics = mock_get_aggregated_metrics
        
        trends = analyzer.analyze_trends([timedelta(days=7)])
        
        assert isinstance(trends, list)
        assert len(trends) > 0
        
        # Find duration trend
        duration_trends = [t for t in trends if t.metric_name == 'avg_pipeline_duration_ms']
        if duration_trends:
            trend = duration_trends[0]
            assert trend.current_value == 1500
            assert trend.previous_value == 2000
            assert trend.change_percentage == -25.0  # (1500-2000)/2000 * 100
            assert trend.trend_direction == "improving"  # Duration decreased
    
    def test_generate_optimization_recommendations_single_pipeline(self):
        """Test generating recommendations for single pipeline."""
        analyzer = PerformanceAnalyzer()
        
        # Create pipeline with issues
        pipeline = PipelineMetrics(
            pipeline_id="slow_pipeline",
            pipeline_name="Slow Pipeline",
            start_time=datetime.now()
        )
        
        # Add very slow step
        slow_step = StepMetrics("bottleneck_step", datetime.now())
        slow_step.duration_ms = 180000  # 3 minutes
        slow_step.records_processed = 100
        slow_step.success_count = 100
        
        pipeline.add_step_metrics(slow_step)
        pipeline.total_duration_ms = 200000  # Total > 1 minute
        pipeline.avg_records_per_second = 5  # Low throughput < 10
        pipeline.total_success_count = 90
        pipeline.total_failure_count = 10  # 10% failure rate
        
        recommendations = analyzer.generate_optimization_recommendations(pipeline_metrics=pipeline)
        
        assert len(recommendations) > 0
        
        # Should have bottleneck recommendation
        bottleneck_recs = [r for r in recommendations if r.category == "bottleneck"]
        assert len(bottleneck_recs) > 0
        
        # Should have throughput recommendation
        throughput_recs = [r for r in recommendations if r.category == "throughput"]
        assert len(throughput_recs) > 0
        
        # Should have reliability recommendation
        reliability_recs = [r for r in recommendations if r.category == "reliability"]
        assert len(reliability_recs) > 0
    
    def test_generate_optimization_recommendations_aggregated(self):
        """Test generating recommendations from aggregated metrics."""
        analyzer = PerformanceAnalyzer()
        
        aggregated_metrics = {
            'pipeline_metrics': {
                'avg_duration_ms': 400000  # > 5 minutes
            },
            'bottlenecks': {
                'common_bottlenecks': [
                    {'step_name': 'data_processing', 'percentage': 25.0, 'occurrence_count': 5},
                    {'step_name': 'validation', 'percentage': 15.0, 'occurrence_count': 3}
                ]
            }
        }
        
        recommendations = analyzer.generate_optimization_recommendations(
            aggregated_metrics=aggregated_metrics
        )
        
        assert len(recommendations) > 0
        
        # Should have recommendation for long duration
        duration_recs = [r for r in recommendations if "duration" in r.description.lower()]
        assert len(duration_recs) > 0
        
        # Should have recommendation for common bottleneck
        bottleneck_recs = [r for r in recommendations if "data_processing" in r.title]
        assert len(bottleneck_recs) > 0
    
    def test_generate_recommendations_sorting(self):
        """Test that recommendations are sorted by priority."""
        analyzer = PerformanceAnalyzer()
        
        # Create pipeline with multiple issues
        pipeline = PipelineMetrics(
            pipeline_id="multi_issue_pipeline",
            pipeline_name="Multi Issue Pipeline",
            start_time=datetime.now()
        )
        
        # Add issues that generate different priority recommendations
        slow_step = StepMetrics("slow_step", datetime.now())
        slow_step.duration_ms = 120000  # 2 minutes - medium issue
        pipeline.add_step_metrics(slow_step)
        
        pipeline.total_duration_ms = 150000
        pipeline.avg_records_per_second = 5  # Low throughput
        pipeline.total_success_count = 50
        pipeline.total_failure_count = 50  # 50% failure rate - high priority
        
        recommendations = analyzer.generate_optimization_recommendations(pipeline_metrics=pipeline)
        
        # Should be sorted with high priority first
        priorities = [r.priority for r in recommendations]
        assert priorities == sorted(priorities, key=lambda x: {'high': 0, 'medium': 1, 'low': 2}[x])
    
    def test_identify_performance_patterns_no_collector(self):
        """Test performance patterns without collector raises error."""
        analyzer = PerformanceAnalyzer()
        
        with pytest.raises(ValueError, match="MetricsCollector required"):
            analyzer.identify_performance_patterns(timedelta(days=7))
    
    def test_identify_performance_patterns(self):
        """Test identifying performance patterns."""
        mock_collector = Mock()
        analyzer = PerformanceAnalyzer(mock_collector)
        
        mock_aggregated_metrics = {
            'step_metrics': {
                'step_types': {
                    'ingestion': 10,
                    'processing': 8,
                    'validation': 5,
                    'export': 3
                },
                'total_steps': 26
            },
            'pipeline_metrics': {
                'min_throughput': 50,
                'max_throughput': 200,
                'avg_throughput': 125
            },
            'bottlenecks': {
                'bottleneck_rate': 0.15,
                'common_bottlenecks': [
                    {'step_name': 'slow_processing', 'percentage': 20}
                ]
            },
            'summary': {'total_pipelines': 5}
        }
        
        mock_collector.get_aggregated_metrics.return_value = mock_aggregated_metrics
        
        patterns = analyzer.identify_performance_patterns(timedelta(days=7))
        
        assert 'execution_patterns' in patterns
        assert 'throughput_patterns' in patterns
        assert 'failure_patterns' in patterns
        assert 'temporal_patterns' in patterns
        
        # Check execution patterns
        exec_patterns = patterns['execution_patterns']
        assert 'most_common_steps' in exec_patterns
        assert exec_patterns['most_common_steps'][0][0] == 'ingestion'  # Most common
        assert exec_patterns['avg_steps_per_pipeline'] == 26 / 5  # total_steps / total_pipelines
        
        # Check throughput patterns
        throughput_patterns = patterns['throughput_patterns']
        variability = throughput_patterns['throughput_variability']
        assert variability['min'] == 50
        assert variability['max'] == 200
        assert variability['range'] == 150
    
    def test_compare_pipelines_insufficient_data(self):
        """Test pipeline comparison with insufficient data."""
        mock_collector = Mock()
        analyzer = PerformanceAnalyzer(mock_collector)
        
        # Mock collector to return only one pipeline
        mock_collector.get_pipeline_metrics.return_value = None
        
        result = analyzer.compare_pipelines(["pipeline_1", "pipeline_2"])
        
        assert 'error' in result
        assert "At least 2 pipelines required" in result['error']
    
    def test_compare_pipelines_success(self):
        """Test successful pipeline comparison."""
        mock_collector = Mock()
        analyzer = PerformanceAnalyzer(mock_collector)
        
        # Create mock pipelines
        pipeline1 = PipelineMetrics("pipeline_1", "Pipeline 1", datetime.now())
        pipeline1.total_duration_ms = 1000
        pipeline1.avg_records_per_second = 100
        pipeline1.bottlenecks = ["slow_step: issue"]
        
        step1 = StepMetrics("step_a", datetime.now())
        step1.duration_ms = 500
        step1.records_per_second = 200
        pipeline1.add_step_metrics(step1)
        
        pipeline2 = PipelineMetrics("pipeline_2", "Pipeline 2", datetime.now())
        pipeline2.total_duration_ms = 1500
        pipeline2.avg_records_per_second = 80
        pipeline2.bottlenecks = ["slow_step: issue", "another_issue"]
        
        step2 = StepMetrics("step_a", datetime.now())
        step2.duration_ms = 700
        step2.records_per_second = 150
        pipeline2.add_step_metrics(step2)
        
        # Mock collector to return these pipelines
        def mock_get_pipeline_metrics(pipeline_id):
            if pipeline_id == "pipeline_1":
                return pipeline1
            elif pipeline_id == "pipeline_2":
                return pipeline2
            return None
        
        mock_collector.get_pipeline_metrics = mock_get_pipeline_metrics
        
        result = analyzer.compare_pipelines(["pipeline_1", "pipeline_2"])
        
        assert result['pipeline_count'] == 2
        assert 'duration_comparison' in result
        assert 'throughput_comparison' in result
        assert 'bottleneck_comparison' in result
        assert 'step_comparison' in result
        assert 'recommendations' in result
        
        # Check duration comparison
        duration_comp = result['duration_comparison']
        assert duration_comp['min_duration_ms'] == 1000
        assert duration_comp['max_duration_ms'] == 1500
        assert duration_comp['avg_duration_ms'] == 1250
        
        # Check bottleneck comparison
        bottleneck_comp = result['bottleneck_comparison']
        assert bottleneck_comp['total_bottlenecks'] == 3  # 1 + 2 bottlenecks
        assert bottleneck_comp['unique_bottleneck_types'] == 2  # "slow_step" and "another_issue"
    
    def test_extract_metric_value(self):
        """Test extracting metric values from aggregated metrics."""
        analyzer = PerformanceAnalyzer()
        
        metrics = {
            'pipeline_metrics': {
                'avg_duration_ms': 1500.5,
                'avg_throughput': 125.0
            },
            'summary': {
                'total_records_processed': 10000
            },
            'bottlenecks': {
                'bottleneck_rate': 0.25
            }
        }
        
        assert analyzer._extract_metric_value(metrics, 'avg_pipeline_duration_ms') == 1500.5
        assert analyzer._extract_metric_value(metrics, 'avg_pipeline_throughput') == 125.0
        assert analyzer._extract_metric_value(metrics, 'total_records_processed') == 10000
        assert analyzer._extract_metric_value(metrics, 'bottleneck_rate') == 0.25
        assert analyzer._extract_metric_value(metrics, 'nonexistent_metric') is None
    
    def test_analyze_single_pipeline_good_performance(self):
        """Test analysis of pipeline with good performance."""
        analyzer = PerformanceAnalyzer()
        
        # Create pipeline with good performance
        pipeline = PipelineMetrics(
            pipeline_id="good_pipeline",
            pipeline_name="Good Pipeline",
            start_time=datetime.now()
        )
        
        pipeline.total_duration_ms = 30000  # 30 seconds - reasonable
        pipeline.avg_records_per_second = 50  # Good throughput
        pipeline.total_success_count = 1000
        pipeline.total_failure_count = 5  # 0.5% failure rate - good
        
        recommendations = analyzer._analyze_single_pipeline(pipeline)
        
        # Should have few or no recommendations for good performance
        high_priority_recs = [r for r in recommendations if r.priority == "high"]
        assert len(high_priority_recs) == 0  # No high priority issues
    
    def test_analyze_aggregated_metrics_good_performance(self):
        """Test analysis of aggregated metrics with good performance."""
        analyzer = PerformanceAnalyzer()
        
        metrics = {
            'pipeline_metrics': {
                'avg_duration_ms': 120000  # 2 minutes - reasonable
            },
            'bottlenecks': {
                'common_bottlenecks': [
                    {'step_name': 'minor_issue', 'percentage': 5.0}  # Low percentage
                ]
            }
        }
        
        recommendations = analyzer._analyze_aggregated_metrics(metrics)
        
        # Should have few recommendations for good performance
        assert len(recommendations) <= 1  # At most one minor recommendation
    
    def test_compare_durations_no_data(self):
        """Test duration comparison with no data."""
        analyzer = PerformanceAnalyzer()
        
        pipeline1 = PipelineMetrics("test1", "Test 1", datetime.now())
        pipeline1.total_duration_ms = None  # No duration data
        
        result = analyzer._compare_durations([pipeline1])
        
        assert 'error' in result
        assert 'No duration data available' in result['error']
    
    def test_compare_throughputs_no_data(self):
        """Test throughput comparison with no data."""
        analyzer = PerformanceAnalyzer()
        
        pipeline1 = PipelineMetrics("test1", "Test 1", datetime.now())
        pipeline1.avg_records_per_second = 0  # No throughput data
        
        result = analyzer._compare_throughputs([pipeline1])
        
        assert 'error' in result
        assert 'No throughput data available' in result['error']