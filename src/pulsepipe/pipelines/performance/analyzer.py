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

# src/pulsepipe/pipelines/performance/analyzer.py

"""
Performance analysis tools for PulsePipe.

Provides bottleneck identification, trend analysis, and optimization recommendations.
"""

import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from .tracker import PipelineMetrics, StepMetrics, BottleneckAnalysis
from .collector import MetricsCollector
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


@dataclass
class TrendAnalysis:
    """Analysis of performance trends over time."""
    
    metric_name: str
    time_period: str
    trend_direction: str  # 'improving', 'degrading', 'stable'
    trend_strength: float  # 0.0 to 1.0
    current_value: float
    previous_value: float
    change_percentage: float
    data_points: int
    confidence: float  # 0.0 to 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'metric_name': self.metric_name,
            'time_period': self.time_period,
            'trend_direction': self.trend_direction,
            'trend_strength': self.trend_strength,
            'current_value': self.current_value,
            'previous_value': self.previous_value,
            'change_percentage': self.change_percentage,
            'data_points': self.data_points,
            'confidence': self.confidence
        }


@dataclass
class OptimizationRecommendation:
    """Recommendation for performance optimization."""
    
    priority: str  # 'high', 'medium', 'low'
    category: str  # 'bottleneck', 'throughput', 'reliability', 'resource'
    title: str
    description: str
    impact_estimate: str  # 'high', 'medium', 'low'
    effort_estimate: str  # 'high', 'medium', 'low'
    affected_steps: List[str] = field(default_factory=list)
    metrics_evidence: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'priority': self.priority,
            'category': self.category,
            'title': self.title,
            'description': self.description,
            'impact_estimate': self.impact_estimate,
            'effort_estimate': self.effort_estimate,
            'affected_steps': self.affected_steps,
            'metrics_evidence': self.metrics_evidence
        }


class PerformanceAnalyzer:
    """
    Advanced performance analysis for PulsePipe pipelines.
    
    Provides bottleneck identification, trend analysis, and optimization recommendations.
    """
    
    def __init__(self, collector: Optional[MetricsCollector] = None):
        self.collector = collector
    
    def analyze_bottlenecks(self, pipeline_metrics: PipelineMetrics) -> BottleneckAnalysis:
        """Analyze bottlenecks for a specific pipeline."""
        return BottleneckAnalysis.from_pipeline_metrics(pipeline_metrics)
    
    def analyze_trends(self, time_periods: List[timedelta], 
                      metrics: List[str] = None) -> List[TrendAnalysis]:
        """
        Analyze performance trends over multiple time periods.
        
        Args:
            time_periods: List of time periods to compare (e.g., [7 days, 30 days])
            metrics: List of metrics to analyze (defaults to key metrics)
        """
        if not self.collector:
            raise ValueError("MetricsCollector required for trend analysis")
        
        if metrics is None:
            metrics = [
                'avg_pipeline_duration_ms',
                'avg_pipeline_throughput', 
                'total_records_processed',
                'bottleneck_rate'
            ]
        
        trends = []
        
        for period in time_periods:
            for metric in metrics:
                trend = self._analyze_metric_trend(metric, period)
                if trend:
                    trends.append(trend)
        
        return trends
    
    def generate_optimization_recommendations(self, 
                                           pipeline_metrics: Optional[PipelineMetrics] = None,
                                           aggregated_metrics: Optional[Dict[str, Any]] = None) -> List[OptimizationRecommendation]:
        """
        Generate optimization recommendations based on metrics.
        
        Args:
            pipeline_metrics: Metrics for a specific pipeline
            aggregated_metrics: Aggregated metrics across multiple pipelines
        """
        recommendations = []
        
        # Analyze single pipeline if provided
        if pipeline_metrics:
            recommendations.extend(self._analyze_single_pipeline(pipeline_metrics))
        
        # Analyze aggregated metrics if provided
        if aggregated_metrics:
            recommendations.extend(self._analyze_aggregated_metrics(aggregated_metrics))
        
        # Sort by priority
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        recommendations.sort(key=lambda x: priority_order.get(x.priority, 3))
        
        return recommendations
    
    def identify_performance_patterns(self, time_window: timedelta) -> Dict[str, Any]:
        """
        Identify patterns in performance data over a time window.
        
        Returns:
            Dictionary with pattern analysis results
        """
        if not self.collector:
            raise ValueError("MetricsCollector required for pattern analysis")
        
        aggregated = self.collector.get_aggregated_metrics(time_window)
        
        patterns = {
            'execution_patterns': self._analyze_execution_patterns(aggregated),
            'throughput_patterns': self._analyze_throughput_patterns(aggregated),
            'failure_patterns': self._analyze_failure_patterns(aggregated),
            'temporal_patterns': self._analyze_temporal_patterns(time_window)
        }
        
        return patterns
    
    def compare_pipelines(self, pipeline_ids: List[str]) -> Dict[str, Any]:
        """
        Compare performance metrics across multiple pipelines.
        
        Args:
            pipeline_ids: List of pipeline IDs to compare
            
        Returns:
            Dictionary with comparison results
        """
        if not self.collector:
            raise ValueError("MetricsCollector required for pipeline comparison")
        
        pipelines = []
        for pipeline_id in pipeline_ids:
            metrics = self.collector.get_pipeline_metrics(pipeline_id)
            if metrics:
                pipelines.append(metrics)
        
        if len(pipelines) < 2:
            return {'error': 'At least 2 pipelines required for comparison'}
        
        return {
            'pipeline_count': len(pipelines),
            'duration_comparison': self._compare_durations(pipelines),
            'throughput_comparison': self._compare_throughputs(pipelines),
            'bottleneck_comparison': self._compare_bottlenecks(pipelines),
            'step_comparison': self._compare_steps(pipelines),
            'recommendations': self._generate_comparison_recommendations(pipelines)
        }
    
    def _analyze_metric_trend(self, metric_name: str, time_period: timedelta) -> Optional[TrendAnalysis]:
        """Analyze trend for a specific metric over a time period."""
        if not self.collector:
            return None
        
        # Get metrics for current and previous periods
        current_metrics = self.collector.get_aggregated_metrics(time_period)
        previous_start = time_period * 2
        previous_metrics = self.collector.get_aggregated_metrics(previous_start)
        
        # Extract metric values
        current_value = self._extract_metric_value(current_metrics, metric_name)
        previous_value = self._extract_metric_value(previous_metrics, metric_name)
        
        if current_value is None or previous_value is None or previous_value == 0:
            return None
        
        # Calculate trend
        change_percentage = ((current_value - previous_value) / previous_value) * 100
        
        # Determine trend direction and strength
        if abs(change_percentage) < 5:
            trend_direction = 'stable'
            trend_strength = 0.0
        elif change_percentage > 0:
            if metric_name in ['avg_pipeline_throughput', 'total_records_processed']:
                trend_direction = 'improving'
            else:
                trend_direction = 'degrading'
            trend_strength = min(abs(change_percentage) / 100, 1.0)
        else:
            if metric_name in ['avg_pipeline_duration_ms', 'bottleneck_rate']:
                trend_direction = 'improving'
            else:
                trend_direction = 'degrading'
            trend_strength = min(abs(change_percentage) / 100, 1.0)
        
        # Calculate confidence based on data points
        data_points = current_metrics.get('summary', {}).get('total_pipelines', 0)
        confidence = min(data_points / 10, 1.0)  # Full confidence at 10+ data points
        
        return TrendAnalysis(
            metric_name=metric_name,
            time_period=str(time_period),
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            current_value=current_value,
            previous_value=previous_value,
            change_percentage=change_percentage,
            data_points=data_points,
            confidence=confidence
        )
    
    def _extract_metric_value(self, metrics: Dict[str, Any], metric_name: str) -> Optional[float]:
        """Extract a metric value from aggregated metrics."""
        if metric_name == 'avg_pipeline_duration_ms':
            return metrics.get('pipeline_metrics', {}).get('avg_duration_ms')
        elif metric_name == 'avg_pipeline_throughput':
            return metrics.get('pipeline_metrics', {}).get('avg_throughput')
        elif metric_name == 'total_records_processed':
            return metrics.get('summary', {}).get('total_records_processed')
        elif metric_name == 'bottleneck_rate':
            return metrics.get('bottlenecks', {}).get('bottleneck_rate')
        else:
            return None
    
    def _analyze_single_pipeline(self, metrics: PipelineMetrics) -> List[OptimizationRecommendation]:
        """Generate recommendations for a single pipeline."""
        recommendations = []
        
        # Analyze duration bottlenecks
        if metrics.total_duration_ms and metrics.total_duration_ms > 60000:  # > 1 minute
            bottleneck_analysis = self.analyze_bottlenecks(metrics)
            if bottleneck_analysis.slowest_steps:
                slowest = bottleneck_analysis.slowest_steps[0]
                recommendations.append(OptimizationRecommendation(
                    priority='high',
                    category='bottleneck',
                    title=f"Optimize slow step: {slowest['step_name']}",
                    description=f"Step '{slowest['step_name']}' takes {slowest['percentage_of_total']}% of total execution time. Consider optimizing this step.",
                    impact_estimate='high',
                    effort_estimate='medium',
                    affected_steps=[slowest['step_name']],
                    metrics_evidence={
                        'duration_ms': slowest['duration_ms'],
                        'percentage_of_total': slowest['percentage_of_total']
                    }
                ))
        
        # Analyze throughput issues
        if metrics.avg_records_per_second < 10:  # Less than 10 records/sec
            recommendations.append(OptimizationRecommendation(
                priority='medium',
                category='throughput',
                title="Low throughput detected",
                description=f"Pipeline throughput is {metrics.avg_records_per_second:.1f} records/sec. Consider batch processing or parallel execution.",
                impact_estimate='medium',
                effort_estimate='high',
                affected_steps=[],
                metrics_evidence={
                    'avg_records_per_second': metrics.avg_records_per_second
                }
            ))
        
        # Analyze failure rates
        total_processed = metrics.total_success_count + metrics.total_failure_count
        if total_processed > 0:
            failure_rate = metrics.total_failure_count / total_processed
            if failure_rate > 0.05:  # > 5% failure rate
                recommendations.append(OptimizationRecommendation(
                    priority='high',
                    category='reliability',
                    title="High failure rate detected",
                    description=f"Pipeline has {failure_rate*100:.1f}% failure rate. Investigate error handling and data quality.",
                    impact_estimate='high',
                    effort_estimate='medium',
                    affected_steps=[],
                    metrics_evidence={
                        'failure_rate': failure_rate,
                        'failure_count': metrics.total_failure_count,
                        'total_processed': total_processed
                    }
                ))
        
        return recommendations
    
    def _analyze_aggregated_metrics(self, metrics: Dict[str, Any]) -> List[OptimizationRecommendation]:
        """Generate recommendations from aggregated metrics."""
        recommendations = []
        
        # Check overall performance trends
        pipeline_metrics = metrics.get('pipeline_metrics', {})
        avg_duration = pipeline_metrics.get('avg_duration_ms', 0)
        
        if avg_duration > 300000:  # > 5 minutes average
            recommendations.append(OptimizationRecommendation(
                priority='medium',
                category='throughput',
                title="Long average pipeline duration",
                description=f"Average pipeline duration is {avg_duration/1000:.1f} seconds. Consider optimizing common bottlenecks.",
                impact_estimate='medium',
                effort_estimate='medium',
                affected_steps=[],
                metrics_evidence={'avg_duration_ms': avg_duration}
            ))
        
        # Check common bottlenecks
        bottlenecks = metrics.get('bottlenecks', {}).get('common_bottlenecks', [])
        for bottleneck in bottlenecks[:2]:  # Top 2 bottlenecks
            if bottleneck['percentage'] > 20:  # Affects > 20% of pipelines
                recommendations.append(OptimizationRecommendation(
                    priority='high',
                    category='bottleneck',
                    title=f"Common bottleneck: {bottleneck['step_name']}",
                    description=f"Step '{bottleneck['step_name']}' is a bottleneck in {bottleneck['percentage']:.1f}% of pipelines.",
                    impact_estimate='high',
                    effort_estimate='medium',
                    affected_steps=[bottleneck['step_name']],
                    metrics_evidence=bottleneck
                ))
        
        return recommendations
    
    def _analyze_execution_patterns(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze execution patterns from aggregated metrics."""
        step_metrics = metrics.get('step_metrics', {})
        step_types = step_metrics.get('step_types', {})
        
        return {
            'most_common_steps': sorted(step_types.items(), key=lambda x: x[1], reverse=True)[:5],
            'avg_steps_per_pipeline': step_metrics.get('total_steps', 0) / max(metrics.get('summary', {}).get('total_pipelines', 1), 1),
            'step_diversity': len(step_types)
        }
    
    def _analyze_throughput_patterns(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze throughput patterns."""
        pipeline_metrics = metrics.get('pipeline_metrics', {})
        
        return {
            'throughput_variability': {
                'min': pipeline_metrics.get('min_throughput', 0),
                'max': pipeline_metrics.get('max_throughput', 0),
                'avg': pipeline_metrics.get('avg_throughput', 0),
                'range': pipeline_metrics.get('max_throughput', 0) - pipeline_metrics.get('min_throughput', 0)
            }
        }
    
    def _analyze_failure_patterns(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze failure patterns."""
        bottlenecks = metrics.get('bottlenecks', {})
        
        return {
            'failure_frequency': bottlenecks.get('bottleneck_rate', 0),
            'common_failure_points': bottlenecks.get('common_bottlenecks', [])[:3]
        }
    
    def _analyze_temporal_patterns(self, time_window: timedelta) -> Dict[str, Any]:
        """Analyze temporal patterns (placeholder for time-based analysis)."""
        return {
            'time_window_hours': time_window.total_seconds() / 3600,
            'analysis_note': 'Temporal pattern analysis requires historical data partitioning'
        }
    
    def _compare_durations(self, pipelines: List[PipelineMetrics]) -> Dict[str, Any]:
        """Compare pipeline durations."""
        durations = [p.total_duration_ms for p in pipelines if p.total_duration_ms]
        
        if not durations:
            return {'error': 'No duration data available'}
        
        return {
            'min_duration_ms': min(durations),
            'max_duration_ms': max(durations),
            'avg_duration_ms': statistics.mean(durations),
            'median_duration_ms': statistics.median(durations),
            'std_dev_ms': statistics.stdev(durations) if len(durations) > 1 else 0
        }
    
    def _compare_throughputs(self, pipelines: List[PipelineMetrics]) -> Dict[str, Any]:
        """Compare pipeline throughputs."""
        throughputs = [p.avg_records_per_second for p in pipelines if p.avg_records_per_second > 0]
        
        if not throughputs:
            return {'error': 'No throughput data available'}
        
        return {
            'min_throughput': min(throughputs),
            'max_throughput': max(throughputs),
            'avg_throughput': statistics.mean(throughputs),
            'median_throughput': statistics.median(throughputs),
            'std_dev_throughput': statistics.stdev(throughputs) if len(throughputs) > 1 else 0
        }
    
    def _compare_bottlenecks(self, pipelines: List[PipelineMetrics]) -> Dict[str, Any]:
        """Compare bottlenecks across pipelines."""
        all_bottlenecks = []
        for pipeline in pipelines:
            all_bottlenecks.extend(pipeline.bottlenecks)
        
        bottleneck_counts = defaultdict(int)
        for bottleneck in all_bottlenecks:
            step_name = bottleneck.split(':')[0] if ':' in bottleneck else bottleneck
            bottleneck_counts[step_name] += 1
        
        return {
            'total_bottlenecks': len(all_bottlenecks),
            'unique_bottleneck_types': len(bottleneck_counts),
            'most_common_bottlenecks': sorted(bottleneck_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        }
    
    def _compare_steps(self, pipelines: List[PipelineMetrics]) -> Dict[str, Any]:
        """Compare step metrics across pipelines."""
        all_steps = []
        for pipeline in pipelines:
            all_steps.extend(pipeline.step_metrics)
        
        step_durations = defaultdict(list)
        step_throughputs = defaultdict(list)
        
        for step in all_steps:
            if step.duration_ms:
                step_durations[step.step_name].append(step.duration_ms)
            if step.records_per_second > 0:
                step_throughputs[step.step_name].append(step.records_per_second)
        
        return {
            'step_performance': {
                step_name: {
                    'avg_duration_ms': statistics.mean(durations),
                    'count': len(durations)
                }
                for step_name, durations in step_durations.items()
            },
            'step_throughput': {
                step_name: {
                    'avg_throughput': statistics.mean(throughputs),
                    'count': len(throughputs)
                }
                for step_name, throughputs in step_throughputs.items()
            }
        }
    
    def _generate_comparison_recommendations(self, pipelines: List[PipelineMetrics]) -> List[OptimizationRecommendation]:
        """Generate recommendations from pipeline comparison."""
        recommendations = []
        
        # Find consistently slow pipelines
        durations = [(p.pipeline_name, p.total_duration_ms) for p in pipelines if p.total_duration_ms]
        if len(durations) > 1:
            avg_duration = statistics.mean([d[1] for d in durations])
            slow_pipelines = [name for name, duration in durations if duration > avg_duration * 1.5]
            
            if slow_pipelines:
                recommendations.append(OptimizationRecommendation(
                    priority='medium',
                    category='bottleneck',
                    title="Consistently slow pipelines identified",
                    description=f"Pipelines {', '.join(slow_pipelines)} are significantly slower than average.",
                    impact_estimate='medium',
                    effort_estimate='medium',
                    affected_steps=[],
                    metrics_evidence={'slow_pipelines': slow_pipelines}
                ))
        
        return recommendations