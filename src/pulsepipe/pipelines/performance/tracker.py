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

# src/pulsepipe/pipelines/performance/tracker.py

"""
Core performance tracking classes for PulsePipe.

Provides step-level timing, throughput metrics, and bottleneck identification.
"""

import time
import threading
import statistics
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from collections import defaultdict, deque

from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


@dataclass
class StepMetrics:
    """Metrics for a single pipeline step execution."""
    
    step_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    records_processed: int = 0
    bytes_processed: int = 0
    success_count: int = 0
    failure_count: int = 0
    records_per_second: float = 0.0
    bytes_per_second: float = 0.0
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def finish(self, records_processed: int = 0, bytes_processed: int = 0, 
               success_count: int = 0, failure_count: int = 0) -> None:
        """Mark the step as completed and calculate final metrics."""
        self.end_time = datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000
        
        self.records_processed = records_processed
        self.bytes_processed = bytes_processed
        self.success_count = success_count
        self.failure_count = failure_count
        
        # Calculate throughput rates
        if self.duration_ms and self.duration_ms > 0:
            duration_seconds = self.duration_ms / 1000
            self.records_per_second = self.records_processed / duration_seconds
            self.bytes_per_second = self.bytes_processed / duration_seconds
        else:
            # Handle zero or very small duration
            self.records_per_second = 0.0
            self.bytes_per_second = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'step_name': self.step_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_ms': self.duration_ms,
            'records_processed': self.records_processed,
            'bytes_processed': self.bytes_processed,
            'success_count': self.success_count,
            'failure_count': self.failure_count,
            'records_per_second': round(self.records_per_second, 2),
            'bytes_per_second': round(self.bytes_per_second, 2),
            'memory_usage_mb': self.memory_usage_mb,
            'cpu_usage_percent': self.cpu_usage_percent,
            'metadata': self.metadata
        }


@dataclass
class PipelineMetrics:
    """Aggregated metrics for entire pipeline execution."""
    
    pipeline_id: str
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration_ms: Optional[float] = None
    step_metrics: List[StepMetrics] = field(default_factory=list)
    total_records_processed: int = 0
    total_bytes_processed: int = 0
    total_success_count: int = 0
    total_failure_count: int = 0
    avg_records_per_second: float = 0.0
    avg_bytes_per_second: float = 0.0
    bottlenecks: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_step_metrics(self, step_metrics: StepMetrics) -> None:
        """Add metrics for a completed step."""
        self.step_metrics.append(step_metrics)
        
        # Update totals
        self.total_records_processed += step_metrics.records_processed
        self.total_bytes_processed += step_metrics.bytes_processed
        self.total_success_count += step_metrics.success_count
        self.total_failure_count += step_metrics.failure_count
    
    def finish(self) -> None:
        """Mark the pipeline as completed and calculate final metrics."""
        self.end_time = datetime.now()
        self.total_duration_ms = (self.end_time - self.start_time).total_seconds() * 1000
        
        # Calculate average throughput
        if self.total_duration_ms > 0:
            duration_seconds = self.total_duration_ms / 1000
            self.avg_records_per_second = self.total_records_processed / duration_seconds
            self.avg_bytes_per_second = self.total_bytes_processed / duration_seconds
        
        # Identify bottlenecks
        self.bottlenecks = self._identify_bottlenecks()
    
    def _identify_bottlenecks(self) -> List[str]:
        """Identify performance bottlenecks based on step metrics."""
        if not self.step_metrics:
            return []
        
        bottlenecks = []
        
        # Find steps that took more than 30% of total time
        if self.total_duration_ms:
            for step in self.step_metrics:
                if step.duration_ms and (step.duration_ms / self.total_duration_ms) > 0.3:
                    bottlenecks.append(f"{step.step_name}: {step.duration_ms:.1f}ms "
                                     f"({step.duration_ms/self.total_duration_ms*100:.1f}% of total)")
        
        # Find steps with low throughput (if any processing occurred)
        step_throughputs = [s.records_per_second for s in self.step_metrics 
                          if s.records_per_second > 0]
        if step_throughputs:
            median_throughput = statistics.median(step_throughputs)
            for step in self.step_metrics:
                if (step.records_per_second > 0 and 
                    step.records_per_second < median_throughput * 0.5):
                    bottlenecks.append(f"{step.step_name}: Low throughput "
                                     f"({step.records_per_second:.1f} records/sec)")
        
        # Find steps with high failure rates
        for step in self.step_metrics:
            total_processed = step.success_count + step.failure_count
            if total_processed > 0:
                failure_rate = step.failure_count / total_processed
                if failure_rate > 0.1:  # More than 10% failure rate
                    bottlenecks.append(f"{step.step_name}: High failure rate "
                                     f"({failure_rate*100:.1f}%)")
        
        return bottlenecks
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'pipeline_id': self.pipeline_id,
            'pipeline_name': self.pipeline_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_duration_ms': self.total_duration_ms,
            'step_metrics': [step.to_dict() for step in self.step_metrics],
            'total_records_processed': self.total_records_processed,
            'total_bytes_processed': self.total_bytes_processed,
            'total_success_count': self.total_success_count,
            'total_failure_count': self.total_failure_count,
            'avg_records_per_second': round(self.avg_records_per_second, 2),
            'avg_bytes_per_second': round(self.avg_bytes_per_second, 2),
            'bottlenecks': self.bottlenecks,
            'metadata': self.metadata
        }


@dataclass
class BottleneckAnalysis:
    """Analysis of performance bottlenecks in pipeline execution."""
    
    pipeline_id: str
    total_duration_ms: float
    slowest_steps: List[Dict[str, Any]] = field(default_factory=list)
    low_throughput_steps: List[Dict[str, Any]] = field(default_factory=list)
    high_failure_steps: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    
    @classmethod
    def from_pipeline_metrics(cls, metrics: PipelineMetrics) -> 'BottleneckAnalysis':
        """Create bottleneck analysis from pipeline metrics."""
        analysis = cls(
            pipeline_id=metrics.pipeline_id,
            total_duration_ms=metrics.total_duration_ms or 0
        )
        
        if not metrics.step_metrics:
            return analysis
        
        # Analyze slowest steps
        sorted_by_duration = sorted(metrics.step_metrics, 
                                  key=lambda x: x.duration_ms or 0, reverse=True)
        for step in sorted_by_duration[:3]:  # Top 3 slowest
            if step.duration_ms:
                pct_of_total = (step.duration_ms / analysis.total_duration_ms) * 100
                analysis.slowest_steps.append({
                    'step_name': step.step_name,
                    'duration_ms': step.duration_ms,
                    'percentage_of_total': round(pct_of_total, 1)
                })
        
        # Analyze low throughput steps
        step_throughputs = [(s.step_name, s.records_per_second) 
                          for s in metrics.step_metrics if s.records_per_second > 0]
        if step_throughputs:
            median_throughput = statistics.median([t[1] for t in step_throughputs])
            for step_name, throughput in step_throughputs:
                if throughput < median_throughput * 0.5:
                    analysis.low_throughput_steps.append({
                        'step_name': step_name,
                        'records_per_second': round(throughput, 2),
                        'vs_median': round((throughput / median_throughput) * 100, 1)
                    })
        
        # Analyze high failure steps
        for step in metrics.step_metrics:
            total_processed = step.success_count + step.failure_count
            if total_processed > 0:
                failure_rate = step.failure_count / total_processed
                if failure_rate > 0.05:  # More than 5% failure rate
                    analysis.high_failure_steps.append({
                        'step_name': step.step_name,
                        'failure_rate': round(failure_rate * 100, 1),
                        'failure_count': step.failure_count,
                        'total_processed': total_processed
                    })
        
        # Generate recommendations
        analysis.recommendations = analysis._generate_recommendations()
        
        return analysis
    
    def _generate_recommendations(self) -> List[str]:
        """Generate performance improvement recommendations."""
        recommendations = []
        
        # Recommendations for slow steps
        if self.slowest_steps:
            slowest = self.slowest_steps[0]
            if slowest['percentage_of_total'] > 50:
                recommendations.append(
                    f"Focus optimization efforts on '{slowest['step_name']}' step - "
                    f"it accounts for {slowest['percentage_of_total']}% of total execution time"
                )
            elif slowest['percentage_of_total'] > 30:
                recommendations.append(
                    f"Consider optimizing '{slowest['step_name']}' step - "
                    f"significant performance impact ({slowest['percentage_of_total']}% of total time)"
                )
        
        # Recommendations for low throughput
        if self.low_throughput_steps:
            for step in self.low_throughput_steps:
                recommendations.append(
                    f"Investigate throughput bottleneck in '{step['step_name']}' step - "
                    f"processing {step['records_per_second']} records/sec "
                    f"({step['vs_median']}% of median)"
                )
        
        # Recommendations for high failure rates
        if self.high_failure_steps:
            for step in self.high_failure_steps:
                recommendations.append(
                    f"Address high failure rate in '{step['step_name']}' step - "
                    f"{step['failure_rate']}% failure rate "
                    f"({step['failure_count']}/{step['total_processed']} failures)"
                )
        
        # General recommendations
        if len(self.slowest_steps) > 1:
            recommendations.append(
                "Consider implementing parallel processing for independent pipeline steps"
            )
        
        if not recommendations:
            recommendations.append("Performance appears optimal - no major bottlenecks identified")
        
        return recommendations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'pipeline_id': self.pipeline_id,
            'total_duration_ms': self.total_duration_ms,
            'slowest_steps': self.slowest_steps,
            'low_throughput_steps': self.low_throughput_steps,
            'high_failure_steps': self.high_failure_steps,
            'recommendations': self.recommendations
        }


class PerformanceTracker:
    """
    Thread-safe performance tracking for pipeline execution.
    
    Tracks step-level timing, throughput metrics, and resource usage.
    """
    
    def __init__(self, pipeline_id: str, pipeline_name: str):
        self.pipeline_metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            start_time=datetime.now()
        )
        self.current_step: Optional[StepMetrics] = None
        self._lock = threading.Lock()
        self._step_history: deque = deque(maxlen=100)  # Keep last 100 steps
    
    def start_step(self, step_name: str, metadata: Optional[Dict[str, Any]] = None) -> StepMetrics:
        """Start tracking a new pipeline step."""
        with self._lock:
            # Finish previous step if it exists
            if self.current_step and not self.current_step.end_time:
                logger.warning(f"Step '{self.current_step.step_name}' was not properly finished")
                self.current_step.finish()
                self.pipeline_metrics.add_step_metrics(self.current_step)
            
            # Start new step
            self.current_step = StepMetrics(
                step_name=step_name,
                start_time=datetime.now(),
                metadata=metadata or {}
            )
            
            logger.debug(f"Started tracking step: {step_name}")
            return self.current_step
    
    def finish_step(self, records_processed: int = 0, bytes_processed: int = 0,
                   success_count: int = 0, failure_count: int = 0,
                   metadata: Optional[Dict[str, Any]] = None) -> Optional[StepMetrics]:
        """Finish tracking the current step."""
        with self._lock:
            if not self.current_step:
                logger.warning("No active step to finish")
                return None
            
            # Update metadata if provided
            if metadata:
                self.current_step.metadata.update(metadata)
            
            # Finish the step
            self.current_step.finish(
                records_processed=records_processed,
                bytes_processed=bytes_processed,
                success_count=success_count,
                failure_count=failure_count
            )
            
            # Add to pipeline metrics
            self.pipeline_metrics.add_step_metrics(self.current_step)
            
            # Add to history
            self._step_history.append(self.current_step)
            
            logger.debug(f"Finished tracking step: {self.current_step.step_name} "
                        f"({self.current_step.duration_ms:.1f}ms)")
            
            finished_step = self.current_step
            self.current_step = None
            return finished_step
    
    def update_step_progress(self, records_processed: int = 0, bytes_processed: int = 0,
                           success_count: int = 0, failure_count: int = 0) -> None:
        """Update progress counters for the current step."""
        if self.current_step:
            self.current_step.records_processed += records_processed
            self.current_step.bytes_processed += bytes_processed
            self.current_step.success_count += success_count
            self.current_step.failure_count += failure_count
    
    def get_current_step(self) -> Optional[StepMetrics]:
        """Get the currently active step metrics."""
        return self.current_step
    
    def get_step_history(self) -> List[StepMetrics]:
        """Get history of completed steps."""
        return list(self._step_history)
    
    def finish_pipeline(self, metadata: Optional[Dict[str, Any]] = None) -> PipelineMetrics:
        """Finish tracking the entire pipeline."""
        with self._lock:
            # Finish any remaining step
            if self.current_step and not self.current_step.end_time:
                # Finish the step directly without calling finish_step to avoid recursion
                self.current_step.finish()
                self.pipeline_metrics.add_step_metrics(self.current_step)
                self._step_history.append(self.current_step)
                logger.debug(f"Auto-finished step: {self.current_step.step_name}")
                self.current_step = None
            
            # Update metadata if provided
            if metadata:
                self.pipeline_metrics.metadata.update(metadata)
            
            # Finish pipeline
            self.pipeline_metrics.finish()
            
            logger.info(f"Finished tracking pipeline: {self.pipeline_metrics.pipeline_name} "
                       f"({self.pipeline_metrics.total_duration_ms:.1f}ms, "
                       f"{len(self.pipeline_metrics.step_metrics)} steps)")
            
            return self.pipeline_metrics
    
    def get_bottleneck_analysis(self) -> BottleneckAnalysis:
        """Get bottleneck analysis for the current pipeline state."""
        return BottleneckAnalysis.from_pipeline_metrics(self.pipeline_metrics)
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get a summary of performance metrics."""
        metrics = self.pipeline_metrics
        
        summary = {
            'pipeline_id': metrics.pipeline_id,
            'pipeline_name': metrics.pipeline_name,
            'status': 'completed' if metrics.end_time else 'running',
            'total_duration_ms': metrics.total_duration_ms,
            'steps_completed': len(metrics.step_metrics),
            'total_records_processed': metrics.total_records_processed,
            'avg_records_per_second': metrics.avg_records_per_second,
            'bottleneck_count': len(metrics.bottlenecks)
        }
        
        if self.current_step:
            current_duration = (datetime.now() - self.current_step.start_time).total_seconds() * 1000
            summary['current_step'] = {
                'name': self.current_step.step_name,
                'duration_ms': current_duration,
                'records_processed': self.current_step.records_processed
            }
        
        return summary