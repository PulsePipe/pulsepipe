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

# src/pulsepipe/pipelines/performance/collector.py

"""
Metrics collection system for PulsePipe performance tracking.

Provides centralized collection and aggregation of performance metrics.
"""

import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from collections import defaultdict, deque
from pathlib import Path

from .tracker import PerformanceTracker, PipelineMetrics, StepMetrics
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


class MetricsCollector:
    """
    Centralized collection and storage of performance metrics.
    
    Handles multiple concurrent pipeline executions and provides
    aggregation capabilities.
    """
    
    def __init__(self, max_pipelines: int = 1000, max_steps_per_pipeline: int = 100):
        self.max_pipelines = max_pipelines
        self.max_steps_per_pipeline = max_steps_per_pipeline
        
        # Thread-safe storage
        self._lock = threading.RLock()
        self._pipeline_metrics: Dict[str, PipelineMetrics] = {}
        self._active_trackers: Dict[str, PerformanceTracker] = {}
        self._completed_pipelines: deque = deque(maxlen=max_pipelines)
        
        # Aggregated metrics
        self._step_aggregates: Dict[str, List[float]] = defaultdict(list)
        self._pipeline_aggregates: Dict[str, List[float]] = defaultdict(list)
    
    def create_tracker(self, pipeline_id: str, pipeline_name: str) -> PerformanceTracker:
        """Create a new performance tracker for a pipeline."""
        with self._lock:
            if pipeline_id in self._active_trackers:
                logger.warning(f"Tracker for pipeline {pipeline_id} already exists")
                return self._active_trackers[pipeline_id]
            
            tracker = PerformanceTracker(pipeline_id, pipeline_name)
            self._active_trackers[pipeline_id] = tracker
            
            logger.debug(f"Created tracker for pipeline: {pipeline_id}")
            return tracker
    
    def get_tracker(self, pipeline_id: str) -> Optional[PerformanceTracker]:
        """Get an existing tracker by pipeline ID."""
        with self._lock:
            return self._active_trackers.get(pipeline_id)
    
    def finish_pipeline(self, pipeline_id: str, 
                       metadata: Optional[Dict[str, Any]] = None) -> Optional[PipelineMetrics]:
        """Finish tracking a pipeline and store its metrics."""
        with self._lock:
            tracker = self._active_trackers.get(pipeline_id)
            if not tracker:
                logger.warning(f"No tracker found for pipeline: {pipeline_id}")
                return None
            
            # Finish the tracker
            metrics = tracker.finish_pipeline(metadata)
            
            # Store completed metrics
            self._pipeline_metrics[pipeline_id] = metrics
            self._completed_pipelines.append(metrics)
            
            # Update aggregates
            self._update_aggregates(metrics)
            
            # Remove from active trackers
            del self._active_trackers[pipeline_id]
            
            logger.info(f"Collected metrics for pipeline: {pipeline_id} "
                       f"({metrics.total_duration_ms:.1f}ms)")
            
            return metrics
    
    def get_pipeline_metrics(self, pipeline_id: str) -> Optional[PipelineMetrics]:
        """Get metrics for a specific pipeline."""
        with self._lock:
            return self._pipeline_metrics.get(pipeline_id)
    
    def get_recent_pipelines(self, limit: int = 10) -> List[PipelineMetrics]:
        """Get most recently completed pipelines."""
        with self._lock:
            return list(self._completed_pipelines)[-limit:]
    
    def get_active_pipelines(self) -> List[str]:
        """Get list of currently active pipeline IDs."""
        with self._lock:
            return list(self._active_trackers.keys())
    
    def get_aggregated_metrics(self, time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """
        Get aggregated performance metrics across all pipelines.
        
        Args:
            time_window: Only include pipelines within this time window
        """
        with self._lock:
            # Filter pipelines by time window if specified
            pipelines = list(self._completed_pipelines)
            if time_window:
                cutoff_time = datetime.now() - time_window
                pipelines = [p for p in pipelines 
                           if p.end_time and p.end_time >= cutoff_time]
            
            if not pipelines:
                return self._empty_aggregation()
            
            # Calculate pipeline-level aggregates
            total_pipelines = len(pipelines)
            total_duration = sum(p.total_duration_ms or 0 for p in pipelines)
            total_records = sum(p.total_records_processed for p in pipelines)
            total_bytes = sum(p.total_bytes_processed for p in pipelines)
            
            durations = [p.total_duration_ms for p in pipelines if p.total_duration_ms]
            throughputs = [p.avg_records_per_second for p in pipelines 
                         if p.avg_records_per_second > 0]
            
            # Calculate step-level aggregates
            all_steps = []
            for pipeline in pipelines:
                all_steps.extend(pipeline.step_metrics)
            
            step_durations = [s.duration_ms for s in all_steps if s.duration_ms]
            step_throughputs = [s.records_per_second for s in all_steps 
                              if s.records_per_second > 0]
            
            # Count failures and bottlenecks
            total_failures = sum(len(p.bottlenecks) for p in pipelines)
            common_bottlenecks = self._identify_common_bottlenecks(pipelines)
            
            return {
                'summary': {
                    'total_pipelines': total_pipelines,
                    'total_duration_ms': total_duration,
                    'total_records_processed': total_records,
                    'total_bytes_processed': total_bytes,
                    'avg_pipeline_duration_ms': total_duration / total_pipelines if total_pipelines > 0 else 0,
                    'avg_pipeline_throughput': sum(throughputs) / len(throughputs) if throughputs else 0
                },
                'pipeline_metrics': {
                    'min_duration_ms': min(durations) if durations else 0,
                    'max_duration_ms': max(durations) if durations else 0,
                    'avg_duration_ms': sum(durations) / len(durations) if durations else 0,
                    'min_throughput': min(throughputs) if throughputs else 0,
                    'max_throughput': max(throughputs) if throughputs else 0,
                    'avg_throughput': sum(throughputs) / len(throughputs) if throughputs else 0
                },
                'step_metrics': {
                    'total_steps': len(all_steps),
                    'avg_step_duration_ms': sum(step_durations) / len(step_durations) if step_durations else 0,
                    'avg_step_throughput': sum(step_throughputs) / len(step_throughputs) if step_throughputs else 0,
                    'step_types': self._count_step_types(all_steps)
                },
                'bottlenecks': {
                    'total_bottlenecks': total_failures,
                    'common_bottlenecks': common_bottlenecks,
                    'bottleneck_rate': total_failures / total_pipelines if total_pipelines > 0 else 0
                },
                'time_window': {
                    'start_time': min(p.start_time for p in pipelines).isoformat(),
                    'end_time': max(p.end_time for p in pipelines if p.end_time).isoformat(),
                    'duration_hours': time_window.total_seconds() / 3600 if time_window else None
                }
            }
    
    def export_metrics(self, file_path: Union[str, Path], 
                      format: str = 'json', 
                      time_window: Optional[timedelta] = None) -> None:
        """
        Export metrics to file.
        
        Args:
            file_path: Path to export file
            format: Export format ('json', 'csv')
            time_window: Time window for filtering metrics
        """
        file_path = Path(file_path)
        
        if format.lower() == 'json':
            self._export_json(file_path, time_window)
        elif format.lower() == 'csv':
            self._export_csv(file_path, time_window)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def clear_old_metrics(self, older_than: timedelta) -> int:
        """
        Clear metrics older than specified duration.
        
        Returns:
            Number of metrics cleared
        """
        cutoff_time = datetime.now() - older_than
        cleared_count = 0
        
        with self._lock:
            # Clear from completed pipelines deque
            initial_count = len(self._completed_pipelines)
            self._completed_pipelines = deque(
                (p for p in self._completed_pipelines 
                 if p.end_time and p.end_time >= cutoff_time),
                maxlen=self.max_pipelines
            )
            cleared_from_deque = initial_count - len(self._completed_pipelines)
            
            # Clear from pipeline metrics dict (only count those not already counted from deque)
            to_remove = []
            for pipeline_id, metrics in self._pipeline_metrics.items():
                if metrics.end_time and metrics.end_time < cutoff_time:
                    to_remove.append(pipeline_id)
            
            for pipeline_id in to_remove:
                del self._pipeline_metrics[pipeline_id]
            
            # Total cleared is the max of deque cleared or dict cleared since they contain the same items
            cleared_count = max(cleared_from_deque, len(to_remove))
            
            # Rebuild aggregates
            self._rebuild_aggregates()
        
        logger.info(f"Cleared {cleared_count} old metrics (older than {older_than})")
        return cleared_count
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get collector statistics."""
        with self._lock:
            return {
                'active_trackers': len(self._active_trackers),
                'completed_pipelines': len(self._completed_pipelines),
                'stored_metrics': len(self._pipeline_metrics),
                'step_aggregates': len(self._step_aggregates),
                'pipeline_aggregates': len(self._pipeline_aggregates),
                'memory_usage': {
                    'max_pipelines': self.max_pipelines,
                    'max_steps_per_pipeline': self.max_steps_per_pipeline
                }
            }
    
    def _update_aggregates(self, metrics: PipelineMetrics) -> None:
        """Update aggregated metrics with new pipeline data."""
        # Pipeline-level aggregates
        if metrics.total_duration_ms:
            self._pipeline_aggregates['duration_ms'].append(metrics.total_duration_ms)
        if metrics.avg_records_per_second > 0:
            self._pipeline_aggregates['throughput'].append(metrics.avg_records_per_second)
        
        # Step-level aggregates
        for step in metrics.step_metrics:
            step_type = step.step_name
            if step.duration_ms:
                self._step_aggregates[f"{step_type}_duration_ms"].append(step.duration_ms)
            if step.records_per_second > 0:
                self._step_aggregates[f"{step_type}_throughput"].append(step.records_per_second)
    
    def _rebuild_aggregates(self) -> None:
        """Rebuild aggregated metrics from current data."""
        self._step_aggregates.clear()
        self._pipeline_aggregates.clear()
        
        for metrics in self._completed_pipelines:
            self._update_aggregates(metrics)
    
    def _identify_common_bottlenecks(self, pipelines: List[PipelineMetrics]) -> List[Dict[str, Any]]:
        """Identify most common bottlenecks across pipelines."""
        bottleneck_counts = defaultdict(int)
        
        for pipeline in pipelines:
            for bottleneck in pipeline.bottlenecks:
                # Extract step name from bottleneck string
                step_name = bottleneck.split(':')[0] if ':' in bottleneck else bottleneck
                bottleneck_counts[step_name] += 1
        
        # Sort by frequency and return top 5
        sorted_bottlenecks = sorted(bottleneck_counts.items(), 
                                  key=lambda x: x[1], reverse=True)[:5]
        
        return [
            {
                'step_name': step_name,
                'occurrence_count': count,
                'percentage': (count / len(pipelines)) * 100
            }
            for step_name, count in sorted_bottlenecks
        ]
    
    def _count_step_types(self, steps: List[StepMetrics]) -> Dict[str, int]:
        """Count occurrences of each step type."""
        step_counts = defaultdict(int)
        for step in steps:
            step_counts[step.step_name] += 1
        return dict(step_counts)
    
    def _empty_aggregation(self) -> Dict[str, Any]:
        """Return empty aggregation structure."""
        return {
            'summary': {
                'total_pipelines': 0,
                'total_duration_ms': 0,
                'total_records_processed': 0,
                'total_bytes_processed': 0,
                'avg_pipeline_duration_ms': 0,
                'avg_pipeline_throughput': 0
            },
            'pipeline_metrics': {
                'min_duration_ms': 0,
                'max_duration_ms': 0,
                'avg_duration_ms': 0,
                'min_throughput': 0,
                'max_throughput': 0,
                'avg_throughput': 0
            },
            'step_metrics': {
                'total_steps': 0,
                'avg_step_duration_ms': 0,
                'avg_step_throughput': 0,
                'step_types': {}
            },
            'bottlenecks': {
                'total_bottlenecks': 0,
                'common_bottlenecks': [],
                'bottleneck_rate': 0
            }
        }
    
    def _export_json(self, file_path: Path, time_window: Optional[timedelta]) -> None:
        """Export metrics to JSON format."""
        aggregated = self.get_aggregated_metrics(time_window)
        
        # Add individual pipeline data
        pipelines = list(self._completed_pipelines)
        if time_window:
            cutoff_time = datetime.now() - time_window
            pipelines = [p for p in pipelines 
                       if p.end_time and p.end_time >= cutoff_time]
        
        export_data = {
            'export_metadata': {
                'generated_at': datetime.now().isoformat(),
                'pipeline_count': len(pipelines),
                'time_window_hours': time_window.total_seconds() / 3600 if time_window else None
            },
            'aggregated_metrics': aggregated,
            'pipelines': [p.to_dict() for p in pipelines]
        }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.info(f"Exported {len(pipelines)} pipeline metrics to {file_path}")
    
    def _export_csv(self, file_path: Path, time_window: Optional[timedelta]) -> None:
        """Export metrics to CSV format."""
        import csv
        
        pipelines = list(self._completed_pipelines)
        if time_window:
            cutoff_time = datetime.now() - time_window
            pipelines = [p for p in pipelines 
                       if p.end_time and p.end_time >= cutoff_time]
        
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'pipeline_id', 'pipeline_name', 'start_time', 'end_time',
                'total_duration_ms', 'total_records_processed', 'total_bytes_processed',
                'avg_records_per_second', 'avg_bytes_per_second', 'step_count',
                'bottleneck_count', 'success_count', 'failure_count'
            ])
            
            # Write pipeline data
            for pipeline in pipelines:
                writer.writerow([
                    pipeline.pipeline_id,
                    pipeline.pipeline_name,
                    pipeline.start_time.isoformat(),
                    pipeline.end_time.isoformat() if pipeline.end_time else '',
                    pipeline.total_duration_ms or 0,
                    pipeline.total_records_processed,
                    pipeline.total_bytes_processed,
                    round(pipeline.avg_records_per_second, 2),
                    round(pipeline.avg_bytes_per_second, 2),
                    len(pipeline.step_metrics),
                    len(pipeline.bottlenecks),
                    pipeline.total_success_count,
                    pipeline.total_failure_count
                ])
        
        logger.info(f"Exported {len(pipelines)} pipeline metrics to {file_path}")


# Global metrics collector instance
_global_collector: Optional[MetricsCollector] = None
_collector_lock = threading.Lock()


def get_global_collector() -> MetricsCollector:
    """Get or create the global metrics collector instance."""
    global _global_collector
    
    with _collector_lock:
        if _global_collector is None:
            _global_collector = MetricsCollector()
        return _global_collector


def reset_global_collector() -> None:
    """Reset the global metrics collector (mainly for testing)."""
    global _global_collector
    
    with _collector_lock:
        _global_collector = None