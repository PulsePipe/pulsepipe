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

# src/pulsepipe/pipelines/performance/__init__.py

"""
Performance tracking module for PulsePipe.

Provides timing decorators, metrics collection, and bottleneck identification.
"""

from .tracker import (
    PerformanceTracker,
    StepMetrics,
    PipelineMetrics,
    BottleneckAnalysis
)
from .decorators import (
    track_performance,
    track_async_performance,
    track_stage_performance
)
from .collector import MetricsCollector
from .analyzer import PerformanceAnalyzer
from .system_metrics import (
    SystemMetricsCollector,
    SystemSnapshot,
    CPUMetrics,
    MemoryMetrics,
    StorageMetrics,
    OSMetrics,
    GPUMetrics,
    get_global_system_collector
)

__all__ = [
    'PerformanceTracker',
    'StepMetrics', 
    'PipelineMetrics',
    'BottleneckAnalysis',
    'track_performance',
    'track_async_performance', 
    'track_stage_performance',
    'MetricsCollector',
    'PerformanceAnalyzer',
    'SystemMetricsCollector',
    'SystemSnapshot',
    'CPUMetrics',
    'MemoryMetrics',
    'StorageMetrics',
    'OSMetrics',
    'GPUMetrics',
    'get_global_system_collector'
]