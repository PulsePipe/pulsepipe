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

# src/pulsepipe/pipelines/performance/system_metrics.py

"""
Environmental system metrics collection for PulsePipe.

Provides CPU, RAM, storage, OS, and GPU detection and monitoring capabilities.
"""

import asyncio
import os
import platform
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import psutil

from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


@dataclass
class CPUMetrics:
    """CPU utilization and performance metrics."""
    
    usage_percent: float
    usage_per_core: List[float] = field(default_factory=list)
    logical_cores: int = 0
    physical_cores: int = 0
    frequency_current: float = 0.0
    frequency_max: float = 0.0
    frequency_min: float = 0.0
    load_average_1min: Optional[float] = None
    load_average_5min: Optional[float] = None
    load_average_15min: Optional[float] = None
    context_switches: int = 0
    interrupts: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'usage_percent': round(self.usage_percent, 2),
            'usage_per_core': [round(u, 2) for u in self.usage_per_core],
            'logical_cores': self.logical_cores,
            'physical_cores': self.physical_cores,
            'frequency_current_mhz': round(self.frequency_current, 2),
            'frequency_max_mhz': round(self.frequency_max, 2),
            'frequency_min_mhz': round(self.frequency_min, 2),
            'load_average_1min': self.load_average_1min,
            'load_average_5min': self.load_average_5min,
            'load_average_15min': self.load_average_15min,
            'context_switches': self.context_switches,
            'interrupts': self.interrupts,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class MemoryMetrics:
    """Memory utilization metrics."""
    
    total_bytes: int
    available_bytes: int
    used_bytes: int
    free_bytes: int
    usage_percent: float
    swap_total_bytes: int = 0
    swap_used_bytes: int = 0
    swap_free_bytes: int = 0
    swap_usage_percent: float = 0.0
    cached_bytes: int = 0
    buffers_bytes: int = 0
    shared_bytes: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'total_mb': round(self.total_bytes / 1024 / 1024, 2),
            'available_mb': round(self.available_bytes / 1024 / 1024, 2),
            'used_mb': round(self.used_bytes / 1024 / 1024, 2),
            'free_mb': round(self.free_bytes / 1024 / 1024, 2),
            'usage_percent': round(self.usage_percent, 2),
            'swap_total_mb': round(self.swap_total_bytes / 1024 / 1024, 2),
            'swap_used_mb': round(self.swap_used_bytes / 1024 / 1024, 2),
            'swap_free_mb': round(self.swap_free_bytes / 1024 / 1024, 2),
            'swap_usage_percent': round(self.swap_usage_percent, 2),
            'cached_mb': round(self.cached_bytes / 1024 / 1024, 2),
            'buffers_mb': round(self.buffers_bytes / 1024 / 1024, 2),
            'shared_mb': round(self.shared_bytes / 1024 / 1024, 2),
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class StorageMetrics:
    """Storage utilization metrics."""
    
    devices: List[Dict[str, Any]] = field(default_factory=list)
    total_bytes: int = 0
    used_bytes: int = 0
    free_bytes: int = 0
    usage_percent: float = 0.0
    io_read_bytes: int = 0
    io_write_bytes: int = 0
    io_read_count: int = 0
    io_write_count: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'devices': self.devices,
            'total_gb': round(self.total_bytes / 1024 / 1024 / 1024, 2),
            'used_gb': round(self.used_bytes / 1024 / 1024 / 1024, 2),
            'free_gb': round(self.free_bytes / 1024 / 1024 / 1024, 2),
            'usage_percent': round(self.usage_percent, 2),
            'io_read_mb': round(self.io_read_bytes / 1024 / 1024, 2),
            'io_write_mb': round(self.io_write_bytes / 1024 / 1024, 2),
            'io_read_count': self.io_read_count,
            'io_write_count': self.io_write_count,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class OSMetrics:
    """Operating system information and metrics."""
    
    system: str
    release: str
    version: str
    machine: str
    processor: str
    architecture: str
    hostname: str
    platform_system: str
    python_version: str
    boot_time: datetime
    uptime_seconds: float
    process_count: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'system': self.system,
            'release': self.release,
            'version': self.version,
            'machine': self.machine,
            'processor': self.processor,
            'architecture': self.architecture,
            'hostname': self.hostname,
            'platform_system': self.platform_system,
            'python_version': self.python_version,
            'boot_time': self.boot_time.isoformat(),
            'uptime_seconds': round(self.uptime_seconds, 2),
            'uptime_hours': round(self.uptime_seconds / 3600, 2),
            'process_count': self.process_count,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class GPUMetrics:
    """GPU detection and utilization metrics."""
    
    devices: List[Dict[str, Any]] = field(default_factory=list)
    cuda_available: bool = False
    cuda_version: Optional[str] = None
    device_count: int = 0
    memory_total_bytes: int = 0
    memory_used_bytes: int = 0
    memory_free_bytes: int = 0
    utilization_percent: float = 0.0
    temperature_celsius: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'devices': self.devices,
            'cuda_available': self.cuda_available,
            'cuda_version': self.cuda_version,
            'device_count': self.device_count,
            'memory_total_mb': round(self.memory_total_bytes / 1024 / 1024, 2),
            'memory_used_mb': round(self.memory_used_bytes / 1024 / 1024, 2),
            'memory_free_mb': round(self.memory_free_bytes / 1024 / 1024, 2),
            'memory_usage_percent': round((self.memory_used_bytes / self.memory_total_bytes * 100) if self.memory_total_bytes > 0 else 0, 2),
            'utilization_percent': round(self.utilization_percent, 2),
            'temperature_celsius': self.temperature_celsius,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class SystemSnapshot:
    """Complete system metrics snapshot."""
    
    cpu: CPUMetrics
    memory: MemoryMetrics
    storage: StorageMetrics
    os: OSMetrics
    gpu: GPUMetrics
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'cpu': self.cpu.to_dict(),
            'memory': self.memory.to_dict(),
            'storage': self.storage.to_dict(),
            'os': self.os.to_dict(),
            'gpu': self.gpu.to_dict(),
            'timestamp': self.timestamp.isoformat()
        }


class SystemMetricsCollector:
    """
    Collects environmental system metrics including CPU, RAM, storage, OS, and GPU.
    
    Provides both one-time snapshots and continuous monitoring capabilities.
    """
    
    def __init__(self, monitoring_interval: float = 1.0):
        self.monitoring_interval = monitoring_interval
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._latest_snapshot: Optional[SystemSnapshot] = None
        self._snapshot_history: List[SystemSnapshot] = []
        self._max_history = 1000
        
        # Cache OS info since it doesn't change
        self._os_metrics_cache: Optional[OSMetrics] = None
        
        logger.debug("SystemMetricsCollector initialized")
    
    def get_cpu_metrics(self) -> CPUMetrics:
        """Get current CPU metrics."""
        try:
            # Get overall CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)
            
            # Get core counts
            logical_cores = psutil.cpu_count(logical=True)
            physical_cores = psutil.cpu_count(logical=False)
            
            # Get CPU frequency
            cpu_freq = psutil.cpu_freq()
            freq_current = cpu_freq.current if cpu_freq else 0.0
            freq_max = cpu_freq.max if cpu_freq else 0.0
            freq_min = cpu_freq.min if cpu_freq else 0.0
            
            # Get load averages (Unix-like systems only)
            load_avg_1min = load_avg_5min = load_avg_15min = None
            if hasattr(os, 'getloadavg'):
                try:
                    load_avg_1min, load_avg_5min, load_avg_15min = os.getloadavg()
                except (OSError, AttributeError):
                    pass
            
            # Get CPU stats
            cpu_stats = psutil.cpu_stats()
            context_switches = cpu_stats.ctx_switches
            interrupts = cpu_stats.interrupts
            
            return CPUMetrics(
                usage_percent=cpu_percent,
                usage_per_core=cpu_per_core,
                logical_cores=logical_cores or 0,
                physical_cores=physical_cores or 0,
                frequency_current=freq_current,
                frequency_max=freq_max,
                frequency_min=freq_min,
                load_average_1min=load_avg_1min,
                load_average_5min=load_avg_5min,
                load_average_15min=load_avg_15min,
                context_switches=context_switches,
                interrupts=interrupts
            )
        except Exception as e:
            logger.warning(f"Failed to collect CPU metrics: {e}")
            return CPUMetrics(usage_percent=0.0)
    
    def get_memory_metrics(self) -> MemoryMetrics:
        """Get current memory metrics."""
        try:
            # Virtual memory
            virtual_mem = psutil.virtual_memory()
            
            # Swap memory
            swap_mem = psutil.swap_memory()
            
            # Additional memory info (Unix-like systems)
            cached_bytes = getattr(virtual_mem, 'cached', 0)
            buffers_bytes = getattr(virtual_mem, 'buffers', 0)
            shared_bytes = getattr(virtual_mem, 'shared', 0)
            
            return MemoryMetrics(
                total_bytes=virtual_mem.total,
                available_bytes=virtual_mem.available,
                used_bytes=virtual_mem.used,
                free_bytes=virtual_mem.free,
                usage_percent=virtual_mem.percent,
                swap_total_bytes=swap_mem.total,
                swap_used_bytes=swap_mem.used,
                swap_free_bytes=swap_mem.free,
                swap_usage_percent=swap_mem.percent,
                cached_bytes=cached_bytes,
                buffers_bytes=buffers_bytes,
                shared_bytes=shared_bytes
            )
        except Exception as e:
            logger.warning(f"Failed to collect memory metrics: {e}")
            return MemoryMetrics(
                total_bytes=0, available_bytes=0, used_bytes=0, 
                free_bytes=0, usage_percent=0.0
            )
    
    def get_storage_metrics(self) -> StorageMetrics:
        """Get current storage metrics."""
        try:
            devices = []
            total_bytes = used_bytes = free_bytes = 0
            
            # Get disk partitions
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    device_info = {
                        'device': partition.device,
                        'mountpoint': partition.mountpoint,
                        'filesystem': partition.fstype,
                        'total_gb': round(usage.total / 1024 / 1024 / 1024, 2),
                        'used_gb': round(usage.used / 1024 / 1024 / 1024, 2),
                        'free_gb': round(usage.free / 1024 / 1024 / 1024, 2),
                        'usage_percent': round((usage.used / usage.total * 100) if usage.total > 0 else 0, 2)
                    }
                    devices.append(device_info)
                    
                    total_bytes += usage.total
                    used_bytes += usage.used
                    free_bytes += usage.free
                except (PermissionError, OSError):
                    # Skip inaccessible partitions
                    continue
            
            # Get disk I/O stats
            disk_io = psutil.disk_io_counters()
            io_read_bytes = disk_io.read_bytes if disk_io else 0
            io_write_bytes = disk_io.write_bytes if disk_io else 0
            io_read_count = disk_io.read_count if disk_io else 0
            io_write_count = disk_io.write_count if disk_io else 0
            
            usage_percent = (used_bytes / total_bytes * 100) if total_bytes > 0 else 0
            
            return StorageMetrics(
                devices=devices,
                total_bytes=total_bytes,
                used_bytes=used_bytes,
                free_bytes=free_bytes,
                usage_percent=usage_percent,
                io_read_bytes=io_read_bytes,
                io_write_bytes=io_write_bytes,
                io_read_count=io_read_count,
                io_write_count=io_write_count
            )
        except Exception as e:
            logger.warning(f"Failed to collect storage metrics: {e}")
            return StorageMetrics()
    
    def get_os_metrics(self) -> OSMetrics:
        """Get current OS metrics (cached after first call)."""
        if self._os_metrics_cache:
            # Update dynamic values
            self._os_metrics_cache.uptime_seconds = time.time() - psutil.boot_time()
            self._os_metrics_cache.process_count = len(psutil.pids())
            self._os_metrics_cache.timestamp = datetime.now()
            return self._os_metrics_cache
        
        try:
            # Static OS information
            system_info = platform.uname()
            boot_time = datetime.fromtimestamp(psutil.boot_time())
            uptime_seconds = time.time() - psutil.boot_time()
            
            # Process count
            process_count = len(psutil.pids())
            
            # Python version
            python_version = platform.python_version()
            
            # Cache additional platform calls to avoid repeated system calls
            processor = system_info.processor or platform.processor()
            architecture = platform.architecture()[0]
            platform_system = platform.system()
            
            self._os_metrics_cache = OSMetrics(
                system=system_info.system,
                release=system_info.release,
                version=system_info.version,
                machine=system_info.machine,
                processor=processor,
                architecture=architecture,
                hostname=system_info.node,
                platform_system=platform_system,
                python_version=python_version,
                boot_time=boot_time,
                uptime_seconds=uptime_seconds,
                process_count=process_count
            )
            
            return self._os_metrics_cache
            
        except Exception as e:
            logger.warning(f"Failed to collect OS metrics: {e}")
            return OSMetrics(
                system="unknown", release="unknown", version="unknown",
                machine="unknown", processor="unknown", architecture="unknown",
                hostname="unknown", platform_system="unknown", python_version="unknown",
                boot_time=datetime.now(), uptime_seconds=0
            )
    
    def get_gpu_metrics(self) -> GPUMetrics:
        """Get current GPU metrics."""
        gpu_metrics = GPUMetrics()
        
        # Check for CUDA availability
        try:
            import torch
            gpu_metrics.cuda_available = torch.cuda.is_available()
            if gpu_metrics.cuda_available:
                gpu_metrics.device_count = torch.cuda.device_count()
                gpu_metrics.cuda_version = torch.version.cuda
                
                devices = []
                total_memory = used_memory = 0
                
                for i in range(gpu_metrics.device_count):
                    try:
                        device_props = torch.cuda.get_device_properties(i)
                        device_name = device_props.name
                        device_memory = device_props.total_memory
                        
                        # Get memory usage
                        torch.cuda.set_device(i)
                        memory_allocated = torch.cuda.memory_allocated(i)
                        memory_cached = torch.cuda.memory_reserved(i)
                        
                        device_info = {
                            'id': i,
                            'name': device_name,
                            'compute_capability': f"{device_props.major}.{device_props.minor}",
                            'total_memory_mb': round(device_memory / 1024 / 1024, 2),
                            'allocated_memory_mb': round(memory_allocated / 1024 / 1024, 2),
                            'cached_memory_mb': round(memory_cached / 1024 / 1024, 2),
                            'multiprocessor_count': device_props.multi_processor_count
                        }
                        devices.append(device_info)
                        
                        total_memory += device_memory
                        used_memory += memory_allocated
                        
                    except Exception as e:
                        logger.debug(f"Failed to get info for GPU {i}: {e}")
                
                gpu_metrics.devices = devices
                gpu_metrics.memory_total_bytes = total_memory
                gpu_metrics.memory_used_bytes = used_memory
                gpu_metrics.memory_free_bytes = total_memory - used_memory
                
        except ImportError:
            logger.debug("PyTorch not available for GPU metrics")
        except Exception as e:
            logger.debug(f"Failed to collect GPU metrics via PyTorch: {e}")
        
        # Try nvidia-smi for additional info
        try:
            result = subprocess.run(['nvidia-smi', '--query-gpu=utilization.gpu,temperature.gpu', 
                                   '--format=csv,noheader,nounits'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if lines and lines[0]:
                    parts = lines[0].split(', ')
                    if len(parts) >= 2:
                        gpu_metrics.utilization_percent = float(parts[0])
                        gpu_metrics.temperature_celsius = float(parts[1])
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, ValueError, FileNotFoundError):
            logger.debug("nvidia-smi not available or failed")
        
        return gpu_metrics
    
    def get_system_snapshot(self) -> SystemSnapshot:
        """Get complete system metrics snapshot."""
        try:
            cpu_metrics = self.get_cpu_metrics()
            memory_metrics = self.get_memory_metrics()
            storage_metrics = self.get_storage_metrics()
            os_metrics = self.get_os_metrics()
            gpu_metrics = self.get_gpu_metrics()
            
            snapshot = SystemSnapshot(
                cpu=cpu_metrics,
                memory=memory_metrics,
                storage=storage_metrics,
                os=os_metrics,
                gpu=gpu_metrics
            )
            
            with self._lock:
                self._latest_snapshot = snapshot
                self._snapshot_history.append(snapshot)
                
                # Trim history if needed
                if len(self._snapshot_history) > self._max_history:
                    self._snapshot_history = self._snapshot_history[-self._max_history:]
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Failed to collect system snapshot: {e}")
            raise
    
    def get_latest_snapshot(self) -> Optional[SystemSnapshot]:
        """Get the latest cached system snapshot."""
        with self._lock:
            return self._latest_snapshot
    
    def get_snapshot_history(self, limit: Optional[int] = None) -> List[SystemSnapshot]:
        """Get snapshot history."""
        with self._lock:
            if limit:
                return self._snapshot_history[-limit:]
            return self._snapshot_history.copy()
    
    def start_monitoring(self) -> None:
        """Start continuous system monitoring."""
        if self._monitoring:
            logger.warning("System monitoring already running")
            return
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info(f"Started system monitoring with {self.monitoring_interval}s interval")
    
    def stop_monitoring(self) -> None:
        """Stop continuous system monitoring."""
        if not self._monitoring:
            return
        
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=self.monitoring_interval * 2)
        logger.info("Stopped system monitoring")
    
    def _monitor_loop(self) -> None:
        """Continuous monitoring loop."""
        while self._monitoring:
            try:
                self.get_system_snapshot()
                time.sleep(self.monitoring_interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.monitoring_interval)
    
    def get_resource_utilization_summary(self) -> Dict[str, Any]:
        """Get a summary of current resource utilization."""
        snapshot = self.get_system_snapshot()
        
        return {
            'cpu_usage_percent': snapshot.cpu.usage_percent,
            'memory_usage_percent': snapshot.memory.usage_percent,
            'storage_usage_percent': snapshot.storage.usage_percent,
            'gpu_utilization_percent': snapshot.gpu.utilization_percent,
            'gpu_memory_usage_percent': round(
                (snapshot.gpu.memory_used_bytes / snapshot.gpu.memory_total_bytes * 100) 
                if snapshot.gpu.memory_total_bytes > 0 else 0, 2
            ),
            'system_load': {
                'load_1min': snapshot.cpu.load_average_1min,
                'load_5min': snapshot.cpu.load_average_5min,
                'load_15min': snapshot.cpu.load_average_15min
            },
            'uptime_hours': round(snapshot.os.uptime_seconds / 3600, 2),
            'process_count': snapshot.os.process_count,
            'timestamp': snapshot.timestamp.isoformat()
        }


# Global system metrics collector instance
_global_system_collector: Optional[SystemMetricsCollector] = None
_system_collector_lock = threading.Lock()


def get_global_system_collector() -> SystemMetricsCollector:
    """Get or create the global system metrics collector instance."""
    global _global_system_collector
    
    with _system_collector_lock:
        if _global_system_collector is None:
            _global_system_collector = SystemMetricsCollector()
        return _global_system_collector


def reset_global_system_collector() -> None:
    """Reset the global system metrics collector (mainly for testing)."""
    global _global_system_collector
    
    with _system_collector_lock:
        if _global_system_collector:
            _global_system_collector.stop_monitoring()
        _global_system_collector = None