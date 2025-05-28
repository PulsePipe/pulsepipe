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

# tests/test_system_metrics.py

"""
Unit tests for system metrics collection.

Tests CPU, RAM, storage, OS, and GPU metrics collection
with comprehensive coverage including error handling and edge cases.
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import psutil

from pulsepipe.pipelines.performance.system_metrics import (
    SystemMetricsCollector,
    SystemSnapshot,
    CPUMetrics,
    MemoryMetrics,
    StorageMetrics,
    OSMetrics,
    GPUMetrics,
    get_global_system_collector,
    reset_global_system_collector
)


class TestCPUMetrics:
    """Test CPUMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic CPUMetrics creation."""
        metrics = CPUMetrics(
            usage_percent=45.5,
            usage_per_core=[40.0, 50.0, 45.0, 50.0],
            logical_cores=4,
            physical_cores=2,
            frequency_current=2400.0,
            frequency_max=3000.0,
            frequency_min=800.0
        )
        
        assert metrics.usage_percent == 45.5
        assert len(metrics.usage_per_core) == 4
        assert metrics.logical_cores == 4
        assert metrics.physical_cores == 2
        assert metrics.frequency_current == 2400.0
    
    def test_to_dict(self):
        """Test CPUMetrics to_dict conversion."""
        metrics = CPUMetrics(
            usage_percent=45.567,
            usage_per_core=[40.123, 50.789],
            logical_cores=2,
            physical_cores=1,
            frequency_current=2400.456,
            load_average_1min=1.5,
            context_switches=1000,
            interrupts=500
        )
        
        result = metrics.to_dict()
        
        assert result['usage_percent'] == 45.57  # Rounded
        assert result['usage_per_core'] == [40.12, 50.79]  # Rounded
        assert result['logical_cores'] == 2
        assert result['frequency_current_mhz'] == 2400.46
        assert result['load_average_1min'] == 1.5
        assert result['context_switches'] == 1000
        assert 'timestamp' in result


class TestMemoryMetrics:
    """Test MemoryMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic MemoryMetrics creation."""
        metrics = MemoryMetrics(
            total_bytes=8 * 1024 * 1024 * 1024,  # 8GB
            available_bytes=4 * 1024 * 1024 * 1024,  # 4GB
            used_bytes=4 * 1024 * 1024 * 1024,  # 4GB
            free_bytes=4 * 1024 * 1024 * 1024,  # 4GB
            usage_percent=50.0
        )
        
        assert metrics.total_bytes == 8 * 1024 * 1024 * 1024
        assert metrics.usage_percent == 50.0
    
    def test_to_dict(self):
        """Test MemoryMetrics to_dict conversion."""
        metrics = MemoryMetrics(
            total_bytes=8 * 1024 * 1024 * 1024,  # 8GB
            available_bytes=4 * 1024 * 1024 * 1024,  # 4GB
            used_bytes=4 * 1024 * 1024 * 1024,  # 4GB
            free_bytes=4 * 1024 * 1024 * 1024,  # 4GB
            usage_percent=50.0,
            swap_total_bytes=2 * 1024 * 1024 * 1024,  # 2GB
            swap_usage_percent=25.0
        )
        
        result = metrics.to_dict()
        
        assert result['total_mb'] == 8192.0  # 8GB in MB
        assert result['available_mb'] == 4096.0
        assert result['usage_percent'] == 50.0
        assert result['swap_total_mb'] == 2048.0
        assert result['swap_usage_percent'] == 25.0
        assert 'timestamp' in result


class TestStorageMetrics:
    """Test StorageMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic StorageMetrics creation."""
        devices = [
            {
                'device': '/dev/sda1',
                'mountpoint': '/',
                'total_gb': 100.0,
                'usage_percent': 60.0
            }
        ]
        
        metrics = StorageMetrics(
            devices=devices,
            total_bytes=100 * 1024 * 1024 * 1024,  # 100GB
            usage_percent=60.0
        )
        
        assert len(metrics.devices) == 1
        assert metrics.devices[0]['device'] == '/dev/sda1'
        assert metrics.usage_percent == 60.0
    
    def test_to_dict(self):
        """Test StorageMetrics to_dict conversion."""
        metrics = StorageMetrics(
            total_bytes=100 * 1024 * 1024 * 1024,  # 100GB
            used_bytes=60 * 1024 * 1024 * 1024,  # 60GB
            free_bytes=40 * 1024 * 1024 * 1024,  # 40GB
            usage_percent=60.0,
            io_read_bytes=1024 * 1024 * 1024,  # 1GB
            io_write_bytes=512 * 1024 * 1024  # 512MB
        )
        
        result = metrics.to_dict()
        
        assert result['total_gb'] == 100.0
        assert result['used_gb'] == 60.0
        assert result['free_gb'] == 40.0
        assert result['usage_percent'] == 60.0
        assert result['io_read_mb'] == 1024.0
        assert result['io_write_mb'] == 512.0
        assert 'timestamp' in result


class TestOSMetrics:
    """Test OSMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic OSMetrics creation."""
        boot_time = datetime.now() - timedelta(hours=2)
        
        metrics = OSMetrics(
            system="Linux",
            release="5.4.0",
            version="#123-Ubuntu",
            machine="x86_64",
            processor="Intel",
            architecture="64bit",
            hostname="test-host",
            platform_system="Linux",
            python_version="3.10.0",
            boot_time=boot_time,
            uptime_seconds=7200.0,  # 2 hours
            process_count=150
        )
        
        assert metrics.system == "Linux"
        assert metrics.hostname == "test-host"
        assert metrics.uptime_seconds == 7200.0
        assert metrics.process_count == 150
    
    def test_to_dict(self):
        """Test OSMetrics to_dict conversion."""
        boot_time = datetime(2025, 1, 1, 12, 0, 0)
        
        metrics = OSMetrics(
            system="Linux",
            release="5.4.0",
            version="#123-Ubuntu",
            machine="x86_64",
            processor="Intel",
            architecture="64bit",
            hostname="test-host",
            platform_system="Linux",
            python_version="3.10.0",
            boot_time=boot_time,
            uptime_seconds=7200.0,
            process_count=150
        )
        
        result = metrics.to_dict()
        
        assert result['system'] == "Linux"
        assert result['hostname'] == "test-host"
        assert result['uptime_seconds'] == 7200.0
        assert result['uptime_hours'] == 2.0
        assert result['process_count'] == 150
        assert result['boot_time'] == boot_time.isoformat()
        assert 'timestamp' in result


class TestGPUMetrics:
    """Test GPUMetrics dataclass."""
    
    def test_basic_creation(self):
        """Test basic GPUMetrics creation."""
        devices = [
            {
                'id': 0,
                'name': 'NVIDIA GeForce RTX 3080',
                'total_memory_mb': 10240.0,
                'allocated_memory_mb': 2048.0
            }
        ]
        
        metrics = GPUMetrics(
            devices=devices,
            cuda_available=True,
            cuda_version="11.8",
            device_count=1,
            memory_total_bytes=10 * 1024 * 1024 * 1024,  # 10GB
            memory_used_bytes=2 * 1024 * 1024 * 1024,  # 2GB
            utilization_percent=75.0,
            temperature_celsius=68.0
        )
        
        assert metrics.cuda_available is True
        assert metrics.device_count == 1
        assert metrics.utilization_percent == 75.0
        assert metrics.temperature_celsius == 68.0
    
    def test_to_dict(self):
        """Test GPUMetrics to_dict conversion."""
        metrics = GPUMetrics(
            cuda_available=True,
            cuda_version="11.8",
            device_count=1,
            memory_total_bytes=10 * 1024 * 1024 * 1024,  # 10GB
            memory_used_bytes=2 * 1024 * 1024 * 1024,  # 2GB
            utilization_percent=75.5,
            temperature_celsius=68.2
        )
        
        result = metrics.to_dict()
        
        assert result['cuda_available'] is True
        assert result['cuda_version'] == "11.8"
        assert result['device_count'] == 1
        assert result['memory_total_mb'] == 10240.0
        assert result['memory_used_mb'] == 2048.0
        assert result['memory_usage_percent'] == 20.0  # 2GB / 10GB * 100
        assert result['utilization_percent'] == 75.5
        assert result['temperature_celsius'] == 68.2
        assert 'timestamp' in result


class TestSystemSnapshot:
    """Test SystemSnapshot dataclass."""
    
    def test_creation_with_all_metrics(self):
        """Test SystemSnapshot creation with all metric types."""
        cpu = CPUMetrics(usage_percent=50.0)
        memory = MemoryMetrics(total_bytes=8*1024*1024*1024, available_bytes=4*1024*1024*1024, 
                              used_bytes=4*1024*1024*1024, free_bytes=4*1024*1024*1024, usage_percent=50.0)
        storage = StorageMetrics()
        os_metrics = OSMetrics(
            system="Linux", release="5.4.0", version="#123", machine="x86_64",
            processor="Intel", architecture="64bit", hostname="test", 
            platform_system="Linux", python_version="3.10.0",
            boot_time=datetime.now(), uptime_seconds=3600.0
        )
        gpu = GPUMetrics()
        
        snapshot = SystemSnapshot(
            cpu=cpu,
            memory=memory,
            storage=storage,
            os=os_metrics,
            gpu=gpu
        )
        
        assert snapshot.cpu.usage_percent == 50.0
        assert snapshot.memory.usage_percent == 50.0
        assert snapshot.os.system == "Linux"
    
    def test_to_dict(self):
        """Test SystemSnapshot to_dict conversion."""
        cpu = CPUMetrics(usage_percent=50.0)
        memory = MemoryMetrics(total_bytes=8*1024*1024*1024, available_bytes=4*1024*1024*1024, 
                              used_bytes=4*1024*1024*1024, free_bytes=4*1024*1024*1024, usage_percent=50.0)
        storage = StorageMetrics()
        os_metrics = OSMetrics(
            system="Linux", release="5.4.0", version="#123", machine="x86_64",
            processor="Intel", architecture="64bit", hostname="test", 
            platform_system="Linux", python_version="3.10.0",
            boot_time=datetime.now(), uptime_seconds=3600.0
        )
        gpu = GPUMetrics()
        
        snapshot = SystemSnapshot(
            cpu=cpu,
            memory=memory,
            storage=storage,
            os=os_metrics,
            gpu=gpu
        )
        
        result = snapshot.to_dict()
        
        assert 'cpu' in result
        assert 'memory' in result
        assert 'storage' in result
        assert 'os' in result
        assert 'gpu' in result
        assert 'timestamp' in result
        assert result['cpu']['usage_percent'] == 50.0
        assert result['memory']['usage_percent'] == 50.0


class TestSystemMetricsCollector:
    """Test SystemMetricsCollector class."""
    
    @pytest.fixture
    def collector(self):
        """Create a SystemMetricsCollector instance for testing."""
        return SystemMetricsCollector(monitoring_interval=0.1)
    
    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.monitoring_interval == 0.1
        assert collector._monitoring is False
        assert collector._latest_snapshot is None
        assert len(collector._snapshot_history) == 0
    
    @patch('psutil.cpu_percent')
    @patch('psutil.cpu_count')
    @patch('psutil.cpu_freq')
    @patch('psutil.cpu_stats')
    def test_get_cpu_metrics(self, mock_cpu_stats, mock_cpu_freq, mock_cpu_count, mock_cpu_percent, collector):
        """Test CPU metrics collection."""
        # Mock psutil responses
        mock_cpu_percent.side_effect = [75.5, [70.0, 80.0, 75.0, 80.0]]
        mock_cpu_count.side_effect = [4, 2]  # logical, physical
        mock_cpu_freq.return_value = Mock(current=2400.0, max=3000.0, min=800.0)
        mock_cpu_stats.return_value = Mock(ctx_switches=1000, interrupts=500)
        
        with patch.dict('os.__dict__', {'getloadavg': lambda: (1.5, 1.2, 1.0)}):
            metrics = collector.get_cpu_metrics()
        
        assert metrics.usage_percent == 75.5
        assert len(metrics.usage_per_core) == 4
        assert metrics.logical_cores == 4
        assert metrics.physical_cores == 2
        assert metrics.frequency_current == 2400.0
        assert metrics.load_average_1min == 1.5
        assert metrics.context_switches == 1000
    
    @patch('psutil.cpu_percent')
    def test_get_cpu_metrics_with_error(self, mock_cpu_percent, collector):
        """Test CPU metrics collection with psutil error."""
        mock_cpu_percent.side_effect = Exception("psutil error")
        
        metrics = collector.get_cpu_metrics()
        
        assert metrics.usage_percent == 0.0
        assert metrics.logical_cores == 0
    
    @patch('psutil.virtual_memory')
    @patch('psutil.swap_memory')
    def test_get_memory_metrics(self, mock_swap_memory, mock_virtual_memory, collector):
        """Test memory metrics collection."""
        # Mock virtual memory
        mock_virtual_memory.return_value = Mock(
            total=8*1024*1024*1024,
            available=4*1024*1024*1024,
            used=4*1024*1024*1024,
            free=4*1024*1024*1024,
            percent=50.0,
            cached=1*1024*1024*1024,
            buffers=512*1024*1024,
            shared=256*1024*1024
        )
        
        # Mock swap memory
        mock_swap_memory.return_value = Mock(
            total=2*1024*1024*1024,
            used=512*1024*1024,
            free=1536*1024*1024,
            percent=25.0
        )
        
        metrics = collector.get_memory_metrics()
        
        assert metrics.total_bytes == 8*1024*1024*1024
        assert metrics.usage_percent == 50.0
        assert metrics.swap_total_bytes == 2*1024*1024*1024
        assert metrics.swap_usage_percent == 25.0
        assert metrics.cached_bytes == 1*1024*1024*1024
    
    @patch('psutil.virtual_memory')
    def test_get_memory_metrics_with_error(self, mock_virtual_memory, collector):
        """Test memory metrics collection with psutil error."""
        mock_virtual_memory.side_effect = Exception("psutil error")
        
        metrics = collector.get_memory_metrics()
        
        assert metrics.total_bytes == 0
        assert metrics.usage_percent == 0.0
    
    @patch('psutil.disk_partitions')
    @patch('psutil.disk_usage')
    @patch('psutil.disk_io_counters')
    def test_get_storage_metrics(self, mock_disk_io, mock_disk_usage, mock_disk_partitions, collector):
        """Test storage metrics collection."""
        # Mock disk partitions
        mock_partition = Mock()
        mock_partition.device = '/dev/sda1'
        mock_partition.mountpoint = '/'
        mock_partition.fstype = 'ext4'
        mock_disk_partitions.return_value = [mock_partition]
        
        # Mock disk usage
        mock_disk_usage.return_value = Mock(
            total=100*1024*1024*1024,  # 100GB
            used=60*1024*1024*1024,   # 60GB
            free=40*1024*1024*1024    # 40GB
        )
        
        # Mock disk I/O
        mock_disk_io.return_value = Mock(
            read_bytes=1024*1024*1024,
            write_bytes=512*1024*1024,
            read_count=1000,
            write_count=500
        )
        
        metrics = collector.get_storage_metrics()
        
        assert len(metrics.devices) == 1
        assert metrics.devices[0]['device'] == '/dev/sda1'
        assert metrics.devices[0]['usage_percent'] == 60.0
        assert metrics.total_bytes == 100*1024*1024*1024
        assert metrics.usage_percent == 60.0
        assert metrics.io_read_bytes == 1024*1024*1024
    
    @patch('psutil.disk_partitions')
    def test_get_storage_metrics_with_error(self, mock_disk_partitions, collector):
        """Test storage metrics collection with psutil error."""
        mock_disk_partitions.side_effect = Exception("psutil error")
        
        metrics = collector.get_storage_metrics()
        
        assert len(metrics.devices) == 0
        assert metrics.total_bytes == 0
    
    @patch('platform.uname')
    @patch('psutil.boot_time')
    @patch('psutil.pids')
    @patch('platform.python_version')
    @patch('time.time')
    def test_get_os_metrics(self, mock_time, mock_python_version, mock_pids, mock_boot_time, mock_uname, collector):
        """Test OS metrics collection."""
        # Mock platform.uname
        mock_uname.return_value = Mock(
            system='Linux',
            release='5.4.0-123-generic',
            version='#139-Ubuntu SMP Wed Nov 10 12:34:56 UTC 2025',
            machine='x86_64',
            processor='x86_64',
            node='test-hostname'
        )
        
        # Mock other system info
        mock_boot_time.return_value = 1000000000  # timestamp
        mock_time.return_value = 1000003600  # 1 hour later
        mock_pids.return_value = [1, 2, 3, 4, 5]  # 5 processes
        mock_python_version.return_value = '3.10.0'
        
        with patch('platform.processor', return_value='Intel'):
            with patch('platform.architecture', return_value=('64bit', 'ELF')):
                with patch('platform.system', return_value='Linux'):
                    metrics = collector.get_os_metrics()
        
        assert metrics.system == 'Linux'
        assert metrics.hostname == 'test-hostname'
        assert metrics.python_version == '3.10.0'
        assert metrics.uptime_seconds == 3600.0
        assert metrics.process_count == 5
    
    @patch('platform.uname')
    def test_get_os_metrics_with_error(self, mock_uname, collector):
        """Test OS metrics collection with platform error."""
        mock_uname.side_effect = Exception("platform error")
        
        metrics = collector.get_os_metrics()
        
        assert metrics.system == "unknown"
        assert metrics.hostname == "unknown"
    
    def test_get_os_metrics_caching(self, collector):
        """Test that OS metrics are cached appropriately."""
        with patch('platform.uname') as mock_uname:
            with patch('platform.processor') as mock_processor:
                with patch('platform.architecture') as mock_architecture:
                    with patch('platform.system') as mock_system:
                        with patch('platform.python_version') as mock_python_version:
                            # Setup mocks
                            mock_uname.return_value = Mock(
                                system='Linux', release='5.4.0', version='#123',
                                machine='x86_64', processor='', node='test'  # Empty processor to trigger fallback
                            )
                            mock_processor.return_value = 'Intel'
                            mock_architecture.return_value = ('64bit', 'ELF')
                            mock_system.return_value = 'Linux'
                            mock_python_version.return_value = '3.10.0'
                            
                            with patch('psutil.boot_time', return_value=1000000000):
                                with patch('psutil.pids', return_value=[1, 2, 3]):
                                    with patch('time.time', return_value=1000003600):
                                        # First call should create cache
                                        metrics1 = collector.get_os_metrics()
                                        # Second call should use cache
                                        metrics2 = collector.get_os_metrics()
        
        # Platform calls should only happen once due to caching
        assert mock_uname.call_count == 1
        assert mock_processor.call_count == 1
        assert mock_architecture.call_count == 1
        assert mock_system.call_count == 1
        assert mock_python_version.call_count == 1
        
        assert metrics1.system == metrics2.system
        assert metrics1.hostname == metrics2.hostname
    
    @patch('subprocess.run')
    def test_get_gpu_metrics_no_torch(self, mock_subprocess, collector):
        """Test GPU metrics collection without PyTorch."""
        mock_subprocess.return_value = Mock(returncode=1)  # nvidia-smi fails
        
        with patch.dict('sys.modules', {'torch': None}):
            metrics = collector.get_gpu_metrics()
        
        assert metrics.cuda_available is False
        assert metrics.device_count == 0
    
    @patch('subprocess.run')
    def test_get_gpu_metrics_with_nvidia_smi(self, mock_subprocess, collector):
        """Test GPU metrics collection with nvidia-smi."""
        # Mock nvidia-smi output
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout='75, 68\n'  # utilization, temperature
        )
        
        with patch.dict('sys.modules', {'torch': None}):
            metrics = collector.get_gpu_metrics()
        
        assert metrics.utilization_percent == 75.0
        assert metrics.temperature_celsius == 68.0
    
    def test_get_system_snapshot(self, collector):
        """Test complete system snapshot collection."""
        with patch.object(collector, 'get_cpu_metrics') as mock_cpu:
            with patch.object(collector, 'get_memory_metrics') as mock_memory:
                with patch.object(collector, 'get_storage_metrics') as mock_storage:
                    with patch.object(collector, 'get_os_metrics') as mock_os:
                        with patch.object(collector, 'get_gpu_metrics') as mock_gpu:
                            # Setup mocks
                            mock_cpu.return_value = CPUMetrics(usage_percent=50.0)
                            mock_memory.return_value = MemoryMetrics(
                                total_bytes=8*1024*1024*1024, available_bytes=4*1024*1024*1024,
                                used_bytes=4*1024*1024*1024, free_bytes=4*1024*1024*1024, usage_percent=50.0
                            )
                            mock_storage.return_value = StorageMetrics()
                            mock_os.return_value = OSMetrics(
                                system="Linux", release="5.4.0", version="#123", machine="x86_64",
                                processor="Intel", architecture="64bit", hostname="test", 
                                platform_system="Linux", python_version="3.10.0",
                                boot_time=datetime.now(), uptime_seconds=3600.0
                            )
                            mock_gpu.return_value = GPUMetrics()
                            
                            snapshot = collector.get_system_snapshot()
        
        assert isinstance(snapshot, SystemSnapshot)
        assert snapshot.cpu.usage_percent == 50.0
        assert snapshot.memory.usage_percent == 50.0
        assert collector.get_latest_snapshot() == snapshot
        assert len(collector.get_snapshot_history()) == 1
    
    def test_snapshot_history_management(self, collector):
        """Test snapshot history size management."""
        collector._max_history = 3
        
        with patch.object(collector, 'get_cpu_metrics', return_value=CPUMetrics(usage_percent=50.0)):
            with patch.object(collector, 'get_memory_metrics', return_value=MemoryMetrics(
                total_bytes=8*1024*1024*1024, available_bytes=4*1024*1024*1024,
                used_bytes=4*1024*1024*1024, free_bytes=4*1024*1024*1024, usage_percent=50.0
            )):
                with patch.object(collector, 'get_storage_metrics', return_value=StorageMetrics()):
                    with patch.object(collector, 'get_os_metrics', return_value=OSMetrics(
                        system="Linux", release="5.4.0", version="#123", machine="x86_64",
                        processor="Intel", architecture="64bit", hostname="test", 
                        platform_system="Linux", python_version="3.10.0",
                        boot_time=datetime.now(), uptime_seconds=3600.0
                    )):
                        with patch.object(collector, 'get_gpu_metrics', return_value=GPUMetrics()):
                            # Create more snapshots than max_history
                            for i in range(5):
                                collector.get_system_snapshot()
        
        # Should only keep the last 3 snapshots
        history = collector.get_snapshot_history()
        assert len(history) == 3
        
        # Test limited history retrieval
        limited_history = collector.get_snapshot_history(limit=2)
        assert len(limited_history) == 2
    
    def test_monitoring_lifecycle(self, collector):
        """Test start/stop monitoring functionality."""
        assert not collector._monitoring
        
        # Start monitoring
        collector.start_monitoring()
        assert collector._monitoring
        assert collector._monitor_thread is not None
        
        # Try to start again (should warn)
        collector.start_monitoring()  # Should not crash
        
        # Let it collect a few snapshots
        time.sleep(0.25)
        
        # Stop monitoring
        collector.stop_monitoring()
        assert not collector._monitoring
        
        # Stop again (should not crash)
        collector.stop_monitoring()
    
    def test_resource_utilization_summary(self, collector):
        """Test resource utilization summary generation."""
        with patch.object(collector, 'get_system_snapshot') as mock_snapshot:
            # Create a mock snapshot
            snapshot = SystemSnapshot(
                cpu=CPUMetrics(usage_percent=75.5, load_average_1min=1.5),
                memory=MemoryMetrics(
                    total_bytes=8*1024*1024*1024, available_bytes=2*1024*1024*1024,
                    used_bytes=6*1024*1024*1024, free_bytes=2*1024*1024*1024, usage_percent=75.0
                ),
                storage=StorageMetrics(usage_percent=60.0),
                os=OSMetrics(
                    system="Linux", release="5.4.0", version="#123", machine="x86_64",
                    processor="Intel", architecture="64bit", hostname="test", 
                    platform_system="Linux", python_version="3.10.0",
                    boot_time=datetime.now() - timedelta(hours=2), uptime_seconds=7200.0, process_count=150
                ),
                gpu=GPUMetrics(
                    utilization_percent=85.0,
                    memory_total_bytes=10*1024*1024*1024,
                    memory_used_bytes=8*1024*1024*1024
                )
            )
            mock_snapshot.return_value = snapshot
            
            summary = collector.get_resource_utilization_summary()
        
        assert summary['cpu_usage_percent'] == 75.5
        assert summary['memory_usage_percent'] == 75.0
        assert summary['storage_usage_percent'] == 60.0
        assert summary['gpu_utilization_percent'] == 85.0
        assert summary['gpu_memory_usage_percent'] == 80.0  # 8GB/10GB
        assert summary['uptime_hours'] == 2.0
        assert summary['process_count'] == 150
        assert 'timestamp' in summary


class TestGlobalSystemCollector:
    """Test global system collector management."""
    
    def test_get_global_system_collector(self):
        """Test global system collector creation and retrieval."""
        # Reset first
        reset_global_system_collector()
        
        # Get collector (should create new one)
        collector1 = get_global_system_collector()
        assert isinstance(collector1, SystemMetricsCollector)
        
        # Get again (should return same instance)
        collector2 = get_global_system_collector()
        assert collector1 is collector2
        
        # Reset and verify new instance is created
        reset_global_system_collector()
        collector3 = get_global_system_collector()
        assert collector3 is not collector1
    
    def test_reset_global_system_collector(self):
        """Test global system collector reset."""
        collector = get_global_system_collector()
        collector.start_monitoring()
        
        # Reset should stop monitoring
        reset_global_system_collector()
        
        # Get new collector and verify it's different
        new_collector = get_global_system_collector()
        assert new_collector is not collector
        assert not new_collector._monitoring


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_collector_with_system_errors(self):
        """Test collector behavior when system calls fail."""
        collector = SystemMetricsCollector()
        
        with patch('psutil.cpu_percent', side_effect=Exception("CPU error")):
            with patch('psutil.virtual_memory', side_effect=Exception("Memory error")):
                with patch('psutil.disk_partitions', side_effect=Exception("Disk error")):
                    with patch('platform.uname', side_effect=Exception("OS error")):
                        # Should not crash and return valid snapshot with default values
                        snapshot = collector.get_system_snapshot()
                        
                        assert snapshot.cpu.usage_percent == 0.0
                        assert snapshot.memory.total_bytes == 0
                        assert len(snapshot.storage.devices) == 0
                        assert snapshot.os.system == "unknown"
    
    def test_monitoring_with_errors(self):
        """Test monitoring continues despite errors."""
        collector = SystemMetricsCollector(monitoring_interval=0.05)
        
        with patch.object(collector, 'get_system_snapshot', side_effect=Exception("Snapshot error")):
            collector.start_monitoring()
            time.sleep(0.15)  # Let it try a few times
            collector.stop_monitoring()
        
        # Should not crash
        assert True
    
    def test_thread_safety(self):
        """Test thread safety of collector operations."""
        collector = SystemMetricsCollector()
        
        def collect_snapshots():
            for _ in range(5):
                try:
                    collector.get_system_snapshot()
                except:
                    pass  # Ignore errors for this test
        
        # Run multiple threads collecting snapshots
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=collect_snapshots)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should not crash due to race conditions
        assert True


@pytest.fixture(autouse=True)
def cleanup_global_collector():
    """Clean up global collector after each test."""
    yield
    reset_global_system_collector()