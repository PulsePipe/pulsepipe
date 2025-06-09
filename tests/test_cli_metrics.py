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

# tests/test_cli_metrics.py

"""
Unit tests for the CLI metrics command module.

Tests all functionality of the metrics command group including export, analyze,
cleanup, and status commands with comprehensive error handling and edge cases.
"""

import os
import pytest
import tempfile
import json
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, mock_open
from click.testing import CliRunner

from pulsepipe.cli.command.metrics import metrics, export, analyze, cleanup, status, _display_metrics_table


class TestMetricsGroup:
    """Tests for the main metrics command group."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    def test_metrics_group_help(self, runner):
        """Test metrics group help command."""
        result = runner.invoke(metrics, ['--help'])
        
        assert result.exit_code == 0
        assert 'Manage and export ingestion metrics' in result.output
        assert 'export' in result.output
        assert 'analyze' in result.output
        assert 'cleanup' in result.output
        assert 'status' in result.output


class TestExportCommand:
    """Tests for the metrics export command."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_persistence_modules(self):
        """Mock persistence modules and their dependencies."""
        mock_repo = MagicMock()
        mock_config_class = MagicMock()
        mock_audit_reporter = MagicMock()
        mock_ingestion_tracker = MagicMock()
        
        return mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_export_specific_pipeline_run_json(self, mock_get_modules, runner, mock_persistence_modules):
        """Test export command for specific pipeline run in JSON format."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock the tracker's export_metrics method
        tracker_instance = MagicMock()
        mock_ingestion_tracker.return_value = tracker_instance
        
        with tempfile.TemporaryDirectory() as temp_dir:
            result = runner.invoke(export, [
                '--pipeline-run-id', 'test-run-123',
                '--format', 'json',
                '--output', os.path.join(temp_dir, 'test_output.json'),
                '--include-details'
            ])
            
            assert result.exit_code == 0
            assert '✅ Metrics exported to:' in result.output
            assert 'test_output.json' in result.output
            
            # Verify tracker was created and export_metrics was called
            mock_ingestion_tracker.assert_called_once()
            tracker_instance.export_metrics.assert_called_once()
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_export_specific_pipeline_run_csv(self, mock_get_modules, runner, mock_persistence_modules):
        """Test export command for specific pipeline run in CSV format."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        tracker_instance = MagicMock()
        mock_ingestion_tracker.return_value = tracker_instance
        
        result = runner.invoke(export, [
            '--pipeline-run-id', 'test-run-456',
            '--format', 'csv'
        ])
        
        assert result.exit_code == 0
        assert '✅ Metrics exported to:' in result.output
        assert '.csv' in result.output
        tracker_instance.export_metrics.assert_called_once()
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_export_summary_report(self, mock_get_modules, runner, mock_persistence_modules):
        """Test export command for summary report without specific pipeline run."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock the audit reporter
        reporter_instance = MagicMock()
        mock_audit_reporter.return_value = reporter_instance
        
        mock_report = MagicMock()
        reporter_instance.generate_summary_report.return_value = mock_report
        
        result = runner.invoke(export, [
            '--days', '14',
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        assert '✅ Metrics exported to:' in result.output
        
        # Verify audit reporter was used
        mock_audit_reporter.assert_called_once_with(mock_repo)
        reporter_instance.generate_summary_report.assert_called_once()
        reporter_instance.export_report.assert_called_once()
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_export_with_default_output_filename(self, mock_get_modules, runner, mock_persistence_modules):
        """Test export command with default output filename generation."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        tracker_instance = MagicMock()
        mock_ingestion_tracker.return_value = tracker_instance
        
        with patch('pulsepipe.cli.command.metrics.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = '20250608_120000'
            
            result = runner.invoke(export, [
                '--pipeline-run-id', 'test-run-789'
            ])
            
            assert result.exit_code == 0
            # Check that default filename pattern is used
            tracker_instance.export_metrics.assert_called_once()
            args = tracker_instance.export_metrics.call_args[0]
            assert 'ingestion_metrics_test-run-789_20250608_120000.json' in args[0]
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_export_exception_handling(self, mock_get_modules, runner):
        """Test export command exception handling."""
        # Make get_persistence_modules raise an exception
        mock_get_modules.side_effect = Exception("Database connection failed")
        
        result = runner.invoke(export, ['--pipeline-run-id', 'test-run'])
        
        assert result.exit_code == 1
        assert '❌ Export failed:' in result.output
        assert 'Database connection failed' in result.output
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_export_tracker_exception(self, mock_get_modules, runner, mock_persistence_modules):
        """Test export command when tracker export fails."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Make tracker export_metrics raise an exception
        tracker_instance = MagicMock()
        tracker_instance.export_metrics.side_effect = Exception("Export failed")
        mock_ingestion_tracker.return_value = tracker_instance
        
        result = runner.invoke(export, ['--pipeline-run-id', 'test-run'])
        
        assert result.exit_code == 1
        assert '❌ Export failed:' in result.output
        assert 'Export failed' in result.output


class TestAnalyzeCommand:
    """Tests for the metrics analyze command."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_persistence_modules(self):
        """Mock persistence modules and their dependencies."""
        mock_repo = MagicMock()
        mock_config_class = MagicMock()
        mock_audit_reporter = MagicMock()
        mock_ingestion_tracker = MagicMock()
        
        return mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_analyze_specific_pipeline_run_json(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test analyze command for specific pipeline run in JSON format."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock the audit reporter
        reporter_instance = MagicMock()
        mock_audit_reporter.return_value = reporter_instance
        
        mock_report = MagicMock()
        mock_report.to_json.return_value = '{"test": "report"}'
        reporter_instance.generate_pipeline_report.return_value = mock_report
        
        result = runner.invoke(analyze, [
            '--pipeline-run-id', 'test-run-123',
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        assert '"test": "report"' in result.output
        
        # Verify methods were called
        reporter_instance.generate_pipeline_report.assert_called_once_with('test-run-123')
        mock_report.to_json.assert_called_once()
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    @patch('pulsepipe.cli.command.metrics._display_metrics_table')
    def test_analyze_specific_pipeline_run_table(self, mock_display_table, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test analyze command for specific pipeline run in table format."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "postgresql"}}
        
        # Mock the audit reporter
        reporter_instance = MagicMock()
        mock_audit_reporter.return_value = reporter_instance
        
        mock_report = MagicMock()
        reporter_instance.generate_pipeline_report.return_value = mock_report
        
        result = runner.invoke(analyze, [
            '--pipeline-run-id', 'test-run-456',
            '--format', 'table'
        ])
        
        assert result.exit_code == 0
        
        # Verify table display was called
        mock_display_table.assert_called_once_with(mock_report)
        reporter_instance.generate_pipeline_report.assert_called_once_with('test-run-456')
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_analyze_summary_report(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test analyze command for summary report without specific pipeline run."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "mongodb"}}
        
        # Mock the audit reporter
        reporter_instance = MagicMock()
        mock_audit_reporter.return_value = reporter_instance
        
        mock_report = MagicMock()
        mock_report.to_json.return_value = '{"summary": "data"}'
        reporter_instance.generate_summary_report.return_value = mock_report
        
        result = runner.invoke(analyze, [
            '--days', '30',
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        assert '"summary": "data"' in result.output
        
        # Verify summary report was generated
        reporter_instance.generate_summary_report.assert_called_once()
        args = reporter_instance.generate_summary_report.call_args
        assert len(args[1]) == 2  # start_date and end_date kwargs
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_analyze_fallback_config(self, mock_get_modules, mock_find_profile, runner, mock_persistence_modules):
        """Test analyze command with fallback SQLite config when main config not found."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock no config found
        mock_find_profile.return_value = None
        
        # Mock the audit reporter
        reporter_instance = MagicMock()
        mock_audit_reporter.return_value = reporter_instance
        
        mock_report = MagicMock()
        mock_report.to_json.return_value = '{"fallback": "config"}'
        reporter_instance.generate_summary_report.return_value = mock_report
        
        result = runner.invoke(analyze, ['--format', 'json'])
        
        assert result.exit_code == 0
        
        # Verify fallback config was used
        mock_get_modules.assert_called_once()
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_analyze_exception_handling(self, mock_get_modules, runner):
        """Test analyze command exception handling."""
        mock_get_modules.side_effect = Exception("Analysis failed")
        
        result = runner.invoke(analyze, ['--days', '7'])
        
        assert result.exit_code == 1
        assert '❌ Analysis failed:' in result.output
        assert 'Analysis failed' in result.output


class TestCleanupCommand:
    """Tests for the metrics cleanup command."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_persistence_modules(self):
        """Mock persistence modules and their dependencies."""
        mock_repo = MagicMock()
        mock_config_class = MagicMock()
        mock_audit_reporter = MagicMock()
        mock_ingestion_tracker = MagicMock()
        
        return mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_cleanup_with_days(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test cleanup command with specific number of days."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock repository cleanup
        mock_repo.cleanup_old_data.return_value = 150
        
        result = runner.invoke(cleanup, ['--days', '30', '--yes'])
        
        assert result.exit_code == 0
        assert '✅ Cleaned up 150 old metric records' in result.output
        assert 'older than 30 days' in result.output
        
        # Verify cleanup was called with correct days
        mock_repo.cleanup_old_data.assert_called_once_with(30)
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_cleanup_delete_all(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test cleanup command to delete all data (days=0)."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "postgresql"}}
        
        # Mock repository cleanup
        mock_repo.cleanup_old_data.return_value = 500
        
        result = runner.invoke(cleanup, ['--days', '0', '--yes'])
        
        assert result.exit_code == 0
        assert '✅ Cleaned up 500 old metric records' in result.output
        assert 'older than 0 days' in result.output
        
        # Verify cleanup was called with 0 days
        mock_repo.cleanup_old_data.assert_called_once_with(0)
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_cleanup_fallback_config(self, mock_get_modules, mock_find_profile, runner, mock_persistence_modules):
        """Test cleanup command with fallback config when main config not found."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock no config found
        mock_find_profile.return_value = None
        
        # Mock repository cleanup
        mock_repo.cleanup_old_data.return_value = 75
        
        result = runner.invoke(cleanup, ['--days', '7', '--yes'])
        
        assert result.exit_code == 0
        assert '✅ Cleaned up 75 old metric records' in result.output
        
        # Verify fallback config was used
        mock_get_modules.assert_called_once()
    
    def test_cleanup_confirmation_prompt(self, runner):
        """Test cleanup command confirmation prompt."""
        # Test that command requires confirmation
        result = runner.invoke(cleanup, ['--days', '30'], input='n\n')
        
        assert result.exit_code == 1  # Aborted
        assert 'Are you sure you want to cleanup old metrics data?' in result.output
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_cleanup_exception_handling(self, mock_get_modules, runner):
        """Test cleanup command exception handling."""
        mock_get_modules.side_effect = Exception("Cleanup failed")
        
        result = runner.invoke(cleanup, ['--days', '30', '--yes'])
        
        assert result.exit_code == 1
        assert '❌ Cleanup failed:' in result.output
        assert 'Cleanup failed' in result.output


class TestStatusCommand:
    """Tests for the metrics status command."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_persistence_modules(self):
        """Mock persistence modules and their dependencies."""
        mock_repo = MagicMock()
        mock_config_class = MagicMock()
        mock_audit_reporter = MagicMock()
        mock_ingestion_tracker = MagicMock()
        
        return mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker
    
    @pytest.fixture
    def mock_pipeline_run(self):
        """Create a mock pipeline run."""
        mock_run = MagicMock()
        mock_run.name = "Test Pipeline Run"
        mock_run.status = "completed"
        mock_run.started_at = datetime.now() - timedelta(hours=1)
        mock_run.completed_at = datetime.now()
        mock_run.total_records = 1000
        mock_run.successful_records = 950
        mock_run.failed_records = 50
        return mock_run
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_specific_pipeline_run(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules, mock_pipeline_run):
        """Test status command for specific pipeline run."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock repository
        mock_repo.get_pipeline_run.return_value = mock_pipeline_run
        
        result = runner.invoke(status, ['--pipeline-run-id', 'test-run-123'])
        
        assert result.exit_code == 0
        assert 'Pipeline Run: Test Pipeline Run' in result.output
        assert 'Status: completed' in result.output
        assert 'Total Records: 1,000' in result.output
        assert 'Successful: 950' in result.output
        assert 'Failed: 50' in result.output
        assert 'Success Rate: 95.0%' in result.output
        
        # Verify repository was called
        mock_repo.get_pipeline_run.assert_called_once_with('test-run-123')
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_pipeline_run_not_found(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test status command for non-existent pipeline run."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock repository returning None
        mock_repo.get_pipeline_run.return_value = None
        
        result = runner.invoke(status, ['--pipeline-run-id', 'nonexistent-run'])
        
        assert result.exit_code == 0
        assert '❌ Pipeline run not found: nonexistent-run' in result.output
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_pipeline_run_zero_records(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test status command for pipeline run with zero records."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock pipeline run with zero records
        mock_run = MagicMock()
        mock_run.name = "Empty Pipeline Run"
        mock_run.status = "completed"
        mock_run.started_at = datetime.now()
        mock_run.completed_at = None
        mock_run.total_records = 0
        mock_run.successful_records = 0
        mock_run.failed_records = 0
        
        mock_repo.get_pipeline_run.return_value = mock_run
        
        result = runner.invoke(status, ['--pipeline-run-id', 'empty-run'])
        
        assert result.exit_code == 0
        assert 'Total Records: 0' in result.output
        # Should not show success rate for zero records
        assert 'Success Rate:' not in result.output
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_recent_runs_summary(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test status command for recent runs summary."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock recent runs
        mock_run1 = MagicMock()
        mock_run1.name = "Pipeline Run 1"
        mock_run1.status = "completed"
        mock_run1.total_records = 500
        mock_run1.successful_records = 475
        mock_run1.failed_records = 25
        
        mock_run2 = MagicMock()
        mock_run2.name = "Pipeline Run 2"
        mock_run2.status = "failed"
        mock_run2.total_records = 200
        mock_run2.successful_records = 100
        mock_run2.failed_records = 100
        
        mock_repo.get_recent_pipeline_runs.return_value = [mock_run1, mock_run2]
        
        result = runner.invoke(status)
        
        assert result.exit_code == 0
        assert 'Recent Pipeline Runs:' in result.output
        assert 'Pipeline Run 1' in result.output
        assert 'Pipeline Run 2' in result.output
        assert '✅' in result.output  # completed status icon
        assert '❌' in result.output  # failed status icon
        assert '95.0% success' in result.output
        assert '50.0% success' in result.output
        
        # Verify repository was called
        mock_repo.get_recent_pipeline_runs.assert_called_once_with(10)
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.utils.config_loader.load_config')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_no_recent_runs(self, mock_get_modules, mock_load_config, mock_find_profile, runner, mock_persistence_modules):
        """Test status command when no recent runs exist."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock config loading
        mock_find_profile.return_value = "/path/to/config.yaml"
        mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
        
        # Mock empty recent runs
        mock_repo.get_recent_pipeline_runs.return_value = []
        
        result = runner.invoke(status)
        
        assert result.exit_code == 0
        assert 'No recent pipeline runs found' in result.output
    
    @patch('pulsepipe.cli.command.run.find_profile_path')
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_fallback_config(self, mock_get_modules, mock_find_profile, runner, mock_persistence_modules):
        """Test status command with fallback config when main config not found."""
        mock_repo, mock_config_class, mock_audit_reporter, mock_ingestion_tracker = mock_persistence_modules
        mock_get_modules.return_value = (
            lambda config: mock_repo, 
            mock_config_class, 
            mock_audit_reporter, 
            mock_ingestion_tracker
        )
        
        # Mock no config found
        mock_find_profile.return_value = None
        
        # Mock empty recent runs
        mock_repo.get_recent_pipeline_runs.return_value = []
        
        result = runner.invoke(status)
        
        assert result.exit_code == 0
        
        # Verify fallback config was used
        mock_get_modules.assert_called_once()
    
    @patch('pulsepipe.cli.command.metrics._get_persistence_modules')
    def test_status_exception_handling(self, mock_get_modules, runner):
        """Test status command exception handling."""
        mock_get_modules.side_effect = Exception("Status check failed")
        
        result = runner.invoke(status)
        
        assert result.exit_code == 1
        assert '❌ Status failed:' in result.output
        assert 'Status check failed' in result.output


class TestDisplayMetricsTable:
    """Tests for the _display_metrics_table function."""
    
    @pytest.fixture
    def mock_report(self):
        """Create a mock audit report."""
        mock_report = MagicMock()
        mock_report.report_type = "pipeline"
        
        # Mock processing summary
        mock_processing_summary = MagicMock()
        mock_processing_summary.total_records = 1000
        mock_processing_summary.successful_records = 950
        mock_processing_summary.failed_records = 50
        mock_processing_summary.success_rate = 95.0
        mock_processing_summary.failure_rate = 5.0
        mock_processing_summary.avg_processing_time_ms = 125.5
        mock_report.processing_summary = mock_processing_summary
        
        # Mock error breakdown
        mock_report.error_breakdown = {
            "ValidationError": 30,
            "TimeoutError": 15,
            "NetworkError": 5
        }
        
        # Mock recommendations
        mock_report.recommendations = [
            "Increase timeout for network operations",
            "Validate input data before processing"
        ]
        
        # Mock pipeline runs
        mock_run1 = MagicMock()
        mock_run1.name = "Pipeline Run 1"
        mock_run1.status = "completed"
        mock_run1.total_records = 500
        mock_run1.successful_records = 475
        mock_run1.failed_records = 25
        
        mock_run2 = MagicMock()
        mock_run2.name = "Pipeline Run 2"
        mock_run2.status = "failed"
        mock_run2.total_records = 500
        mock_run2.successful_records = 475
        mock_run2.failed_records = 25
        
        mock_report.pipeline_runs = [mock_run1, mock_run2]
        
        return mock_report
    
    def test_display_metrics_table_complete_report(self, mock_report, capsys):
        """Test _display_metrics_table with complete report data."""
        _display_metrics_table(mock_report)
        
        captured = capsys.readouterr()
        output = captured.out
        
        # Check header
        assert 'INGESTION METRICS REPORT - PIPELINE' in output
        
        # Check processing summary
        assert 'Processing Summary:' in output
        assert 'Total Records:' in output and '1,000' in output
        assert 'Successful Records:' in output and '950' in output
        assert 'Failed Records:' in output and '50' in output
        assert 'Success Rate:' in output and '95.0%' in output
        assert 'Failure Rate:' in output and '5.0%' in output
        assert 'Avg Processing Time:' in output and '125.5ms' in output
        
        # Check error breakdown
        assert 'Error Breakdown:' in output
        assert 'ValidationError' in output
        assert 'TimeoutError' in output
        assert 'NetworkError' in output
        
        # Check recommendations
        assert 'Recommendations:' in output
        assert 'Increase timeout for network operations' in output
        assert 'Validate input data before processing' in output
        
        # Check pipeline runs
        assert 'Recent Pipeline Runs (2):' in output
        assert 'Pipeline Run 1' in output
        assert 'Pipeline Run 2' in output
    
    def test_display_metrics_table_no_errors(self, mock_report, capsys):
        """Test _display_metrics_table with no error breakdown."""
        mock_report.error_breakdown = {}
        
        _display_metrics_table(mock_report)
        
        captured = capsys.readouterr()
        output = captured.out
        
        # Should not show error breakdown section
        assert 'Error Breakdown:' not in output
        
        # But should still show other sections
        assert 'Processing Summary:' in output
        assert 'Recommendations:' in output
    
    def test_display_metrics_table_no_recommendations(self, mock_report, capsys):
        """Test _display_metrics_table with no recommendations."""
        mock_report.recommendations = []
        
        _display_metrics_table(mock_report)
        
        captured = capsys.readouterr()
        output = captured.out
        
        # Should not show recommendations section
        assert 'Recommendations:' not in output
        
        # But should still show other sections
        assert 'Processing Summary:' in output
        assert 'Error Breakdown:' in output
    
    def test_display_metrics_table_single_pipeline_run(self, mock_report, capsys):
        """Test _display_metrics_table with single pipeline run."""
        mock_report.pipeline_runs = [mock_report.pipeline_runs[0]]  # Only one run
        
        _display_metrics_table(mock_report)
        
        captured = capsys.readouterr()
        output = captured.out
        
        # Should not show recent pipeline runs section for single run
        assert 'Recent Pipeline Runs' not in output
    
    def test_display_metrics_table_error_breakdown_percentages(self, mock_report, capsys):
        """Test _display_metrics_table error breakdown percentage calculations."""
        _display_metrics_table(mock_report)
        
        captured = capsys.readouterr()
        output = captured.out
        
        # Check that percentages are calculated correctly
        # ValidationError: 30/50 = 60%
        # TimeoutError: 15/50 = 30%
        # NetworkError: 5/50 = 10%
        assert '60.0%' in output  # ValidationError percentage
        assert '30.0%' in output  # TimeoutError percentage
        assert '10.0%' in output  # NetworkError percentage


class TestLazyLoadingFunction:
    """Tests for the _get_persistence_modules lazy loading function."""
    
    def test_get_persistence_modules_import(self):
        """Test that _get_persistence_modules imports modules correctly."""
        from pulsepipe.cli.command.metrics import _get_persistence_modules
        
        # Call the function to trigger imports
        result = _get_persistence_modules()
        
        # Should return a tuple of 4 items
        assert len(result) == 4
        
        # Each item should be callable (imported modules/classes)
        for item in result:
            assert callable(item)


class TestEdgeCasesAndErrorPaths:
    """Tests for edge cases and error paths to improve coverage."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    def test_export_command_with_all_options(self, runner):
        """Test export command with all possible options."""
        with patch('pulsepipe.cli.command.metrics._get_persistence_modules') as mock_get_modules:
            mock_repo = MagicMock()
            mock_get_modules.return_value = (
                lambda config: mock_repo,
                MagicMock(),
                MagicMock(),
                MagicMock()
            )
            
            # Mock tracker
            tracker_instance = MagicMock()
            mock_get_modules.return_value[3].return_value = tracker_instance
            
            result = runner.invoke(export, [
                '--pipeline-run-id', 'test-run',
                '--format', 'csv',
                '--output', '/tmp/test.csv',
                '--days', '10',
                '--include-details'
            ])
            
            assert result.exit_code == 0
    
    def test_analyze_command_with_all_options(self, runner):
        """Test analyze command with all possible options."""
        with patch('pulsepipe.cli.command.metrics._get_persistence_modules') as mock_get_modules:
            with patch('pulsepipe.cli.command.run.find_profile_path') as mock_find_profile:
                with patch('pulsepipe.utils.config_loader.load_config') as mock_load_config:
                    mock_repo = MagicMock()
                    mock_get_modules.return_value = (
                        lambda config: mock_repo,
                        MagicMock(),
                        MagicMock(),
                        MagicMock()
                    )
                    
                    mock_find_profile.return_value = "/path/to/config.yaml"
                    mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
                    
                    # Mock reporter
                    reporter_instance = MagicMock()
                    mock_report = MagicMock()
                    mock_report.to_json.return_value = '{"test": "data"}'
                    reporter_instance.generate_summary_report.return_value = mock_report
                    mock_get_modules.return_value[2].return_value = reporter_instance
                    
                    result = runner.invoke(analyze, [
                        '--pipeline-run-id', 'test-run',
                        '--days', '14',
                        '--format', 'json'
                    ])
                    
                    assert result.exit_code == 0
    
    def test_status_command_tail_option(self, runner):
        """Test status command with tail option (not implemented but should handle gracefully)."""
        with patch('pulsepipe.cli.command.metrics._get_persistence_modules') as mock_get_modules:
            with patch('pulsepipe.cli.command.run.find_profile_path') as mock_find_profile:
                with patch('pulsepipe.utils.config_loader.load_config') as mock_load_config:
                    mock_repo = MagicMock()
                    mock_get_modules.return_value = (
                        lambda config: mock_repo,
                        MagicMock(),
                        MagicMock(),
                        MagicMock()
                    )
                    
                    mock_find_profile.return_value = "/path/to/config.yaml"
                    mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
                    
                    # Mock empty runs
                    mock_repo.get_recent_pipeline_runs.return_value = []
                    
                    result = runner.invoke(status, ['--tail'])
                    
                    assert result.exit_code == 0
    
    def test_status_running_pipeline_icon(self, runner):
        """Test status command with running pipeline (🔄 icon)."""
        with patch('pulsepipe.cli.command.metrics._get_persistence_modules') as mock_get_modules:
            with patch('pulsepipe.cli.command.run.find_profile_path') as mock_find_profile:
                with patch('pulsepipe.utils.config_loader.load_config') as mock_load_config:
                    mock_repo = MagicMock()
                    mock_get_modules.return_value = (
                        lambda config: mock_repo,
                        MagicMock(),
                        MagicMock(),
                        MagicMock()
                    )
                    
                    mock_find_profile.return_value = "/path/to/config.yaml"
                    mock_load_config.return_value = {"persistence": {"type": "sqlite"}}
                    
                    # Mock running pipeline
                    mock_run = MagicMock()
                    mock_run.name = "Running Pipeline"
                    mock_run.status = "running"
                    mock_run.total_records = 100
                    mock_run.successful_records = 80
                    mock_run.failed_records = 20
                    
                    mock_repo.get_recent_pipeline_runs.return_value = [mock_run]
                    
                    result = runner.invoke(status)
                    
                    assert result.exit_code == 0
                    assert '🔄' in result.output  # running status icon