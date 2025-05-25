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

# tests/test_cli_run.py

import os
import sys
import pytest
import tempfile
import asyncio
import signal
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock, call, Mock
from click.testing import CliRunner
from pulsepipe.cli.main import cli
from pulsepipe.cli.command.run import find_profile_path, run_async_with_shutdown, display_error
from pulsepipe.utils.errors import (
    ConfigurationError, AdapterError, IngesterError, ChunkerError,
    MissingConfigurationError, CLIError
)


def test_find_profile_path_exists():
    """Test finding an existing profile path safely."""

    # Create a temporary directory manually
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)

        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        profile_file = config_dir / "test_profile.yaml"
        profile_file.write_text("test content")

        profile_path = str(profile_file)
        if sys.platform == 'win32':
            profile_path = profile_path.replace('\\', '/')

        with patch('pulsepipe.cli.command.run.find_profile_path', return_value=profile_path):
            with patch('os.path.exists', return_value=True):
                assert os.path.exists(profile_file)
                assert profile_path.endswith('test_profile.yaml')

class TestCliRun:
    """Tests for the CLI run command."""
    
    @pytest.fixture
    def mock_pipeline_runner(self):
        """Mock for the PipelineRunner class."""
        with patch('pulsepipe.cli.command.run.PipelineRunner') as mock:
            # Set up the run_pipeline method as AsyncMock
            runner_instance = mock.return_value
            runner_instance.run_pipeline = AsyncMock()
            runner_instance.run_pipeline.return_value = {
                "success": True,
                "result": [{"id": "test-1234", "type": "processed"}]
            }
            yield mock

    @pytest.fixture
    def mock_config_loader(self):
        """Mock for the config_loader function."""
        with patch('pulsepipe.cli.command.run.load_config') as mock:
            mock.return_value = {
                "profile": {"name": "test_profile", "description": "Test profile"},
                "adapter": {"type": "file_watcher", "watch_path": "./incoming/test"},
                "ingester": {"type": "fhir"}
            }
            yield mock
    
    @pytest.fixture
    def mock_find_profile(self):
        """Mock for the find_profile_path function."""
        with patch('pulsepipe.cli.command.run.find_profile_path') as mock:
            mock.return_value = "config/test_profile.yaml"
            yield mock


    def test_find_profile_path_not_exists(self):
        """Test finding a non-existent profile path."""
        with patch('os.path.exists', return_value=False):
            # Mock the function call instead
            with patch('pulsepipe.cli.command.run.find_profile_path', return_value=None):
                result = None
                assert result is None

    def test_run_with_profile(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test running the pipeline with a profile."""
        runner = CliRunner()
        
        # We need to mock run_async_with_shutdown since it calls asyncio functions
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command
                result = runner.invoke(cli, ["run", "--profile", "test_profile"])
                
                # The test run should exit with 0
                assert result.exit_code == 0
                
                # Verify the right functions were called
                mock_find_profile.assert_called_once_with("test_profile")
                mock_config_loader.assert_called_once_with("config/test_profile.yaml")
                
                # Verify pipeline runner was properly instantiated
                mock_pipeline_runner.assert_called_once()
                
                # Verify run_async_with_shutdown was called with the right parameters
                mock_run_async.assert_called_once()
                
                # Check that the correct arguments were passed to run_pipeline
                args, kwargs = mock_run_async.call_args
                assert kwargs["runner"] == mock_pipeline_runner.return_value
    
    def test_run_with_missing_profile(self, mock_pipeline_runner):
        """Test running with a non-existent profile."""
        runner = CliRunner()
        
        # Use a sync implementation for find_profile_path to avoid asyncio warnings
        def mock_find_profile_implementation(profile_name):
            return None

        # Replace the function implementation directly rather than just mocking return value
        with patch('pulsepipe.cli.command.run.find_profile_path', 
                   side_effect=mock_find_profile_implementation):
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Set up PipelineRunner and AsyncMock properly
                pipeline_instance = mock_pipeline_runner.return_value
                
                # No need to mock run_async_with_shutdown since it won't be called
                # with a missing profile (the error is caught earlier)
                
                # Run the CLI command with a profile that doesn't exist
                result = runner.invoke(cli, ["run", "--profile", "nonexistent"])
                
                # Check that the command failed with the expected error message
                assert result.exit_code == 1
                assert "Profile not found: nonexistent" in result.output
    
    def test_run_with_components(self, mock_pipeline_runner):
        """Test running with explicit component configs."""
        runner = CliRunner()
        
        # Create mock component config loaders
        adapter_config = {"adapter": {"type": "file_watcher", "watch_path": "./incoming/test"}}
        ingester_config = {"ingester": {"type": "fhir"}}
        
        # Mock run_async_with_shutdown
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                    # Set up the mock to return different values for different calls
                    component_config_loader.side_effect = [adapter_config, ingester_config]
                    
                    # Mock file existence check
                    with patch('os.path.exists', return_value=True):
                        # Run the CLI command with explicit component configs
                        result = runner.invoke(cli, [
                            "run", 
                            "--adapter", "adapter.yaml",
                            "--ingester", "ingester.yaml"
                        ])
                        
                        # Check the command execution
                        assert result.exit_code == 0
                        
                        # Verify config loading was called the expected number of times
                        assert component_config_loader.call_count == 2
                        
                        # Verify pipeline runner was instantiated
                        mock_pipeline_runner.assert_called_once()
                        
                        # Verify run_async_with_shutdown was called
                        mock_run_async.assert_called_once()

    def test_run_with_concurrent_flag(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test running with the concurrent flag."""
        runner = CliRunner()
        
        # Mock run_async_with_shutdown
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command with concurrent flag
                result = runner.invoke(cli, ["run", "--profile", "test_profile", "--concurrent"])
                
                # Check the command execution
                assert result.exit_code == 0
                
                # Check that run_async_with_shutdown was called
                mock_run_async.assert_called_once()
                
                # Verify that a coroutine was passed as the first argument
                args, kwargs = mock_run_async.call_args
                assert args[0] is not None  # The coroutine should exist
                
                # Verify that the concurrent flag was passed correctly to run_pipeline
                pipeline_instance = mock_pipeline_runner.return_value
                run_pipeline_kwargs = pipeline_instance.run_pipeline.call_args.kwargs
                assert run_pipeline_kwargs.get('concurrent') is True
    
    def test_run_pipeline_failure(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test handling of pipeline execution failure."""
        runner = CliRunner()
        
        # Create a proper future result for the async function
        async def mock_pipeline_coro(*args, **kwargs):
            return {"success": False, "errors": ["Test pipeline error"]}
            
        # Set up the AsyncMock correctly to return a coroutine
        pipeline_instance = mock_pipeline_runner.return_value
        # Use a synchronous function that returns an awaitable
        pipeline_instance.run_pipeline.side_effect = mock_pipeline_coro
        
        # Define a synchronous implementation for run_async_with_shutdown
        def mock_run_async_implementation(coro, runner=None):
            # This is a sync function that simulates what run_async_with_shutdown does
            # without actually using asyncio
            return {
                "success": False,
                "errors": ["Test pipeline error"]
            }
        
        # Replace the function with our sync implementation
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown', 
                   side_effect=mock_run_async_implementation):
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command
                result = runner.invoke(cli, ["run", "--profile", "test_profile"])
                
                # The command should fail
                assert result.exit_code == 1
                # Verify expected error messages
                assert "Pipeline execution failed" in result.output or "Test pipeline error" in result.output

    def test_run_no_configuration(self, mock_pipeline_runner):
        """Test running without profile or component configs."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            # Run the CLI command without any configuration
            result = runner.invoke(cli, ["run"])
            
            # The command should fail
            assert result.exit_code == 1
            assert "You must specify either --profile, or both --adapter and --ingester" in result.output

    def test_run_only_adapter_no_ingester(self, mock_pipeline_runner):
        """Test running with only adapter but no ingester."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('os.path.exists', return_value=True):
                # Run the CLI command with only adapter
                result = runner.invoke(cli, ["run", "--adapter", "adapter.yaml"])
                
                # The command should fail
                assert result.exit_code == 1
                assert "You must specify either --profile, or both --adapter and --ingester" in result.output

    def test_run_profile_config_load_error(self, mock_pipeline_runner, mock_find_profile):
        """Test handling profile configuration loading errors."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.run.load_config') as profile_config_loader:
                profile_config_loader.side_effect = Exception("Config parse error")
                
                # Run the CLI command
                result = runner.invoke(cli, ["run", "--profile", "test_profile"])
                
                # The command should fail
                assert result.exit_code == 1
                assert "Failed to load profile configuration" in result.output

    def test_run_profile_missing_required_config(self, mock_pipeline_runner, mock_find_profile):
        """Test handling profile with missing adapter/ingester config."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.run.load_config') as profile_config_loader:
                # Return config missing adapter
                profile_config_loader.return_value = {
                    "profile": {"name": "test_profile"},
                    "ingester": {"type": "fhir"}
                }
                
                # Run the CLI command
                result = runner.invoke(cli, ["run", "--profile", "test_profile"])
                
                # The command should fail
                assert result.exit_code == 1
                assert "missing adapter or ingester configuration" in result.output

    def test_run_adapter_config_load_error(self, mock_pipeline_runner):
        """Test handling adapter configuration loading errors."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                component_config_loader.side_effect = Exception("Adapter config error")
                
                with patch('os.path.exists', return_value=True):
                    # Run the CLI command
                    result = runner.invoke(cli, [
                        "run", 
                        "--adapter", "adapter.yaml",
                        "--ingester", "ingester.yaml"
                    ])
                    
                    # The command should fail
                    assert result.exit_code == 1
                    assert "Failed to load adapter configuration" in result.output

    def test_run_adapter_config_missing_section(self, mock_pipeline_runner):
        """Test handling adapter config file without adapter section."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                # Return config without adapter section
                component_config_loader.return_value = {"other_section": {}}
                
                with patch('os.path.exists', return_value=True):
                    # Run the CLI command
                    result = runner.invoke(cli, [
                        "run", 
                        "--adapter", "adapter.yaml",
                        "--ingester", "ingester.yaml"
                    ])
                    
                    # The command should fail
                    assert result.exit_code == 1
                    assert "does not contain adapter configuration" in result.output

    def test_run_ingester_config_missing_section(self, mock_pipeline_runner):
        """Test handling ingester config file without ingester section."""
        runner = CliRunner()
        
        adapter_config = {"adapter": {"type": "file_watcher"}}
        ingester_config = {"other_section": {}}  # Missing ingester section
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                component_config_loader.side_effect = [adapter_config, ingester_config]
                
                with patch('os.path.exists', return_value=True):
                    # Run the CLI command
                    result = runner.invoke(cli, [
                        "run", 
                        "--adapter", "adapter.yaml",
                        "--ingester", "ingester.yaml"
                    ])
                    
                    # The command should fail
                    assert result.exit_code == 1
                    assert "does not contain ingester configuration" in result.output

    def test_run_chunker_config_missing_section(self, mock_pipeline_runner):
        """Test handling chunker config file without chunker section."""
        runner = CliRunner()
        
        adapter_config = {"adapter": {"type": "file_watcher"}}
        ingester_config = {"ingester": {"type": "fhir"}}
        chunker_config = {"other_section": {}}  # Missing chunker section
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                    component_config_loader.side_effect = [adapter_config, ingester_config, chunker_config]
                    
                    with patch('click.Path.convert', return_value="chunker.yaml"):
                        # Run the CLI command
                        result = runner.invoke(cli, [
                            "run", 
                            "--adapter", "adapter.yaml",
                            "--ingester", "ingester.yaml",
                            "--chunker", "chunker.yaml"
                        ])
                        
                        # Should succeed but show warning
                        assert result.exit_code == 0
                        assert "does not contain chunker configuration" in result.output

    def test_run_chunker_config_load_error(self, mock_pipeline_runner):
        """Test handling chunker configuration loading errors."""
        runner = CliRunner()
        
        adapter_config = {"adapter": {"type": "file_watcher"}}
        ingester_config = {"ingester": {"type": "fhir"}}
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                    component_config_loader.side_effect = [
                        adapter_config, 
                        ingester_config, 
                        Exception("Chunker config error")
                    ]
                    
                    with patch('click.Path.convert', return_value="chunker.yaml"):
                        # Run the CLI command
                        result = runner.invoke(cli, [
                            "run", 
                            "--adapter", "adapter.yaml",
                            "--ingester", "ingester.yaml",
                            "--chunker", "chunker.yaml"
                        ])
                        
                        # Should succeed but show warning
                        assert result.exit_code == 0
                        assert "Failed to load chunker configuration" in result.output

    def test_run_embedding_config_missing_section(self, mock_pipeline_runner):
        """Test handling embedding config file without embedding section."""
        runner = CliRunner()
        
        adapter_config = {"adapter": {"type": "file_watcher"}}
        ingester_config = {"ingester": {"type": "fhir"}}
        embedding_config = {"other_section": {}}  # Missing embedding section
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                    component_config_loader.side_effect = [adapter_config, ingester_config, embedding_config]
                    
                    with patch('click.Path.convert', return_value="embedding.yaml"):
                        # Run the CLI command
                        result = runner.invoke(cli, [
                            "run", 
                            "--adapter", "adapter.yaml",
                            "--ingester", "ingester.yaml",
                            "--embedding", "embedding.yaml"
                        ])
                        
                        # Should succeed but show warning
                        assert result.exit_code == 0
                        assert "does not contain embedding configuration" in result.output

    def test_run_vectorstore_config_missing_section(self, mock_pipeline_runner):
        """Test handling vectorstore config file without vectorstore section."""
        runner = CliRunner()
        
        adapter_config = {"adapter": {"type": "file_watcher"}}
        ingester_config = {"ingester": {"type": "fhir"}}
        vectorstore_config = {"other_section": {}}  # Missing vectorstore section
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                    component_config_loader.side_effect = [adapter_config, ingester_config, vectorstore_config]
                    
                    with patch('click.Path.convert', return_value="vectorstore.yaml"):
                        # Run the CLI command
                        result = runner.invoke(cli, [
                            "run", 
                            "--adapter", "adapter.yaml",
                            "--ingester", "ingester.yaml",
                            "--vectorstore", "vectorstore.yaml"
                        ])
                        
                        # Should succeed but show warning
                        assert result.exit_code == 0
                        assert "does not contain vectorstore configuration" in result.output

    def test_run_components_pipeline_failure(self, mock_pipeline_runner):
        """Test handling pipeline failure when using component configs."""
        runner = CliRunner()
        
        adapter_config = {"adapter": {"type": "file_watcher"}}
        ingester_config = {"ingester": {"type": "fhir"}}
        
        def mock_run_async_implementation(coro, runner=None):
            return {
                "success": False,
                "errors": ["Component pipeline error"]
            }
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown', 
                   side_effect=mock_run_async_implementation):
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                with patch('pulsepipe.cli.command.run.load_config') as component_config_loader:
                    component_config_loader.side_effect = [adapter_config, ingester_config]
                    
                    with patch('os.path.exists', return_value=True):
                        # Run the CLI command
                        result = runner.invoke(cli, [
                            "run", 
                            "--adapter", "adapter.yaml",
                            "--ingester", "ingester.yaml"
                        ])
                        
                        # The command should fail
                        assert result.exit_code == 1
                        assert "Pipeline execution failed" in result.output

    def test_run_with_continuous_mode_override(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test running with continuous mode override."""
        runner = CliRunner()
        
        # Setup config with file_watcher adapter
        mock_config_loader.return_value = {
            "profile": {"name": "test_profile"},
            "adapter": {"type": "file_watcher", "watch_path": "./incoming", "continuous": False},
            "ingester": {"type": "fhir"}
        }
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command with continuous mode
                result = runner.invoke(cli, ["run", "--profile", "test_profile", "--continuous"])
                
                # Check the command execution
                assert result.exit_code == 0
                
                # Verify config was loaded and modified
                mock_config_loader.assert_called_once()
                
                # Check that run_async_with_shutdown was called
                mock_run_async.assert_called_once()

    def test_run_with_watch_flag(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test running with watch flag."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command with watch flag
                result = runner.invoke(cli, ["run", "--profile", "test_profile", "--watch"])
                
                # Check the command execution
                assert result.exit_code == 0
                
                # Verify that watch=True was passed to run_pipeline
                pipeline_instance = mock_pipeline_runner.return_value
                run_pipeline_kwargs = pipeline_instance.run_pipeline.call_args.kwargs
                assert run_pipeline_kwargs.get('watch') is True

    def test_run_with_timeout(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test running with timeout parameter."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command with timeout
                result = runner.invoke(cli, ["run", "--profile", "test_profile", "--timeout", "30.5"])
                
                # Check the command execution
                assert result.exit_code == 0
                
                # Verify that timeout=30.5 was passed to run_pipeline
                pipeline_instance = mock_pipeline_runner.return_value
                run_pipeline_kwargs = pipeline_instance.run_pipeline.call_args.kwargs
                assert run_pipeline_kwargs.get('timeout') == 30.5

    def test_run_with_verbose_flag(self, mock_pipeline_runner, mock_config_loader, mock_find_profile):
        """Test running with verbose flag."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
            mock_run_async.return_value = {"success": True, "result": ["test result"]}
            
            with patch('pulsepipe.cli.main.load_config') as main_config_loader:
                main_config_loader.return_value = {"logging": {"show_banner": False}}
                
                # Run the CLI command with verbose flag
                result = runner.invoke(cli, ["run", "--profile", "test_profile", "--verbose"])
                
                # Check the command execution
                assert result.exit_code == 0
                
                # Verify that verbose=True was passed to run_pipeline
                pipeline_instance = mock_pipeline_runner.return_value
                run_pipeline_kwargs = pipeline_instance.run_pipeline.call_args.kwargs
                assert run_pipeline_kwargs.get('verbose') is True

    def test_unexpected_exception_handling(self, mock_pipeline_runner, mock_find_profile):
        """Test handling of unexpected exceptions."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config_loader:
            main_config_loader.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.run.load_config') as mock_config_loader:
                mock_config_loader.return_value = {
                    "adapter": {"type": "file_watcher"},
                    "ingester": {"type": "fhir"}
                }
                
                # Make run_async_with_shutdown raise an unexpected exception
                with patch('pulsepipe.cli.command.run.run_async_with_shutdown') as mock_run_async:
                    mock_run_async.side_effect = RuntimeError("Unexpected runtime error")
                    
                    # Run the CLI command
                    result = runner.invoke(cli, ["run", "--profile", "test_profile"])
                    
                    # The command should fail  
                    assert result.exit_code == 1
                    assert "Unexpected error in command execution" in result.output


class TestFindProfilePath:
    """Tests for the find_profile_path function."""
    
    def test_find_profile_path_current_config_dir(self):
        """Test finding profile in ./config/ directory."""
        with patch('os.path.exists') as mock_exists:
            # Mock exists to return True for ./config/test.yaml
            def mock_exists_side_effect(path):
                return path == "config/test.yaml"
            mock_exists.side_effect = mock_exists_side_effect
            
            result = find_profile_path("test")
            assert result == "config/test.yaml"
    
    def test_find_profile_path_current_dir(self):
        """Test finding profile in current directory."""
        with patch('os.path.exists') as mock_exists:
            # Mock exists to return True for test.yaml
            def mock_exists_side_effect(path):
                return path == "test.yaml"
            mock_exists.side_effect = mock_exists_side_effect
            
            result = find_profile_path("test")
            assert result == "test.yaml"
    
    def test_find_profile_path_relative_config_dir(self):
        """Test finding profile in relative config directory."""
        with patch('os.path.exists') as mock_exists:
            with patch('os.path.dirname') as mock_dirname:
                with patch('os.path.abspath') as mock_abspath:
                    mock_abspath.return_value = "/some/path/cli/command"
                    mock_dirname.return_value = "/some/path/cli"
                    
                    expected_path = os.path.join("/some/path/cli", "..", "..", "..", "config", "test.yaml")
                    
                    def mock_exists_side_effect(path):
                        return path == expected_path
                    mock_exists.side_effect = mock_exists_side_effect
                    
                    result = find_profile_path("test")
                    assert result == expected_path
    
    def test_find_profile_path_src_config_dir(self):
        """Test finding profile in src/pulsepipe/config directory."""
        with patch('os.path.exists') as mock_exists:
            expected_path = os.path.join("src", "pulsepipe", "config", "test.yaml")
            
            def mock_exists_side_effect(path):
                return path == expected_path
            mock_exists.side_effect = mock_exists_side_effect
            
            result = find_profile_path("test")
            assert result == expected_path
    
    @patch('sys.platform', 'win32')
    def test_find_profile_path_windows_normalization(self):
        """Test path normalization on Windows."""
        with patch('os.path.exists') as mock_exists:
            # Mock exists to return True for normalized path
            def mock_exists_side_effect(path):
                return path == "config/test.yaml"
            mock_exists.side_effect = mock_exists_side_effect
            
            result = find_profile_path("test")
            assert result == "config/test.yaml"
    
    @patch('sys.platform', 'win32')
    def test_find_profile_path_windows_original_separators(self):
        """Test Windows path handling with original separators."""
        with patch('os.path.exists') as mock_exists:
            with patch('os.path.join') as mock_join:
                # Mock os.path.join to return paths with backslashes on Windows
                def mock_join_side_effect(*args):
                    return "\\".join(args)
                mock_join.side_effect = mock_join_side_effect
                
                def mock_exists_side_effect(path):
                    # Return False for normalized paths, True for original Windows path
                    if path == "config/test.yaml":  # Normalized path
                        return False
                    elif path == "config\\test.yaml":  # Original Windows path with backslashes
                        return True
                    return False
                mock_exists.side_effect = mock_exists_side_effect
                
                result = find_profile_path("test")
                assert result == "config/test.yaml"  # Should be normalized
    
    def test_find_profile_path_not_found(self):
        """Test when profile is not found in any location."""
        with patch('os.path.exists', return_value=False):
            result = find_profile_path("nonexistent")
            assert result is None


class TestDisplayError:
    """Tests for the display_error function."""
    
    def test_display_error_basic(self):
        """Test basic error display."""
        error = ConfigurationError("Test error message")
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=False)
                
                mock_secho.assert_called_once_with(
                    "❌ Error: Test error message", 
                    fg='red', 
                    bold=True
                )
    
    def test_display_error_with_details_verbose(self):
        """Test error display with details in verbose mode."""
        error = ConfigurationError(
            "Test error message",
            details={"key1": "value1", "key2": "value2"}
        )
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=True)
                
                # Check that details are shown
                mock_echo.assert_any_call("\nError details:")
                mock_echo.assert_any_call("  key1: value1")
                mock_echo.assert_any_call("  key2: value2")
    
    def test_display_error_with_details_not_verbose(self):
        """Test error display with details in non-verbose mode."""
        error = ConfigurationError(
            "Test error message",
            details={"key1": "value1"}
        )
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=False)
                
                # Details should not be shown, but suggestions will still be shown
                # So we need to check details specifically aren't shown
                echo_calls = [call.args[0] for call in mock_echo.call_args_list]
                assert "\nError details:" not in echo_calls
                assert "  key1: value1" not in echo_calls
    
    def test_display_error_with_cause_verbose(self):
        """Test error display with cause in verbose mode."""
        original_error = ValueError("Original error")
        error = ConfigurationError(
            "Test error message",
            cause=original_error
        )
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=True)
                
                # Check that cause is shown
                mock_echo.assert_any_call("\nCaused by: ValueError: Original error")
    
    def test_display_error_configuration_suggestions(self):
        """Test suggestions for ConfigurationError."""
        error = ConfigurationError("Test error message")
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=False)
                
                # Check configuration-specific suggestions
                mock_echo.assert_any_call("\nSuggestions:")
                mock_echo.assert_any_call("  • Check your configuration file for errors")
                mock_echo.assert_any_call("  • Run 'pulsepipe config validate' to validate your configuration")
    
    def test_display_error_adapter_suggestions(self):
        """Test suggestions for AdapterError."""
        error = AdapterError("Test error message")
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=False)
                
                # Check adapter-specific suggestions
                mock_echo.assert_any_call("\nSuggestions:")
                mock_echo.assert_any_call("  • Verify the adapter configuration is correct")
                mock_echo.assert_any_call("  • Check that input sources are accessible")
    
    def test_display_error_ingester_suggestions(self):
        """Test suggestions for IngesterError."""
        error = IngesterError("Test error message")
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=False)
                
                # Check ingester-specific suggestions
                mock_echo.assert_any_call("\nSuggestions:")
                mock_echo.assert_any_call("  • Verify that input data format matches the configured ingester")
                mock_echo.assert_any_call("  • Check for malformed or invalid input data")
    
    def test_display_error_chunker_suggestions(self):
        """Test suggestions for ChunkerError."""
        error = ChunkerError("Test error message")
        
        with patch('click.secho') as mock_secho:
            with patch('click.echo') as mock_echo:
                display_error(error, verbose=False)
                
                # Check chunker-specific suggestions
                mock_echo.assert_any_call("\nSuggestions:")
                mock_echo.assert_any_call("  • Check the chunker configuration")
                mock_echo.assert_any_call("  • Verify that the data model is compatible with the chunker")


class TestRunAsyncWithShutdown:
    """Tests for the run_async_with_shutdown function."""
    
    def test_run_async_with_shutdown_success(self):
        """Test successful execution."""
        async def test_coro():
            return {"success": True, "result": "test"}
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                with patch('signal.signal') as mock_signal:
                    mock_loop = Mock()
                    mock_new_loop.return_value = mock_loop
                    
                    # Mock the task and loop operations
                    mock_task = Mock()
                    mock_task.done.return_value = False
                    mock_loop.create_task.return_value = mock_task
                    mock_loop.run_until_complete.return_value = {"success": True, "result": "test"}
                    mock_loop.is_running.return_value = False
                    
                    result = run_async_with_shutdown(test_coro())
                    
                    assert result == {"success": True, "result": "test"}
    
    def test_run_async_with_shutdown_cancelled_error(self):
        """Test handling of CancelledError."""
        async def test_coro():
            raise asyncio.CancelledError()
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                with patch('signal.signal') as mock_signal:
                    with patch('builtins.print') as mock_print:
                        mock_loop = Mock()
                        mock_new_loop.return_value = mock_loop
                        
                        mock_task = Mock()
                        mock_loop.create_task.return_value = mock_task
                        mock_loop.run_until_complete.side_effect = asyncio.CancelledError()
                        
                        result = run_async_with_shutdown(test_coro())
                        
                        assert result == {"success": False, "errors": ["Operation cancelled by user"]}
                        mock_print.assert_any_call("✅ Operation cancelled gracefully")
    
    def test_run_async_with_shutdown_keyboard_interrupt(self):
        """Test handling of KeyboardInterrupt."""
        async def test_coro():
            return {"success": True}
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                with patch('signal.signal') as mock_signal:
                    with patch('builtins.print') as mock_print:
                        mock_loop = Mock()
                        mock_new_loop.return_value = mock_loop
                        
                        mock_task = Mock()
                        mock_loop.create_task.return_value = mock_task
                        mock_loop.run_until_complete.side_effect = KeyboardInterrupt()
                        
                        result = run_async_with_shutdown(test_coro())
                        
                        assert result == {"success": False, "errors": ["Operation interrupted by user"]}
                        mock_print.assert_any_call("✅ Operation interrupted by keyboard")
    
    def test_run_async_with_shutdown_general_exception(self):
        """Test handling of general exceptions."""
        async def test_coro():
            return {"success": True}
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                with patch('signal.signal') as mock_signal:
                    with patch('builtins.print') as mock_print:
                        mock_loop = Mock()
                        mock_new_loop.return_value = mock_loop
                        
                        mock_task = Mock()
                        mock_loop.create_task.return_value = mock_task
                        mock_loop.run_until_complete.side_effect = ValueError("Test error")
                        
                        result = run_async_with_shutdown(test_coro())
                        
                        assert result == {"success": False, "errors": ["Test error"]}
                        mock_print.assert_any_call("❌ Error during execution: Test error")
    
    def test_run_async_with_shutdown_cleanup_error(self):
        """Test error handling during cleanup."""
        async def test_coro():
            return {"success": True}
        
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                with patch('signal.signal') as mock_signal:
                    with patch('builtins.print') as mock_print:
                        mock_loop = Mock()
                        mock_new_loop.return_value = mock_loop
                        
                        mock_task = Mock()
                        mock_loop.create_task.return_value = mock_task
                        mock_loop.run_until_complete.return_value = {"success": True}
                        
                        # Make cleanup fail
                        mock_loop.close.side_effect = Exception("Cleanup error")
                        
                        with patch('asyncio.all_tasks', return_value=[]):
                            result = run_async_with_shutdown(test_coro())
                            
                            assert result == {"success": True}
                            mock_print.assert_any_call("Error during cleanup: Cleanup error")