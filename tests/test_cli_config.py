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

# tests/test_cli_config.py

import os
import pytest
import yaml
from unittest.mock import patch, MagicMock, mock_open
from click.testing import CliRunner

from pulsepipe.cli.main import cli
from pulsepipe.cli.command.config import config
from pulsepipe.utils.errors import ConfigurationError


class TestCliConfig:
    """Tests for the CLI config command."""
    
    @pytest.fixture
    def mock_config_loader(self):
        """Mock for the config_loader function."""
        with patch('pulsepipe.cli.command.config.load_config') as mock:
            mock.return_value = {
                "profile": {"name": "test_profile", "description": "Test profile"},
                "adapter": {"type": "file_watcher", "watch_path": "./incoming/test"},
                "ingester": {"type": "fhir"},
                "logging": {"level": "INFO", "show_banner": True}
            }
            yield mock
    
    @pytest.fixture
    def mock_os_listdir(self):
        """Mock for os.listdir."""
        with patch('os.listdir') as mock:
            mock.return_value = ["pulsepipe.yaml", "test_profile.yaml", "_internal.yaml"]
            yield mock
    
    def test_config_help(self):
        """Test the config command help text."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config:
            main_config.return_value = {"logging": {"show_banner": False}}
            
            # Run the config command with --help
            result = runner.invoke(cli, ["config", "--help"])
            
            # Check the command execution
            assert result.exit_code == 0
            assert "Manage PulsePipe configuration" in result.output
    
    def test_config_validate_help(self):
        """Test the config validate command shows help text."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config:
            main_config.return_value = {"logging": {"show_banner": False}}
            
            # Run the validate command with --help
            result = runner.invoke(cli, ["config", "validate", "--help"])
            
            # Check the command execution
            assert result.exit_code == 0
            assert "Validate configuration files" in result.output
    
    def test_config_create_profile_help(self):
        """Test the config create-profile command shows help text."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config:
            main_config.return_value = {"logging": {"show_banner": False}}
            
            # Run the create-profile command with --help
            result = runner.invoke(cli, ["config", "create-profile", "--help"])
            
            # Check the command execution
            assert result.exit_code == 0
            assert "Create a unified profile from separate config files" in result.output
    
    def test_config_validate_invalid_profile(self, mock_config_loader):
        """Test the config validate command with an invalid profile."""
        runner = CliRunner()
        
        with patch('os.path.exists', side_effect=lambda path: "nonexistent" not in path):
            with patch('pulsepipe.cli.main.load_config') as main_config:
                main_config.return_value = {"logging": {"show_banner": False}}
                
                # Run the validate command with a non-existent profile
                result = runner.invoke(cli, ["config", "validate", "--profile", "nonexistent"])
                
                # Check the command execution
                assert result.exit_code == 0  # CLI still exits successfully
                assert "❌ Profile not found: nonexistent" in result.output
    
    def test_filewatcher_command_help(self):
        """Test the filewatcher command help text."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config:
            main_config.return_value = {"logging": {"show_banner": False}}
            
            # Run the filewatcher command to get help
            result = runner.invoke(cli, ["config", "filewatcher", "--help"])
            
            # Check the command execution
            assert result.exit_code == 0
            assert "File Watcher bookmark and file management" in result.output
    
    def test_delete_profile_command(self):
        """Test the delete-profile command."""
        runner = CliRunner()
        
        with patch('os.path.exists', return_value=True):
            with patch('pulsepipe.cli.main.load_config') as main_config:
                main_config.return_value = {"logging": {"show_banner": False}}
                
                # Mock os.remove to avoid actual file deletion
                with patch('os.remove') as mock_remove:
                    
                    # Run the delete-profile command with force flag to skip confirmation
                    result = runner.invoke(cli, [
                        "config", "delete-profile",
                        "--name", "test_profile",
                        "--force"
                    ])
                    
                    # Check the command execution
                    assert result.exit_code == 0
                    assert "✅ Deleted profile: test_profile" in result.output
                    
                    # Verify that os.remove was called with the right path
                    mock_remove.assert_called_once()
                    
    def test_filewatcher_list_command(self):
        """Test the filewatcher list command."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.main.load_config') as main_config:
            main_config.return_value = {"logging": {"show_banner": False}}
            
            with patch('pulsepipe.cli.command.config.load_config') as config_load:
                config_load.return_value = {}
                
                # Mock SQLiteBookmarkStore.get_all to return test bookmarks
                with patch('pulsepipe.adapters.file_watcher_bookmarks.sqlite_store.SQLiteBookmarkStore.get_all') as mock_get_all:
                        mock_get_all.return_value = [
                            "/path/to/file1.txt",
                            "/path/to/file2.txt"
                        ]
                        
                        # Run the filewatcher list command
                        result = runner.invoke(cli, ["config", "filewatcher", "list"])
                        
                        # Check the command execution
                        assert result.exit_code == 0
                        assert "📌 Processed Files:" in result.output
                        assert "/path/to/file1.txt" in result.output
                        assert "/path/to/file2.txt" in result.output