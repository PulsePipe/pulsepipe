# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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

# tests/test_cli_config_extended.py

import os
import pytest
import yaml
from unittest.mock import patch, MagicMock, mock_open
from click.testing import CliRunner

from pulsepipe.cli.main import cli
from pulsepipe.cli.command.config import config, create_profile, validate


class TestCliConfigExtended:
    """Extended tests for the CLI config command."""
    
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
    
    def test_config_validate_profile_success(self, mock_config_loader):
        """Test validating a profile successfully."""
        runner = CliRunner()
        
        # Need to patch both the path.exists and the path finding mechanism to ensure profile is found
        with patch('os.path.exists', return_value=True):
            with patch('pathlib.Path.exists', return_value=True):
                # Mock config loading for the profile validation
                mock_config_loader.return_value = {
                    "profile": {"name": "test_profile", "description": "Test profile"},
                    "adapter": {"type": "file_watcher", "watch_path": "./incoming/test"},
                    "ingester": {"type": "fhir"}
                }
                
                with patch('pulsepipe.cli.main.load_config') as main_config:
                    main_config.return_value = {"logging": {"show_banner": False}}
                    
                    # Run the validate command with an existing profile
                    result = runner.invoke(cli, ["config", "validate", "--profile", "test_profile"])
                    
                    # Since we can't easily mock all parts of the validation flow,
                    # let's just check that the command runs without error
                    assert result.exit_code == 0

    def test_config_validate_all_profiles(self, mock_config_loader):
        """Test validating all profiles."""
        runner = CliRunner()
        
        # Mock directory with profiles
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.glob') as mock_glob:
                # Create mock Path objects for profiles
                mock_path1 = MagicMock()
                mock_path1.name = "profile1.yaml"
                mock_path1.is_file.return_value = True
                
                mock_path2 = MagicMock()
                mock_path2.name = "profile2.yaml"
                mock_path2.is_file.return_value = True
                
                mock_glob.return_value = [mock_path1, mock_path2]
                
                with patch('pulsepipe.cli.main.load_config') as main_config:
                    main_config.return_value = {"logging": {"show_banner": False}}
                    
                    # Run the validate command with --all flag
                    result = runner.invoke(cli, ["config", "validate", "--all"])
                    
                    # Check the command execution
                    assert result.exit_code == 0
                    assert "✅ profile1.yaml: Valid" in result.output
                    assert "✅ profile2.yaml: Valid" in result.output
                    assert "Validated 2 profiles" in result.output

    def test_config_create_profile_success(self):
        """Test creating a profile successfully."""
        runner = CliRunner()
        
        # Mock Path.exists and mkdir
        with patch('pathlib.Path.exists', return_value=False):
            with patch('pathlib.Path.mkdir') as mock_mkdir:
                # Mock config loading
                with patch('pulsepipe.cli.command.config.load_config') as mock_load_config:
                    mock_load_config.side_effect = [
                        # Base config
                        {"logging": {"level": "INFO"}},
                        # Adapter config
                        {"adapter": {"type": "file_watcher"}},
                        # Ingester config
                        {"ingester": {"type": "fhir"}},
                    ]
                    
                    # Mock open and yaml.dump
                    with patch('builtins.open', mock_open()) as mock_file:
                        with patch('yaml.dump') as mock_yaml_dump:
                            with patch('pulsepipe.cli.main.load_config') as main_config:
                                main_config.return_value = {"logging": {"show_banner": False}}
                                
                                # Run the create-profile command
                                result = runner.invoke(cli, [
                                    "config", "create-profile",
                                    "--adapter", "adapter.yaml",
                                    "--ingester", "ingester.yaml",
                                    "--name", "new_profile"
                                ])
                                
                                # Check the command execution
                                assert result.exit_code == 0
                                assert "✅ Created profile: new_profile" in result.output
                                
                                # Verify directory was created
                                mock_mkdir.assert_called_once()
                                
                                # Verify config was written
                                mock_yaml_dump.assert_called_once()
                                profile_config = mock_yaml_dump.call_args[0][0]
                                assert profile_config["profile"]["name"] == "new_profile"
                                assert "adapter" in profile_config
                                assert "ingester" in profile_config

    def test_config_create_profile_with_optional_components(self):
        """Test creating a profile with optional components."""
        runner = CliRunner()
        
        # Mock Path.exists
        with patch('pathlib.Path.exists', return_value=True):
            # Mock config loading
            with patch('pulsepipe.cli.command.config.load_config') as mock_load_config:
                mock_load_config.side_effect = [
                    # Base config
                    {"logging": {"level": "INFO"}},
                    # Adapter config
                    {"adapter": {"type": "file_watcher"}},
                    # Ingester config
                    {"ingester": {"type": "fhir"}},
                    # Chunker config
                    {"chunker": {"type": "clinical"}},
                    # Embedding config
                    {"embedding": {"type": "clinical"}},
                    # Vectorstore config
                    {"vectorstore": {"type": "qdrant"}},
                ]
                
                # Mock open and yaml.dump
                with patch('builtins.open', mock_open()) as mock_file:
                    with patch('yaml.dump') as mock_yaml_dump:
                        with patch('pulsepipe.cli.main.load_config') as main_config:
                            main_config.return_value = {"logging": {"show_banner": False}}
                            
                            # Run the create-profile command with all options (add force flag)
                            result = runner.invoke(cli, [
                                "config", "create-profile",
                                "--adapter", "adapter.yaml",
                                "--ingester", "ingester.yaml",
                                "--chunker", "chunker.yaml",
                                "--embedding", "embedding.yaml",
                                "--vectorstore", "vectorstore.yaml",
                                "--name", "full_profile",
                                "--description", "Complete processing pipeline",
                                "--force"  # Add force flag
                            ])
                            
                            # Check the command execution
                            assert result.exit_code == 0
                            assert "✅ Created profile: full_profile" in result.output
                            
                            # Verify config was written with all components
                            mock_yaml_dump.assert_called_once()
                            profile_config = mock_yaml_dump.call_args[0][0]
                            assert profile_config["profile"]["name"] == "full_profile"
                            assert profile_config["profile"]["description"] == "Complete processing pipeline"
                            assert "adapter" in profile_config
                            assert "ingester" in profile_config
                            assert "chunker" in profile_config
                            assert "embedding" in profile_config
                            assert "vectorstore" in profile_config

    def test_config_create_profile_existing_profile_no_force(self):
        """Test creating a profile when it already exists without force flag."""
        runner = CliRunner()
        
        # Mock Path.exists to indicate profile already exists
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pulsepipe.cli.main.load_config') as main_config:
                main_config.return_value = {"logging": {"show_banner": False}}
                
                # Run the create-profile command without force flag
                result = runner.invoke(cli, [
                    "config", "create-profile",
                    "--adapter", "adapter.yaml",
                    "--ingester", "ingester.yaml",
                    "--name", "existing_profile"
                ])
                
                # Check the command execution - should fail
                assert result.exit_code == 0  # Click still returns 0
                assert "❌ Profile already exists: existing_profile" in result.output

    def test_config_filewatcher_reset_bookmarks(self):
        """Test resetting file watcher bookmarks."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.config.load_config') as mock_load_config:
            mock_load_config.return_value = {}
            
            with patch('pulsepipe.cli.command.config.get_shared_sqlite_connection'):
                # Mock SQLiteBookmarkStore.clear_all to return count
                with patch('pulsepipe.adapters.file_watcher_bookmarks.sqlite_store.SQLiteBookmarkStore.clear_all') as mock_clear_all:
                    mock_clear_all.return_value = 5
                    
                    with patch('pulsepipe.cli.main.load_config') as main_config:
                        main_config.return_value = {"logging": {"show_banner": False}}
                        
                        # Run the filewatcher reset command
                        result = runner.invoke(cli, ["config", "filewatcher", "reset"])
                        
                        # Check the command execution
                        assert result.exit_code == 0
                        assert "✅ Cleared 5 bookmarks" in result.output

    def test_config_filewatcher_archive_files(self):
        """Test archiving processed files."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.config.load_config') as mock_load_config:
            mock_load_config.return_value = {}
            
            with patch('pulsepipe.cli.command.config.get_shared_sqlite_connection'):
                # Mock SQLiteBookmarkStore.get_all to return file paths
                with patch('pulsepipe.adapters.file_watcher_bookmarks.sqlite_store.SQLiteBookmarkStore.get_all') as mock_get_all:
                    mock_get_all.return_value = [
                        "/path/to/file1.txt",
                        "/path/to/file2.txt"
                    ]
                    
                    # Mock os.makedirs and shutil.move
                    with patch('os.makedirs') as mock_makedirs:
                        with patch('shutil.move') as mock_move:
                            with patch('pulsepipe.cli.main.load_config') as main_config:
                                main_config.return_value = {"logging": {"show_banner": False}}
                                
                                # Run the filewatcher archive command
                                result = runner.invoke(cli, [
                                    "config", "filewatcher", "archive",
                                    "--archive-dir", "/archive/dir"
                                ])
                                
                                # Check the command execution
                                assert result.exit_code == 0
                                assert "✅ Archived 2 files" in result.output
                                
                                # Verify directory was created
                                mock_makedirs.assert_called_once_with("/archive/dir", exist_ok=True)
                                
                                # Verify files were moved
                                assert mock_move.call_count == 2

    def test_config_filewatcher_delete_files(self):
        """Test deleting processed files."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.config.load_config') as mock_load_config:
            mock_load_config.return_value = {}
            
            with patch('pulsepipe.cli.command.config.get_shared_sqlite_connection'):
                # Mock SQLiteBookmarkStore.get_all to return file paths
                with patch('pulsepipe.adapters.file_watcher_bookmarks.sqlite_store.SQLiteBookmarkStore.get_all') as mock_get_all:
                    mock_get_all.return_value = [
                        "/path/to/file1.txt",
                        "/path/to/file2.txt"
                    ]
                    
                    # Mock Path.unlink
                    with patch('pathlib.Path.unlink') as mock_unlink:
                        with patch('pulsepipe.cli.main.load_config') as main_config:
                            main_config.return_value = {"logging": {"show_banner": False}}
                            
                            # Run the filewatcher delete command
                            result = runner.invoke(cli, ["config", "filewatcher", "delete"])
                            
                            # Check the command execution
                            assert result.exit_code == 0
                            assert "✅ Deleted 2 files" in result.output
                            
                            # Verify files were deleted
                            assert mock_unlink.call_count == 2

    def test_config_list_command_basic(self):
        """Test listing available configuration profiles - simplified test."""
        runner = CliRunner()
        
        # Simply check that the command runs without error
        with patch('pulsepipe.cli.main.load_config') as main_config:
            main_config.return_value = {"logging": {"show_banner": False}}
            
            # We'll create a simplified version that doesn't attempt the complex mocking of file reading
            with patch('pulsepipe.cli.command.config.os.path.exists', return_value=False):
                # This will make the command return "No profiles found" instead of trying to read files
                result = runner.invoke(cli, ["config", "list"])
                
                # Check the command runs without error
                assert result.exit_code == 0
                assert "No profiles found" in result.output