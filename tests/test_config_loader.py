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

# tests/test_config_loader.py

import os
import sys
import pytest
import yaml
from unittest.mock import patch, mock_open, MagicMock

from pulsepipe.utils.config_loader import (
    get_config_dir, find_config_file, load_mapping_config, load_config
)


class TestConfigLoader:
    """Tests for the config loader utilities."""

    @pytest.fixture
    def mock_filesystem(self):
        """Mock filesystem paths and access."""
        with patch('os.path.isdir') as mock_isdir:
            with patch('os.path.exists') as mock_exists:
                with patch('os.path.isabs') as mock_isabs:
                    mock_isdir.return_value = True
                    mock_exists.return_value = True
                    mock_isabs.return_value = False
                    yield mock_exists

    def test_get_config_dir_executable(self):
        """Test get_config_dir when running as executable."""
        with patch('sys.frozen', True, create=True):
            with patch('sys.executable', '/path/to/executable'):
                with patch('os.path.dirname') as mock_dirname:
                    mock_dirname.return_value = '/path/to'
                    with patch('os.path.isdir') as mock_isdir:
                        # First check should find config dir next to executable
                        mock_isdir.return_value = True
                        result = get_config_dir()
                        assert result == '/path/to/config'
                        
                        # Second check should fall back to package dir
                        mock_isdir.return_value = False
                        with patch('os.path.abspath') as mock_abspath:
                            mock_abspath.return_value = '/package/path/config_loader.py'
                            mock_dirname.side_effect = ['/path/to', '/package/path']
                            result = get_config_dir()
                            assert result == '/package/path/../config'

    def test_get_config_dir_development(self):
        """Test get_config_dir in development mode."""
        with patch('os.getcwd') as mock_getcwd:
            mock_getcwd.return_value = '/dev/path'
            with patch('os.path.isdir') as mock_isdir:
                # First check should find config dir in cwd
                mock_isdir.return_value = True
                result = get_config_dir()
                assert result == '/dev/path/config'
                
                # Second check should fall back to package dir
                mock_isdir.return_value = False
                with patch('os.path.abspath') as mock_abspath:
                    mock_abspath.return_value = '/package/path/config_loader.py'
                    with patch('os.path.dirname') as mock_dirname:
                        mock_dirname.return_value = '/package/path'
                        result = get_config_dir()
                        assert result == '/package/path/../config'

    def test_find_config_file_absolute_path(self, mock_filesystem):
        """Test find_config_file with an absolute path."""
        with patch('os.path.isabs') as mock_isabs:
            mock_isabs.return_value = True
            result = find_config_file('/absolute/path/to/config.yaml')
            assert result == '/absolute/path/to/config.yaml'

    def test_find_config_file_explicit_relative_path(self, mock_filesystem):
        """Test find_config_file with an explicit relative path."""
        result = find_config_file('./relative/path/to/config.yaml')
        assert result == './relative/path/to/config.yaml'

    def test_find_config_file_search_locations(self, mock_filesystem):
        """Test find_config_file searching multiple locations."""
        # Make the first location not exist, second one exist
        mock_filesystem.side_effect = [False, True, False, False]
        
        with patch('pulsepipe.utils.config_loader.get_config_dir') as mock_get_config_dir:
            mock_get_config_dir.return_value = '/package/config'
            
            result = find_config_file('config.yaml')
            # Should find it in the second location (current directory)
            assert result == 'config.yaml'

    def test_find_config_file_not_found(self, mock_filesystem):
        """Test find_config_file when no file is found."""
        # Make all locations not exist
        mock_filesystem.return_value = False
        
        with patch('pulsepipe.utils.config_loader.get_config_dir') as mock_get_config_dir:
            mock_get_config_dir.return_value = '/package/config'
            
            result = find_config_file('nonexistent.yaml')
            assert result is None

    def test_load_mapping_config_success(self):
        """Test successful loading of mapping config."""
        test_yaml = """
        mapping:
          key1: value1
          key2: value2
        """
        
        with patch('pulsepipe.utils.config_loader.find_config_file') as mock_find:
            mock_find.return_value = '/path/to/mapping.yaml'
            with patch('builtins.open', mock_open(read_data=test_yaml)):
                result = load_mapping_config('mapping.yaml')
                
                assert result == {'mapping': {'key1': 'value1', 'key2': 'value2'}}

    def test_load_mapping_config_file_not_found(self):
        """Test loading mapping config when file isn't found."""
        with patch('pulsepipe.utils.config_loader.find_config_file') as mock_find:
            mock_find.return_value = None
            
            # Should return empty dict and log warning
            with patch('pulsepipe.utils.config_loader.logger.warning') as mock_warn:
                result = load_mapping_config('nonexistent.yaml')
                
                assert result == {}
                mock_warn.assert_called_once()

    def test_load_mapping_config_unicode_error(self):
        """Test handling of unicode errors in mapping config."""
        with patch('pulsepipe.utils.config_loader.find_config_file') as mock_find:
            mock_find.return_value = '/path/to/mapping.yaml'
            
            # First open raises UnicodeDecodeError, second succeeds
            mock_open_obj = MagicMock()
            mock_open_obj.__enter__.side_effect = [
                UnicodeDecodeError('utf-8', b'', 0, 1, 'test error'),
                MagicMock()
            ]
            
            with patch('builtins.open') as mock_file:
                mock_file.return_value = mock_open_obj
                with patch('yaml.safe_load') as mock_yaml:
                    mock_yaml.return_value = {'key': 'value'}
                    with patch('pulsepipe.utils.config_loader.logger.warning'):
                        
                        result = load_mapping_config('mapping.yaml')
                        
                        # Should try alternative encoding
                        assert mock_file.call_count == 2
                        assert result == {'key': 'value'}

    def test_load_mapping_config_general_error(self):
        """Test handling of general errors in mapping config."""
        with patch('pulsepipe.utils.config_loader.find_config_file') as mock_find:
            mock_find.return_value = '/path/to/mapping.yaml'
            
            with patch('builtins.open', side_effect=Exception('Test error')):
                with patch('pulsepipe.utils.config_loader.logger.error') as mock_error:
                    
                    result = load_mapping_config('mapping.yaml')
                    
                    assert result == {}
                    mock_error.assert_called_once()

    def test_load_config_success(self):
        """Test successful loading of config."""
        test_yaml = """
        profile:
          name: test
        adapter:
          type: file_watcher
        """
        
        # Direct path exists
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            with patch('builtins.open', mock_open(read_data=test_yaml)):
                result = load_config('direct_path.yaml')
                
                assert result == {'profile': {'name': 'test'}, 'adapter': {'type': 'file_watcher'}}

    def test_load_config_find_in_standard_locations(self):
        """Test loading config from standard locations."""
        test_yaml = """
        profile:
          name: test
        adapter:
          type: file_watcher
        """
        
        # Direct path doesn't exist, but found in standard location
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            with patch('pulsepipe.utils.config_loader.find_config_file') as mock_find:
                mock_find.return_value = '/standard/location/config.yaml'
                with patch('builtins.open', mock_open(read_data=test_yaml)):
                    result = load_config('config.yaml')
                    
                    assert result == {'profile': {'name': 'test'}, 'adapter': {'type': 'file_watcher'}}

    def test_load_config_file_not_found(self):
        """Test error when config file isn't found."""
        # Path doesn't exist and not found in standard locations
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            with patch('pulsepipe.utils.config_loader.find_config_file') as mock_find:
                mock_find.return_value = None
                
                with pytest.raises(FileNotFoundError) as excinfo:
                    load_config('nonexistent.yaml')
                
                assert "Config file not found" in str(excinfo.value)

    def test_load_config_unicode_error(self):
        """Test handling of unicode errors in config."""
        # Path exists but has encoding issues
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            # First open raises UnicodeDecodeError, second succeeds
            mock_open_obj = MagicMock()
            mock_open_obj.__enter__.side_effect = [
                UnicodeDecodeError('utf-8', b'', 0, 1, 'test error'),
                MagicMock()
            ]
            
            with patch('builtins.open') as mock_file:
                mock_file.return_value = mock_open_obj
                with patch('yaml.safe_load') as mock_yaml:
                    mock_yaml.return_value = {'key': 'value'}
                    with patch('pulsepipe.utils.config_loader.logger.warning'):
                        
                        result = load_config('config.yaml')
                        
                        # Should try alternative encoding
                        assert mock_file.call_count == 2
                        assert result == {'key': 'value'}

    def test_load_config_unicode_error_all_failed(self):
        """Test handling of unicode errors when all encodings fail."""
        # Path exists but has encoding issues that can't be resolved
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            with patch('builtins.open') as mock_file:
                mock_file.side_effect = UnicodeDecodeError('utf-8', b'', 0, 1, 'test error')
                
                with pytest.raises(FileNotFoundError) as excinfo:
                    load_config('config.yaml')
                
                assert "Could not read config file with any encoding" in str(excinfo.value)

    def test_load_config_general_error(self):
        """Test handling of general errors in config loading."""
        # Path exists but other error occurs
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            with patch('builtins.open', side_effect=Exception('Test error')):
                with pytest.raises(FileNotFoundError) as excinfo:
                    load_config('config.yaml')
                
                assert "Error loading config file" in str(excinfo.value)