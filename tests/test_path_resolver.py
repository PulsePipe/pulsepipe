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

# tests/test_path_resolver.py

"""Unit tests for path resolution utilities."""

import os
import sys
import pytest
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from pulsepipe.utils.path_resolver import (
    expand_path,
    get_app_data_dir,
    ensure_directory_exists,
    get_default_log_path
)


class TestPathResolver(unittest.TestCase):
    """Test cases for path resolution utilities."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        
        # Save the original environment
        self.original_environ = os.environ.copy()
        
        # Set up test environment variables
        os.environ['TEST_VAR'] = 'test_value'
        os.environ['HOME_VAR'] = 'home_value'
        os.environ['PATH_VAR'] = os.path.join(self.temp_dir, 'test_path')
    
    def tearDown(self):
        """Tear down test fixtures."""
        # Restore the original environment
        os.environ.clear()
        os.environ.update(self.original_environ)
        
        # Clean up the temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_expand_path_none_or_empty(self):
        """Test that None or empty paths are returned as is."""
        self.assertIsNone(expand_path(None))
        self.assertEqual("", expand_path(""))
    
    def test_expand_path_home_directory(self):
        """Test expansion of home directory."""
        home_path = os.path.expanduser('~')
        
        # Test with just ~
        self.assertEqual(home_path, expand_path('~'))
        
        # Test with ~/path
        expected_path = os.path.join(home_path, 'test')
        self.assertEqual(expected_path, os.path.normpath(expand_path('~/test')))
    
    def test_expand_windows_env_vars(self):
        """Test expansion of Windows-style environment variables."""
        # Test with %TEST_VAR%
        expected = os.path.abspath('test_value/file.txt')
        self.assertEqual(expected, expand_path('%TEST_VAR%/file.txt'))
        
        # Test with multiple variables
        expected = os.path.abspath('test_value/home_value')
        self.assertEqual(expected, expand_path('%TEST_VAR%/%HOME_VAR%'))
        
        # Test with undefined variable
        expected = os.path.abspath('%UNDEFINED_VAR%/file.txt')
        self.assertEqual(expected, expand_path('%UNDEFINED_VAR%/file.txt'))
    
    def test_expand_unix_env_vars(self):
        """Test expansion of Unix-style environment variables."""
        # Test with $TEST_VAR
        expected = os.path.abspath('test_value/file.txt')
        self.assertEqual(expected, expand_path('$TEST_VAR/file.txt'))
        
        # Test with ${TEST_VAR}
        expected = os.path.abspath('test_value/file.txt')
        self.assertEqual(expected, expand_path('${TEST_VAR}/file.txt'))
        
        # Test with multiple variables
        expected = os.path.abspath('test_value/home_value')
        self.assertEqual(expected, expand_path('$TEST_VAR/$HOME_VAR'))
        
        # Test with undefined variable
        expected = os.path.abspath('$UNDEFINED_VAR/file.txt')
        self.assertEqual(expected, expand_path('$UNDEFINED_VAR/file.txt'))
        
        # Test with undefined variable in braces
        expected = os.path.abspath('${UNDEFINED_VAR}/file.txt')
        self.assertEqual(expected, expand_path('${UNDEFINED_VAR}/file.txt'))
    
    def test_expand_path_mixed_styles(self):
        """Test expansion with mixed Unix and Windows styles."""
        expected = os.path.abspath('test_value/home_value/file.txt')
        self.assertEqual(expected, expand_path('$TEST_VAR/%HOME_VAR%/file.txt'))
        
        expected = os.path.abspath('test_value/home_value/file.txt')
        self.assertEqual(expected, expand_path('%TEST_VAR%/${HOME_VAR}/file.txt'))
    
    def test_expand_relative_path(self):
        """Test that relative paths are converted to absolute."""
        rel_path = 'relative/path'
        abs_path = os.path.abspath(rel_path)
        self.assertEqual(abs_path, expand_path(rel_path))
    
    def test_expand_absolute_path(self):
        """Test that absolute paths remain unchanged."""
        # Create an absolute path based on the test's temporary directory
        abs_path = os.path.join(self.temp_dir, 'absolute/path')
        self.assertEqual(abs_path, expand_path(abs_path))
    
    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific path handling test")
    @patch('sys.platform', 'win32')
    @patch('os.makedirs')  # mock makedirs
    def test_get_app_data_dir_windows(self, mock_makedirs):  # Add mock_makedirs parameter
        """Test app data directory resolution on Windows."""
        with patch.dict(os.environ, {'APPDATA': r'C:\Users\Test\AppData\Roaming'}):
            app_dir = get_app_data_dir('TestApp')
            expected_dir = Path(r'C:\Users\Test\AppData\Roaming\TestApp')
            self.assertEqual(expected_dir, app_dir)
            
            # Test with default app name
            app_dir = get_app_data_dir()
            expected_dir = Path(r'C:\Users\Test\AppData\Roaming\PulsePipe')
            self.assertEqual(expected_dir, app_dir)

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific path handling test")
    @patch('sys.platform', 'win32')
    @patch('os.makedirs')
    def test_get_app_data_dir_windows_fallback(self, mock_makedirs):
        """Test Windows app data fallback when APPDATA is not set."""
        # More thorough approach to ensure APPDATA is properly mocked as empty
        with patch.dict('os.environ', {}, clear=True):  # Clear all environment variables
            with patch('os.path.expanduser', return_value=r'C:\Users\Test'):
                # Directly patch the specific function that reads APPDATA in the implementation
                with patch('pulsepipe.utils.path_resolver.os.environ.get', 
                        side_effect=lambda k, d=None: '' if k == 'APPDATA' else d):
                    app_dir = get_app_data_dir('TestApp')
                    expected_dir = Path(r'C:\Users\Test\TestApp')
                    self.assertEqual(expected_dir, app_dir)
    
    @patch('sys.platform', 'darwin')
    @patch('os.makedirs') 
    def test_get_app_data_dir_macos(self, mock_makedirs):  # Add mock_makedirs parameter
        """Test app data directory resolution on macOS."""
        with patch('os.path.expanduser', return_value='/Users/test'):
            app_dir = get_app_data_dir('TestApp')
            expected_dir = Path('/Users/test/Library/Application Support/TestApp')
            self.assertEqual(expected_dir, app_dir)
    
    @patch('sys.platform', 'linux')
    @patch('os.makedirs')
    def test_get_app_data_dir_linux(self, mock_makedirs):  # Add mock_makedirs parameter
        """Test app data directory resolution on Linux."""
        with patch('os.path.expanduser', return_value='/home/test'):
            app_dir = get_app_data_dir('TestApp')
            expected_dir = Path('/home/test/.local/share/TestApp')
            self.assertEqual(expected_dir, app_dir)
    
    @patch('os.makedirs')
    def test_app_data_dir_creation(self, mock_makedirs):
        """Test that the app data directory is created if it doesn't exist."""
        get_app_data_dir('TestApp')
        mock_makedirs.assert_called_once()
        # Check that exist_ok is True
        self.assertTrue(mock_makedirs.call_args[1]['exist_ok'])
    
    def test_ensure_directory_exists_success(self):
        """Test successful directory creation."""
        test_path = os.path.join(self.temp_dir, 'new_dir/file.txt')
        self.assertTrue(ensure_directory_exists(test_path))
        self.assertTrue(os.path.isdir(os.path.dirname(test_path)))
    
    def test_ensure_directory_exists_file_with_no_dir(self):
        """Test handling a file with no directory component."""
        self.assertTrue(ensure_directory_exists('file.txt'))
    
    @patch('os.makedirs')
    def test_ensure_directory_exists_failure(self, mock_makedirs):
        """Test handling directory creation failure."""
        mock_makedirs.side_effect = PermissionError("Permission denied")
        test_path = os.path.join(self.temp_dir, 'new_dir/file.txt')
        self.assertFalse(ensure_directory_exists(test_path))
    
    @patch('pulsepipe.utils.path_resolver.get_app_data_dir')
    @patch('os.makedirs')
    def test_get_default_log_path(self, mock_makedirs, mock_get_app_data_dir):
        """Test retrieving the default log path."""
        mock_app_dir = MagicMock(spec=Path)
        mock_app_dir.__truediv__.return_value = Path('/fake/app/dir/logs')
        mock_get_app_data_dir.return_value = mock_app_dir
        
        log_path = get_default_log_path()
        expected_path = str(Path('/fake/app/dir/logs/pulsepipe.log'))
        self.assertEqual(expected_path, log_path)
        
        # Check that the log directory is created
        mock_makedirs.assert_called_once()
        self.assertTrue(mock_makedirs.call_args[1]['exist_ok'])


if __name__ == '__main__':
    unittest.main()
