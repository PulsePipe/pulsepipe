import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from pulsepipe.utils.path_resolver import (
    expand_path,
    get_app_data_dir,
    ensure_directory_exists,
    get_default_log_path
)

class TestExpandPath:
    def test_empty_path(self):
        assert expand_path("") == ""
        assert expand_path(None) is None
    
    def test_home_directory_expansion(self):
        home_dir = os.path.expanduser("~")
        test_path = "~/test/path"
        expected = os.path.join(home_dir, "test/path")
        assert os.path.normpath(expand_path(test_path)) == os.path.normpath(expected)
    
    @patch.dict(os.environ, {"TEST_VAR": "/test/env/path"})
    def test_windows_env_expansion(self):
        test_path = "%TEST_VAR%/subdir"
        expected = "/test/env/path/subdir"
        assert os.path.normpath(expand_path(test_path)) == os.path.normpath(expected)
    
    @patch.dict(os.environ, {"TEST_VAR": "/test/env/path"})
    def test_unix_brace_env_expansion(self):
        test_path = "${TEST_VAR}/subdir"
        expected = "/test/env/path/subdir"
        assert os.path.normpath(expand_path(test_path)) == os.path.normpath(expected)
    
    @patch.dict(os.environ, {"TEST_VAR": "/test/env/path"})
    def test_unix_simple_env_expansion(self):
        test_path = "$TEST_VAR/subdir"
        expected = "/test/env/path/subdir"
        assert os.path.normpath(expand_path(test_path)) == os.path.normpath(expected)
    
    def test_non_existent_env_var(self):
        test_path = "$NONEXISTENT_ENV_VAR/path"
        # Should keep original when env var doesn't exist
        assert expand_path(test_path) == os.path.abspath("$NONEXISTENT_ENV_VAR/path")
    
    def test_relative_to_absolute(self):
        test_path = "relative/path"
        expected = os.path.abspath("relative/path")
        assert expand_path(test_path) == expected
    
    def test_already_absolute(self):
        if sys.platform == 'win32':
            test_path = "C:\\absolute\\path"
        else:
            test_path = "/absolute/path"
        assert expand_path(test_path) == test_path

class TestGetAppDataDir:
    @patch('sys.platform', 'win32')
    @patch.dict(os.environ, {"APPDATA": "C:\\Users\\Test\\AppData\\Roaming"})
    @patch('os.makedirs')
    def test_windows_app_data(self, mock_makedirs):
        app_dir = get_app_data_dir("TestApp")
        expected = Path("C:\\Users\\Test\\AppData\\Roaming") / "TestApp"
        assert app_dir == expected
        mock_makedirs.assert_called_once_with(expected, exist_ok=True)
    
    @patch('sys.platform', 'darwin')
    @patch('os.path.expanduser')
    @patch('os.makedirs')
    def test_macos_app_data(self, mock_makedirs, mock_expanduser):
        mock_expanduser.return_value = "/Users/test"
        app_dir = get_app_data_dir("TestApp")
        expected = Path("/Users/test/Library/Application Support/TestApp")
        assert app_dir == expected
        mock_makedirs.assert_called_once_with(expected, exist_ok=True)
    
    @patch('sys.platform', 'linux')
    @patch('os.path.expanduser')
    @patch('os.makedirs')
    def test_linux_app_data(self, mock_makedirs, mock_expanduser):
        mock_expanduser.return_value = "/home/user"
        app_dir = get_app_data_dir("TestApp")
        expected = Path("/home/user/.local/share/TestApp")
        assert app_dir == expected
        mock_makedirs.assert_called_once_with(expected, exist_ok=True)
    
    @patch('sys.platform', 'win32')
    @patch.dict(os.environ, {}, clear=True)  # Clear APPDATA
    @patch('os.path.expanduser')
    @patch('os.makedirs')
    def test_windows_fallback_to_home(self, mock_makedirs, mock_expanduser):
        mock_expanduser.return_value = "C:\\Users\\Test"
        app_dir = get_app_data_dir("TestApp")
        expected = Path("C:\\Users\\Test") / "TestApp"
        assert app_dir == expected
        mock_makedirs.assert_called_once_with(expected, exist_ok=True)

class TestEnsureDirectoryExists:
    @patch('os.makedirs')
    def test_ensure_dir_exists_success(self, mock_makedirs):
        result = ensure_directory_exists("/test/directory/file.txt")
        assert result is True
        mock_makedirs.assert_called_once_with("/test/directory", exist_ok=True)
    
    @patch('os.makedirs', side_effect=Exception("Permission denied"))
    def test_ensure_dir_exists_failure(self, mock_makedirs):
        result = ensure_directory_exists("/test/directory/file.txt")
        assert result is False
        mock_makedirs.assert_called_once_with("/test/directory", exist_ok=True)
    
    @patch('os.makedirs')
    def test_no_directory_path(self, mock_makedirs):
        result = ensure_directory_exists("file.txt")
        assert result is True
        # Should not try to create a directory for a simple filename
        mock_makedirs.assert_not_called()

class TestGetDefaultLogPath:
    @patch('pulsepipe.utils.path_resolver.get_app_data_dir')
    @patch('os.makedirs')
    def test_default_log_path(self, mock_makedirs, mock_get_app_data_dir):
        mock_app_dir = Path("/app")
        mock_get_app_data_dir.return_value = mock_app_dir
        
        log_path = get_default_log_path()
        expected = str(Path("/app/logs/pulsepipe.log"))
        
        assert log_path == expected
        mock_get_app_data_dir.assert_called_once_with("PulsePipe")
        mock_makedirs.assert_called_once_with(Path("/app/logs"), exist_ok=True)