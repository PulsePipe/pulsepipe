"""
Test the Windows mock module's functionality.

This module tests the Windows-specific mock functionality that prevents
file I/O issues on Windows platforms. On non-Windows platforms, these tests 
are automatically skipped.
"""
import os
import sys
import io
import logging
import pytest
from pathlib import Path

# Skip the entire test file on non-Windows platforms
pytestmark = pytest.mark.skipif(sys.platform != 'win32', reason="Windows mock tests only run on Windows")

# Only import the mock module on Windows
if sys.platform == 'win32':
    from tests.windows_mock import (
        enable_windows_mocks, 
        disable_windows_mocks,
        MockFile,
        _virtual_fs,
        _virtual_dirs
    )
else:
    # Create dummy values for non-Windows platforms to avoid import errors
    enable_windows_mocks = lambda: None
    disable_windows_mocks = lambda: None
    MockFile = io.StringIO
    _virtual_fs = {}
    _virtual_dirs = {}

# Tests will only meaningfully test the mock on Windows
# On other platforms, they'll still pass but without using the mock functionality

@pytest.fixture(autouse=True)
def setup_test():
    """Setup and cleanup for tests."""
    # Save original state
    original_platform = sys.platform
    original_env = os.environ.copy()
    
    # Force the test environment variable to simulate pytest
    os.environ['PYTEST_CURRENT_TEST'] = 'test_windows_mock.py'
    
    # Enable mocks if we're on Windows or simulating Windows
    is_windows = sys.platform == 'win32'
    if is_windows:
        enable_windows_mocks()
    
    # Clean virtual FS for test
    _virtual_fs.clear()
    _virtual_dirs.clear()
    
    yield
    
    # Restore original state
    if is_windows:
        disable_windows_mocks()
    
    # Reset environment
    os.environ.clear()
    os.environ.update(original_env)


def test_mock_file_operations():
    """Test basic file operations with the mock."""
    # Only fully test on Windows
    if sys.platform != 'win32':
        pytest.skip("Not on Windows, skipping detailed mock tests")
    
    # Test file writing
    test_file = "/tmp/test_file.txt"
    with open(test_file, "w") as f:
        f.write("Hello, world!")
    
    # Test file exists
    assert os.path.exists(test_file)
    
    # Test file reading
    with open(test_file, "r") as f:
        content = f.read()
    assert content == "Hello, world!"
    
    # Test directory creation
    test_dir = "/tmp/test_dir"
    os.makedirs(test_dir, exist_ok=True)
    assert os.path.isdir(test_dir)
    
    # Test file in directory
    nested_file = os.path.join(test_dir, "nested.txt")
    with open(nested_file, "w") as f:
        f.write("Nested content")
    
    # Test listing directory
    assert "nested.txt" in os.listdir(test_dir)
    
    # Test file removal
    os.remove(nested_file)
    assert not os.path.exists(nested_file)


def test_path_normalization():
    """Test path normalization with the mock."""
    # Test forward slashes are preserved
    path = "/tmp/test/file.txt"
    assert os.path.dirname(path) == "/tmp/test"
    
    # Test backslashes are converted to forward slashes
    if sys.platform == 'win32':
        path = "C:\\tmp\\test\\file.txt"
        assert os.path.dirname(path) == "C:/tmp/test"
    
    # Test path joining
    joined = os.path.join("/tmp", "test", "file.txt")
    assert joined == "/tmp/test/file.txt"
    
    # Test Path objects
    path_obj = Path("/tmp/test/file.txt")
    assert str(path_obj) == "/tmp/test/file.txt"


def test_logging_output():
    """Test logging with the mock."""
    if sys.platform != 'win32':
        pytest.skip("Not on Windows, skipping detailed mock tests")
    
    # Configure a file logger
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    
    # Add a file handler (which will be mocked on Windows)
    log_file = "/tmp/test.log"
    handler = logging.FileHandler(log_file)
    logger.addHandler(handler)
    
    # Log some messages
    logger.info("Test log message")
    logger.warning("Test warning message")
    
    # Close handler
    handler.close()
    
    # The file shouldn't actually be written to disk,
    # but our virtual FS should have it
    assert log_file in _virtual_fs


def test_fixture_access():
    """Test that fixture files are still accessible."""
    # This should work on any platform
    # It verifies that our mock doesn't interfere with test fixtures
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    assert os.path.isdir(fixtures_dir), "Fixtures directory should exist"
    
    # List the fixtures directory
    files = os.listdir(fixtures_dir)
    assert len(files) > 0, "Should have test fixtures"