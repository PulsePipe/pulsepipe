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

"""
Mock decorators for PulsePipe tests.

These help with Windows compatibility by skipping problematic tests
or applying mocks selectively.
"""

import sys
import os
import functools
import pytest
import logging
import io
from unittest.mock import patch, MagicMock

# Track if we need to apply Windows-specific fixes
IS_WINDOWS = sys.platform == 'win32'
IS_TEST = 'PYTEST_CURRENT_TEST' in os.environ
IS_WINDOWS_TEST = IS_WINDOWS and IS_TEST

# Mock objects for Windows tests
class MockStream(io.StringIO):
    """A StringIO that can't be closed."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._closed = False
        
    def close(self):
        """Prevent actual closing to avoid errors."""
        self._closed = True
        
    @property
    def closed(self):
        """Always report as not closed to prevent errors."""
        return False
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockFileHandler(logging.Handler):
    """A logging handler that doesn't actually write to files."""
    
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        super().__init__()
        self.stream = MockStream()
        self.filename = filename
        self.baseFilename = filename
        self.mode = mode
        self.encoding = encoding
        self._closed = False
    
    def emit(self, record):
        """Write to the mock stream."""
        try:
            msg = self.format(record)
            self.stream.write(msg + '\n')
            self.stream.flush()
        except Exception:
            self.handleError(record)
    
    def close(self):
        """Pretend to close but don't really."""
        self._closed = True
        
    def flush(self):
        """Flush the stream."""
        if hasattr(self, 'stream') and self.stream:
            self.stream.flush()


def windows_safe_test(func):
    """
    Decorator for tests that need special handling on Windows.
    
    This decorator intercepts file operations that might cause
    "I/O operation on closed file" errors on Windows.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not IS_WINDOWS_TEST:
            # On non-Windows platforms, just run the test normally
            return func(*args, **kwargs)
            
        # On Windows, apply patches for file operations
        patches = [
            # Replace FileHandler with mock version
            patch('logging.FileHandler', MockFileHandler),
            
            # Mock _cleanup_file_handlers to be a no-op
            patch('pulsepipe.utils.log_factory.LogFactory._cleanup_file_handlers', 
                  lambda: None),
            
            # Mock close_all to be a no-op
            patch('pulsepipe.utils.log_factory.WindowsSafeFileHandler.close_all', 
                  lambda: None),
        ]
        
        # Apply all the patches
        for p in patches:
            p.start()
            
        try:
            # Run the test with patches applied
            result = func(*args, **kwargs)
            return result
        finally:
            # Clean up patches
            for p in patches:
                p.stop()
    
    return wrapper


def windows_skip_test(reason="Test not supported on Windows"):
    """
    Decorator to skip a test on Windows.
    
    This is useful for tests that just won't work on Windows
    and aren't worth fixing.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if IS_WINDOWS_TEST:
                pytest.skip(reason)
            return func(*args, **kwargs)
        return wrapper
    return decorator