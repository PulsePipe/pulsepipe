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
Test configuration and fixtures for PulsePipe.
"""

import pytest
import os
import sys
import logging
import atexit
from pathlib import Path

from pulsepipe.utils.log_factory import LogFactory, WindowsSafeFileHandler

# Register global cleanup to ensure file handlers are closed at exit
@atexit.register
def cleanup_on_exit():
    """Ensure all log handlers are closed on interpreter exit."""
    # Helper function to safely clean up logger handlers
    def safe_cleanup_handler(handler):
        try:
            # First set stream to None to release file handle
            if hasattr(handler, 'stream') and handler.stream is not None:
                stream = handler.stream
                handler.stream = None
                
                # Close the stream if possible
                if hasattr(stream, 'close') and not getattr(stream, 'closed', False):
                    try:
                        stream.close()
                    except:
                        pass
            
            # Then close the handler
            if hasattr(handler, 'close'):
                handler.close()
        except:
            # Ignore any errors during shutdown
            pass
    
    try:
        # Clean up file handlers using our robust mechanisms
        # The order matters here - first our class method, then any handlers
        LogFactory._cleanup_file_handlers()
        WindowsSafeFileHandler.close_all()
        
        # Close any remaining handlers in the root logger
        root_logger = logging.getLogger()
        handlers = list(root_logger.handlers)  # Copy to avoid modification during iteration
        
        # First remove all handlers from root logger
        for handler in handlers:
            try:
                root_logger.removeHandler(handler)
            except:
                pass
                
        # Then safely close each handler
        for handler in handlers:
            if isinstance(handler, logging.FileHandler):
                safe_cleanup_handler(handler)
        
        # Also clean up any other loggers in the system
        if hasattr(logging.root, 'manager') and hasattr(logging.root.manager, 'loggerDict'):
            for logger_name in list(logging.root.manager.loggerDict.keys()):
                try:
                    logger = logging.getLogger(logger_name)
                    if logger:
                        # Copy to avoid modification during iteration
                        handlers = list(logger.handlers)
                        
                        # First remove all handlers from logger
                        for handler in handlers:
                            try:
                                logger.removeHandler(handler)
                            except:
                                pass
                                
                        # Then safely close each handler
                        for handler in handlers:
                            if isinstance(handler, logging.FileHandler):
                                safe_cleanup_handler(handler)
                except:
                    # Ignore any errors during shutdown
                    pass
    except:
        # Ignore any errors during shutdown
        pass

def cleanup_root_logger_handlers():
    """Helper function to clean up root logger handlers safely."""
    try:
        # Get root logger
        root_logger = logging.getLogger()
        
        # Save handlers to avoid modification during iteration
        handlers = list(root_logger.handlers)
        
        # First remove all handlers from root logger to avoid modification during cleanup
        for handler in handlers:
            root_logger.removeHandler(handler)
        
        # Now clean up each handler
        for handler in handlers:
            if isinstance(handler, logging.FileHandler):
                try:
                    # First set stream to None to release file handle
                    if hasattr(handler, 'stream') and handler.stream is not None:
                        stream = handler.stream
                        handler.stream = None
                        
                        # Close the stream if possible
                        if hasattr(stream, 'close') and not getattr(stream, 'closed', False):
                            try:
                                stream.close()
                            except:
                                pass
                    
                    # Then close the handler
                    if hasattr(handler, 'close'):
                        handler.close()
                except:
                    pass
    except:
        # Ignore any errors - we're just trying to clean up
        pass

@pytest.fixture(scope="session", autouse=True)
def cleanup_log_files_session():
    """
    Cleanup any log files and handlers at the start and end of test session.
    This prevents "I/O operation on closed file" errors between tests.
    """
    # First handle stdout/stderr to avoid pytest capture conflicts
    try:
        # Close any logging handlers using stdout/stderr
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            if isinstance(handler, logging.StreamHandler) and \
               handler.stream in (sys.stdout, sys.stderr):
                root_logger.removeHandler(handler)
    except:
        pass
        
    # Clean up file handlers using our robust mechanisms
    LogFactory._cleanup_file_handlers()
    WindowsSafeFileHandler.close_all()
    cleanup_root_logger_handlers()
    
    # Run tests
    yield
    
    # Clean up again at end of session using the same mechanisms
    LogFactory._cleanup_file_handlers()
    WindowsSafeFileHandler.close_all()
    cleanup_root_logger_handlers()

@pytest.fixture(autouse=True)
def cleanup_log_files_test():
    """
    Cleanup any log files and handlers before and after EACH test.
    This ensures file handlers don't leak between tests on Windows.
    """
    # Special handling for Windows platform
    if sys.platform == "win32":
        # On Windows, be extra cautious with file handles before each test
        LogFactory._cleanup_file_handlers()
        WindowsSafeFileHandler.close_all()
        cleanup_root_logger_handlers()
        
        # Run the test
        yield
        
        # On Windows, do extra cleanup after each test
        LogFactory._cleanup_file_handlers()
        WindowsSafeFileHandler.close_all()
        cleanup_root_logger_handlers()
    else:
        # On non-Windows platforms, we can be more relaxed
        # as they don't have the same file handle issues
        yield

@pytest.fixture(autouse=True)
def normalize_paths_for_tests():
    """
    On Windows, ensure path separators are normalized for cross-platform test consistency.
    This is essential to prevent "not a normalized and relative path" errors.
    """
    original_join = os.path.join
    original_abspath = os.path.abspath
    original_normpath = os.path.normpath
    
    def normalized_join(*args):
        """Wrapper to normalize path separators in test environments"""
        result = original_join(*args)
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            # In Windows test environments, convert backslashes to forward slashes
            result = result.replace('\\', '/')
        return result
    
    def normalized_abspath(path):
        """Wrapper to normalize absolute paths in test environments"""
        result = original_abspath(path)
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            result = result.replace('\\', '/')
        return result
        
    def normalized_normpath(path):
        """Wrapper to normalize path normalization in test environments"""
        result = original_normpath(path)
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            result = result.replace('\\', '/')
        return result
    
    # Only patch in testing environments
    if 'PYTEST_CURRENT_TEST' in os.environ:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(os.path, "join", normalized_join)
            mp.setattr(os.path, "abspath", normalized_abspath)
            mp.setattr(os.path, "normpath", normalized_normpath)
            
            # For pathlib Path objects, ensure they use forward slashes too
            if sys.platform == 'win32':
                original_path_str = Path.__str__
                
                def normalized_path_str(self):
                    """Normalize Path string representation in Windows tests"""
                    result = original_path_str(self)
                    return result.replace('\\', '/')
                
                mp.setattr(Path, "__str__", normalized_path_str)
            
            yield
    else:
        yield