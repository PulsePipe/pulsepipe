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

from pulsepipe.utils.log_factory import LogFactory, WindowsSafeFileHandler

@pytest.fixture(scope="session", autouse=True)
def cleanup_log_files():
    """
    Cleanup any log files and handlers at the start and end of test session.
    This prevents "I/O operation on closed file" errors between tests.
    """
    # Clean up at start of session
    LogFactory._cleanup_file_handlers()
    WindowsSafeFileHandler.close_all()
    
    # Run tests
    yield
    
    # Clean up at end of session
    LogFactory._cleanup_file_handlers()
    WindowsSafeFileHandler.close_all()
    
    # Close any remaining handlers in the root logger
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            try:
                handler.close()
            except:
                pass

@pytest.fixture(autouse=True)
def normalize_paths_for_tests():
    """
    On Windows, ensure path separators are normalized for cross-platform test consistency.
    """
    original_join = os.path.join
    
    def normalized_join(*args):
        """Wrapper to normalize path separators in test environments"""
        result = original_join(*args)
        if 'PYTEST_CURRENT_TEST' in os.environ and sys.platform == 'win32':
            # In Windows test environments, convert backslashes to forward slashes
            result = result.replace('\\', '/')
        return result
    
    # Only patch in testing environments
    if 'PYTEST_CURRENT_TEST' in os.environ:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(os.path, "join", normalized_join)
            yield
    else:
        yield