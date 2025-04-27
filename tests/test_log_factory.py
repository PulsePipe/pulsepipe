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

# tests/test_log_factory.py

import pytest
import logging
import os
import sys
import tempfile
import json

from pulsepipe.utils.log_factory import (
    LogFactory,
    add_emoji_to_log_message,
    WindowsSafeStreamHandler,
    WindowsSafeFileHandler,
    DomainAwareJsonFormatter,
    DomainAwareTextFormatter,
)

@pytest.fixture(autouse=True)
def reset_log_factory():
    """Reset LogFactory and clear root logger handlers before and after each test."""
    LogFactory.reset()
    yield
    LogFactory.reset()
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        try:
            root_logger.removeHandler(handler)
            if hasattr(handler, 'close'):
                handler.close()
        except Exception:
            pass

def test_add_emoji_to_log_message_with_emoji():
    msg = add_emoji_to_log_message("pulsepipe.patient", "Test message", use_emoji=True)
    assert msg.startswith("P ")

def test_add_emoji_to_log_message_without_emoji():
    msg = add_emoji_to_log_message("pulsepipe.patient", "Test message", use_emoji=False)
    assert msg.startswith("[PT] ")

def test_windows_safe_stream_handler_behavior():
    stream = logging.StreamHandler(sys.stdout).stream
    handler = WindowsSafeStreamHandler(stream)
    assert hasattr(handler, 'emit')

def test_windows_safe_file_handler_create_and_emit():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "test.log")
        handler = WindowsSafeFileHandler(filepath)
        assert os.path.exists(os.path.dirname(filepath))

        record = logging.LogRecord("test", logging.INFO, "", 0, "Hello World", None, None)
        handler.emit(record)
        handler.close()

def test_domain_aware_json_formatter_structure():
    formatter = DomainAwareJsonFormatter(include_emoji=True)
    output = formatter.format(
        logger_name="pulsepipe.lab",
        level="INFO",
        message="Lab result updated",
        context={"patient_id": "456"}
    )
    data = json.loads(output)
    assert data["domain"] == "lab"
    assert data["level"] == "INFO"
    assert "patient_id" in data["context"]

def test_domain_aware_text_formatter_output():
    formatter = DomainAwareTextFormatter(use_emoji=False)
    output = formatter.format(
        logger_name="pulsepipe.lab",
        level="INFO",
        message="Lab result updated",
        context={"patient_id": "456"}
    )
    assert "[LAB]" in output
    assert "[patient_id=456]" in output

def test_log_factory_init_config_basic_text():
    config = {"format": "text", "level": "DEBUG", "include_emoji": True}
    LogFactory.init_from_config(config)
    assert LogFactory._config["format"] == "text"
    assert LogFactory._config["include_emoji"] is True

def test_log_factory_get_logger_caching():
    config = {"format": "text"}
    LogFactory.init_from_config(config)

    logger1 = LogFactory.get_logger("pulsepipe.test")
    logger2 = LogFactory.get_logger("pulsepipe.test")
    assert logger1 is logger2

def test_log_factory_enhanced_logger_methods_work():
    config = {"format": "text"}
    LogFactory.init_from_config(config)

    logger = LogFactory.get_logger("pulsepipe.example")
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")
    assert logger.name == "pulsepipe.example"

@pytest.mark.skipif(sys.platform == "win32", reason="File handler creation is disabled under pytest on Windows")
def test_log_factory_file_handler_actual_write():
    # Set environment variables to force file logging
    os.environ['PULSEPIPE_TEST_FILE_LOGGING_REQUIRED'] = '1'
    if 'PULSEPIPE_TEST_NO_FILE_LOGGING' in os.environ:
        del os.environ['PULSEPIPE_TEST_NO_FILE_LOGGING']
        
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            logfile = os.path.join(tmpdir, "logfile.log")
            
            # Create log directory structure to ensure it exists
            os.makedirs(os.path.dirname(logfile), exist_ok=True)
            
            # Make sure the file doesn't exist yet
            if os.path.exists(logfile):
                os.unlink(logfile)
                
            # Create an empty file to ensure path exists
            with open(logfile, 'w', encoding='utf-8') as f:
                f.write('')
                
            assert os.path.exists(logfile), f"Log file creation failed at {logfile}"
            
            config = {
                "format": "text", 
                "destination": "file",
                "file_path": logfile,
                "level": "INFO"
            }
            
            LogFactory.init_from_config(config)
            logger = LogFactory.get_logger("pulsepipe.filetest")
            
            # Write to the log
            logger.info("Logging to file!")
            
            # Manually flush handlers to ensure write
            for handler in logger.handlers:
                if hasattr(handler, "flush"):
                    handler.flush()
                    
            # Force close the handlers
            for handler in logger.handlers[:]:
                if hasattr(handler, "close"):
                    handler.close()
                    
            # Explicitly close and release all file handlers
            LogFactory._cleanup_file_handlers()
            
            # Verify file exists and contents
            assert os.path.exists(logfile), f"Log file doesn't exist at {logfile}"
            with open(logfile, 'r', encoding='utf-8') as f:
                content = f.read()
                assert "Logging to file" in content
                
    finally:
        # Clean up environment variables
        if 'PULSEPIPE_TEST_FILE_LOGGING_REQUIRED' in os.environ:
            del os.environ['PULSEPIPE_TEST_FILE_LOGGING_REQUIRED']

def test_add_emoji_to_log_message_unknown_domain():
    """Test fallback when logger name has unknown domain."""
    msg = add_emoji_to_log_message("pulsepipe.unknown_domain", "Unknown message", use_emoji=True)
    assert msg == "Unknown message"

def test_domain_aware_json_formatter_no_context_no_emoji():
    """Test JSON formatter without context or emoji."""
    formatter = DomainAwareJsonFormatter(include_emoji=False)
    output = formatter.format(logger_name="pulsepipe.unknown", level="WARNING", message="No emoji here")
    data = json.loads(output)
    assert data["logger"] == "pulsepipe.unknown"
    assert "emoji" not in data
    assert "context" not in data

def test_domain_aware_text_formatter_no_context():
    """Test Text formatter without context."""
    formatter = DomainAwareTextFormatter(use_emoji=True)
    output = formatter.format(logger_name="pulsepipe.unknown", level="WARNING", message="Simple message")
    assert output.startswith("Simple message") or output.startswith("[UNKNOWN]")

def test_windows_safe_file_handler_open_failure(monkeypatch):
    """Force WindowsSafeFileHandler._open() failure path gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_dir = os.path.join(tmpdir, "nonexistent", "logfile.log")

        # Simulate directory creation failure
        def always_fail_makedirs(*args, **kwargs):
            raise OSError("Simulated makedirs failure")

        monkeypatch.setattr(os, "makedirs", always_fail_makedirs)
        
        handler = None
        try:
            handler = WindowsSafeFileHandler(bad_dir)
        except OSError:
            # If constructor itself bubbles OSError, skip
            pytest.skip("Environment could not simulate handler open failure cleanly")
        
        if handler:
            assert handler.stream is None
            assert handler._closed

def test_windows_safe_file_handler_del_behavior():
    """Test that __del__ does not crash."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "deltest.log")
        handler = WindowsSafeFileHandler(filepath)
        handler.__del__()  # Should not raise

def test_log_factory_reset_clears_state():
    """Test that LogFactory.reset() fully clears internal state."""
    config = {"format": "text", "level": "DEBUG", "include_emoji": True}
    LogFactory.init_from_config(config)
    logger = LogFactory.get_logger("pulsepipe.test")
    assert logger is not None
    LogFactory.reset()
    # After reset, no logger should be cached
    assert LogFactory._logger_cache == {}
    assert LogFactory._config["format"] == "rich"  # Default format after reset
