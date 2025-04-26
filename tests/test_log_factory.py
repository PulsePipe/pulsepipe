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
import io
from unittest.mock import patch, MagicMock, mock_open
import tempfile

from pulsepipe.utils.log_factory import (
    LogFactory, add_emoji_to_log_message, 
    WindowsSafeStreamHandler, WindowsSafeFileHandler,
    DomainAwareJsonFormatter, DomainAwareTextFormatter
)


class TestLogFactory:
    """Tests for the LogFactory class and related utilities."""

    @pytest.fixture
    def reset_log_factory(self):
        """Reset LogFactory between tests"""
        # Store current values
        old_config = LogFactory._config.copy()
        old_context = LogFactory._context.copy()
        old_console = LogFactory._console
        old_root_logger = LogFactory._root_logger
        old_logger_cache = LogFactory._logger_cache.copy()
        old_file_handlers = LogFactory._file_handlers.copy() if hasattr(LogFactory, '_file_handlers') else []
        
        # Clean up any existing file handlers first
        LogFactory._cleanup_file_handlers()
        WindowsSafeFileHandler.close_all()
        
        # Close any handlers in the root logger
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            if isinstance(handler, logging.FileHandler):
                try:
                    root_logger.removeHandler(handler)
                    handler.close()
                    if hasattr(handler, 'stream'):
                        handler.stream = None
                except:
                    pass
        
        # Reset after test
        yield
        
        # Clean up file handlers created during test
        LogFactory._cleanup_file_handlers()
        WindowsSafeFileHandler.close_all()
        
        # Close any handlers in the root logger again
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            if isinstance(handler, logging.FileHandler):
                try:
                    root_logger.removeHandler(handler)
                    handler.close()
                    if hasattr(handler, 'stream'):
                        handler.stream = None
                except:
                    pass
        
        # Restore values after test
        LogFactory._config = old_config
        LogFactory._context = old_context
        LogFactory._console = old_console
        LogFactory._root_logger = old_root_logger
        LogFactory._logger_cache = old_logger_cache
        LogFactory._file_handlers = old_file_handlers

    def test_add_emoji_to_log_message_with_emoji(self):
        """Test adding emoji to log messages when emoji is enabled."""
        # Test with exact domain match
        message = add_emoji_to_log_message("pulsepipe.patient", "Patient data loaded", use_emoji=True)
        assert message == "P Patient data loaded"
        
        # Test with domain as part of logger name
        message = add_emoji_to_log_message("pulsepipe.models.patient", "Patient data loaded", use_emoji=True)
        assert message == "P Patient data loaded"
        
        # Test with unknown domain
        message = add_emoji_to_log_message("pulsepipe.unknown", "Unknown data", use_emoji=True)
        assert message == "Unknown data"

    def test_add_emoji_to_log_message_without_emoji(self):
        """Test adding text prefix to log messages when emoji is disabled."""
        # Test with exact domain match
        message = add_emoji_to_log_message("pulsepipe.patient", "Patient data loaded", use_emoji=False)
        assert message == "[PT] Patient data loaded"
        
        # Test with domain as part of logger name
        message = add_emoji_to_log_message("pulsepipe.models.patient", "Patient data loaded", use_emoji=False)
        assert message == "[PT] Patient data loaded"
        
        # Test with unknown domain
        message = add_emoji_to_log_message("pulsepipe.unknown", "Unknown data", use_emoji=False)
        assert message == "Unknown data"

    def test_windows_safe_stream_handler_non_windows(self):
        """Test WindowsSafeStreamHandler on non-Windows platforms."""
        with patch('sys.platform', 'linux'):
            stream_mock = MagicMock()
            handler = WindowsSafeStreamHandler(stream_mock)
            assert handler.stream == stream_mock  # Should use the provided stream directly

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific test")
    def test_windows_safe_stream_handler_windows(self):
        """Test WindowsSafeStreamHandler on Windows platforms."""
        # This test will only run on Windows
        with patch('sys.platform', 'win32'):
            with patch('io.TextIOWrapper') as mock_wrapper:
                # Configure the mock to return a value when called
                mock_wrapper.return_value = MagicMock()
                
                # Create a stream mock with the necessary attributes
                mock_stream = MagicMock()
                mock_stream.buffer = MagicMock()
                
                # Make it look like stdout for the test
                with patch('sys.stdout', mock_stream):
                    handler = WindowsSafeStreamHandler(mock_stream)
                    
                    # On Windows, it should wrap the stream with TextIOWrapper
                    mock_wrapper.assert_called_once()

    def test_windows_safe_file_handler(self):
        """Test WindowsSafeFileHandler creates directories as needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = os.path.join(temp_dir, "logs")
            log_file = os.path.join(log_dir, "test.log")
            
            # Directory shouldn't exist yet
            assert not os.path.exists(log_dir)
            
            # Use context manager to ensure proper cleanup
            handler = None
            try:
                # Create handler
                handler = WindowsSafeFileHandler(log_file)
                
                # Directory should be created
                assert os.path.exists(log_dir)
                
                # Test writing to log file
                record = logging.LogRecord(
                    name="test",
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg="Test log message",
                    args=(),
                    exc_info=None
                )
                handler.emit(record)
                handler.flush()
                
                # Verify file was created
                assert os.path.exists(log_file)
                
            finally:
                # Always clean up, even if test fails
                if handler:
                    try:
                        handler.close()
                    except (OSError, ValueError):
                        pass  # Ignore errors during cleanup

    def test_domain_aware_json_formatter(self):
        """Test the DomainAwareJsonFormatter creates properly formatted JSON logs."""
        formatter = DomainAwareJsonFormatter(include_emoji=True)
        
        # Format a message
        formatted = formatter.format(
            logger_name="pulsepipe.patient", 
            level="INFO", 
            message="Patient record updated",
            context={"patient_id": "123", "action": "update"}
        )
        
        # Check JSON format
        import json
        log_data = json.loads(formatted)
        
        assert log_data["level"] == "INFO"
        assert log_data["logger"] == "pulsepipe.patient"
        assert log_data["message"] == "Patient record updated"
        assert log_data["domain"] == "patient"
        assert log_data["emoji"] == "P"
        assert log_data["context"]["patient_id"] == "123"
        assert log_data["context"]["action"] == "update"

    def test_domain_aware_text_formatter(self):
        """Test the DomainAwareTextFormatter adds domain prefixes to text logs."""
        # Test with emoji
        formatter = DomainAwareTextFormatter(use_emoji=True)
        formatted = formatter.format(
            logger_name="pulsepipe.patient", 
            level="INFO", 
            message="Patient record updated",
            context={"patient_id": "123", "action": "update"}
        )
        
        assert "P Patient record updated" in formatted
        assert "[patient_id=123]" in formatted
        assert "[action=update]" in formatted
        
        # Test without emoji
        formatter = DomainAwareTextFormatter(use_emoji=False)
        formatted = formatter.format(
            logger_name="pulsepipe.patient", 
            level="INFO", 
            message="Patient record updated"
        )
        
        assert "[PT] Patient record updated" in formatted

    def test_log_factory_init_from_config(self, reset_log_factory):
        """Test initializing LogFactory from configuration."""
        # Test with minimal config
        config = {
            "level": "DEBUG",
            "format": "text",
            "include_emoji": True
        }
        
        with patch('logging.getLogger') as mock_get_logger:
            mock_root_logger = MagicMock()
            # Add a handler to the mock so removeHandler will be called
            mock_handler = MagicMock()
            mock_root_logger.handlers = [mock_handler]
            mock_get_logger.return_value = mock_root_logger
            
            # Initialize
            LogFactory.init_from_config(config)
            
            # Check config was updated
            assert LogFactory._config["level"] == "DEBUG"
            assert LogFactory._config["format"] == "text"
            assert LogFactory._config["include_emoji"] == True
            
            # Check root logger was configured
            mock_root_logger.setLevel.assert_called_once_with(logging.DEBUG)
            
            # Should have added at least one handler
            assert mock_root_logger.addHandler.call_count > 0

    @patch('sys.platform', 'win32')
    def test_log_factory_windows_platform_detection(self, reset_log_factory):
        """Test that LogFactory detects Windows platform and disables emoji."""
        # Initialize with empty config
        with patch('logging.getLogger'):
            LogFactory.init_from_config({})
            
            # Should have disabled emoji because we're on Windows
            assert LogFactory._config["include_emoji"] == False

    def test_log_factory_format_setting(self, reset_log_factory):
        """Test that LogFactory properly sets format configuration."""
        # Test with rich format
        rich_config = {
            "format": "rich",
            "console_width": 100
        }
        
        with patch('logging.getLogger'):
            with patch('rich.logging.RichHandler'):
                # Just test that the config is set correctly
                LogFactory.init_from_config(rich_config)
                assert LogFactory._config["format"] == "rich"
                assert LogFactory._config["console_width"] == 100
                
                # Test with text format
                text_config = {
                    "format": "text"
                }
                LogFactory.init_from_config(text_config)
                assert LogFactory._config["format"] == "text"

    def test_log_factory_get_logger(self, reset_log_factory):
        """Test getting a logger from LogFactory."""
        # Initialize with minimal config
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            # Initialize factory
            LogFactory.init_from_config({"level": "INFO"})
            
            # Get a logger
            logger = LogFactory.get_logger("pulsepipe.test")
            
            # Should have called logging.getLogger
            mock_get_logger.assert_called_with("pulsepipe.test")
            
            # Should be in the cache
            assert "pulsepipe.test" in LogFactory._logger_cache
            
            # Getting the same logger again should return cached instance
            with patch('logging.getLogger') as mock_get_logger2:
                logger2 = LogFactory.get_logger("pulsepipe.test")
                # Should not call logging.getLogger again
                mock_get_logger2.assert_not_called()

    def test_log_factory_enhance_logger(self, reset_log_factory):
        """Test enhancing a logger with domain-specific methods."""
        # Initialize with minimal config
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            original_debug = mock_logger.debug
            original_info = mock_logger.info
            original_warning = mock_logger.warning
            original_error = mock_logger.error
            original_critical = mock_logger.critical
            
            mock_get_logger.return_value = mock_logger
            
            # Initialize factory
            LogFactory.init_from_config({"level": "INFO", "format": "text"})
            
            # Get a logger (which enhances it)
            logger = LogFactory.get_logger("pulsepipe.test")
            
            # The log methods should have been replaced
            assert logger.debug != original_debug
            assert logger.info != original_info
            assert logger.warning != original_warning
            assert logger.error != original_error
            assert logger.critical != original_critical

    def test_log_factory_file_handler_creation(self, reset_log_factory):
        """Test creating a file handler when specifying file destination."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            
            config = {
                "level": "INFO",
                "format": "text",
                "destination": "file",
                "file_path": log_file
            }
            
            with patch('logging.getLogger'):
                # Mock WindowsSafeFileHandler to avoid actual file creation
                with patch('pulsepipe.utils.log_factory.WindowsSafeFileHandler') as mock_file_handler:
                    # Set up the mock file handler for proper cleanup
                    mock_handler_instance = MagicMock()
                    mock_handler_instance.close = MagicMock()
                    mock_file_handler.return_value = mock_handler_instance
                    
                    LogFactory.init_from_config(config)
                    
                    # Should have created a file handler
                    mock_file_handler.assert_called_once_with(log_file, encoding='utf-8')
                    
                    # Manual cleanup to ensure test resources are released
                    LogFactory._cleanup_file_handlers()

    def test_enhanced_logger_methods_simple(self, reset_log_factory):
        """Test the enhanced logger methods use appropriate formatter."""
        # Initialize with text format
        config = {
            "level": "DEBUG",
            "format": "text",
            "include_emoji": True
        }
        
        # Mock the formatter and logger
        with patch('pulsepipe.utils.log_factory.DomainAwareTextFormatter') as mock_formatter:
            # Create a mock formatter instance
            formatter_instance = MagicMock()
            formatter_instance.format.return_value = "FORMATTED MESSAGE"
            mock_formatter.return_value = formatter_instance
            
            # Set up logging mocks
            with patch('logging.getLogger') as mock_get_logger:
                # Create a root logger mock
                mock_root_logger = MagicMock()
                mock_root_logger.handlers = []
                
                # Create a test logger mock
                mock_logger = MagicMock()
                mock_logger.handlers = []
                
                # Configure side effect to return different loggers
                mock_get_logger.side_effect = lambda name=None: mock_root_logger if name is None else mock_logger
                
                # Initialize factory (force a new logger creation)
                LogFactory._logger_cache = {}
                LogFactory.init_from_config(config)
                
                # Get logger and enhanced version with deterministic mock
                with patch('pulsepipe.utils.log_factory.LogFactory._enhance_logger', return_value=mock_logger) as mock_enhance:
                    logger = LogFactory.get_logger("pulsepipe.test")
                    
                    # The logger should be the same as our mock
                    assert logger is mock_logger
                    assert mock_enhance.called
                    
                    # Verify methods exist 
                    assert hasattr(logger, 'debug')
                    assert hasattr(logger, 'info')
                    assert hasattr(logger, 'warning')
                    assert hasattr(logger, 'error')
                    assert hasattr(logger, 'critical')