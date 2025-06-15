# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Chunk, Embed. Healthcare Data, AI-Ready with RAG.
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

import pytest
from unittest.mock import patch, MagicMock
from pulsepipe.utils.database_diagnostics import (
    diagnose_database_connection,
    create_detailed_error_message,
    raise_database_diagnostic_error,
    DatabaseDiagnosticError,
    _test_network_connectivity
)


class TestDatabaseDiagnostics:
    """Test database diagnostic functionality"""
    
    def test_diagnose_missing_persistence_config(self):
        """Test diagnosis when persistence config is missing"""
        config = {}
        
        issue_type, suggested_fixes, diagnostic_info = diagnose_database_connection(config, timeout=5)
        
        assert issue_type == "missing_persistence_config"
        assert "Add persistence configuration" in suggested_fixes[0]
        assert diagnostic_info["config_type"] is None
    
    def test_diagnose_missing_database_type(self):
        """Test diagnosis when database type is missing"""
        config = {
            "persistence": {
                # Missing 'type' field
                "postgresql": {
                    "host": "localhost"
                }
            }
        }
        
        issue_type, suggested_fixes, diagnostic_info = diagnose_database_connection(config, timeout=5)
        
        assert issue_type == "missing_database_type"
        assert "Set persistence.type" in suggested_fixes[0]
        assert diagnostic_info["config_type"] is None
    
    @patch('pulsepipe.utils.database_diagnostics._test_network_connectivity')
    def test_diagnose_network_unreachable(self, mock_network_test):
        """Test diagnosis when network is unreachable"""
        mock_network_test.return_value = False
        
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "unreachable-host",
                    "port": 5432
                }
            }
        }
        
        issue_type, suggested_fixes, diagnostic_info = diagnose_database_connection(config, timeout=5)
        
        assert issue_type == "network_unreachable"
        assert "Database server is not accessible" in suggested_fixes[0]
        assert diagnostic_info["network_accessible"] is False
    
    @patch('pulsepipe.utils.database_diagnostics._test_network_connectivity')
    @patch('pulsepipe.utils.database_diagnostics.get_database_connection')
    def test_diagnose_connection_working(self, mock_get_conn, mock_network_test):
        """Test diagnosis when connection is working"""
        mock_network_test.return_value = True
        
        # Mock successful connection that doesn't raise exceptions
        mock_connection = MagicMock()
        mock_connection.execute.return_value = None
        # Ensure the mock doesn't raise any exceptions
        mock_get_conn.return_value = mock_connection
        mock_get_conn.side_effect = None  # Clear any side effects
        
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "test_db",
                    "username": "test_user",
                    "password": "test_pass"
                }
            }
        }
        
        with patch('pulsepipe.utils.database_diagnostics.get_sql_dialect') as mock_dialect:
            mock_dialect.return_value = MagicMock()
            
            issue_type, suggested_fixes, diagnostic_info = diagnose_database_connection(config, timeout=5)
        
        assert issue_type == "connection_working"
        assert "Database connection is functional" in suggested_fixes[0] or "No action required" in suggested_fixes[1]
        assert diagnostic_info["network_accessible"] is True
        assert diagnostic_info["auth_attempted"] is True
    
    def test_create_detailed_error_message(self):
        """Test error message creation with diagnostic info"""
        issue_type = "network_timeout"
        suggested_fixes = [
            "Network connection timed out after 5.0s",
            "Check if database server is running"
        ]
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "slow-host"
                }
            }
        }
        diagnostic_info = {
            "config_type": "postgresql",
            "connection_timeout": 5.2
        }
        
        error_msg = create_detailed_error_message(issue_type, suggested_fixes, config, diagnostic_info)
        
        assert "🔴 Database Connection Failed: Network Timeout" in error_msg
        assert "postgresql database" in error_msg
        assert "Connection attempt duration: 5.20s" in error_msg
        assert "Network connection timed out after 5.0s" in error_msg
        assert "Fix the database connection using the suggested steps above" in error_msg
    
    def test_create_detailed_error_message_none_config_type(self):
        """Test error message formatting when config_type is None"""
        issue_type = "missing_database_type"
        suggested_fixes = [
            "Set persistence.database.type in configuration",
            "Supported types: postgresql, mongodb, sqlite"
        ]
        config = {}
        diagnostic_info = {
            "config_type": None,  # This should be handled gracefully
            "connection_timeout": None
        }
        
        error_msg = create_detailed_error_message(issue_type, suggested_fixes, config, diagnostic_info)
        
        assert "🔴 Database Connection Failed: Missing Database Type" in error_msg
        assert "unknown database" in error_msg
        assert "None database" not in error_msg  # Should not show None
        assert "1. Set persistence.database.type in configuration" in error_msg
    
    @patch('pulsepipe.utils.database_diagnostics.diagnose_database_connection')
    def test_raise_database_diagnostic_error(self, mock_diagnose):
        """Test raise_database_diagnostic_error function"""
        mock_diagnose.return_value = (
            "authentication_failed",
            ["Verify username and password", "Check database user permissions"],
            {"config_type": "postgresql", "connection_timeout": 2.1}
        )
        
        config = {"persistence": {"type": "postgresql"}}
        
        with pytest.raises(DatabaseDiagnosticError) as excinfo:
            raise_database_diagnostic_error(config, timeout=5)
        
        error = excinfo.value
        assert error.issue_type == "authentication_failed"
        assert "Verify username and password" in error.suggested_fixes
        assert error.config_info["config_type"] == "postgresql"
    
    @patch('pulsepipe.utils.database_diagnostics.diagnose_database_connection')
    def test_raise_database_diagnostic_error_working_connection(self, mock_diagnose):
        """Test that working connections don't raise errors"""
        mock_diagnose.return_value = (
            "connection_working",
            ["No action required"],
            {"config_type": "postgresql"}
        )
        
        config = {"persistence": {"type": "postgresql"}}
        
        # Should not raise an error for working connections
        raise_database_diagnostic_error(config, timeout=5)
    
    def test_network_connectivity_postgresql(self):
        """Test network connectivity test for PostgreSQL"""
        db_config = {
            "host": "127.0.0.1",
            "port": 5432
        }
        
        # This will test actual network connectivity (may fail if PostgreSQL not running)
        # but we're mainly testing the function doesn't crash
        result = _test_network_connectivity("postgresql", db_config, timeout=1)
        assert isinstance(result, bool)
    
    def test_network_connectivity_sqlite(self):
        """Test network connectivity test for SQLite"""
        db_config = {
            "path": "/tmp/test.db"
        }
        
        # SQLite is always "accessible" since it's file-based
        result = _test_network_connectivity("sqlite", db_config, timeout=1)
        assert result is True


class TestDatabaseDiagnosticError:
    """Test DatabaseDiagnosticError exception class"""
    
    def test_exception_initialization(self):
        """Test DatabaseDiagnosticError initialization"""
        message = "Test error message"
        issue_type = "test_issue"
        suggested_fixes = ["Fix 1", "Fix 2"]
        config_info = {"db_type": "postgresql"}
        
        error = DatabaseDiagnosticError(message, issue_type, suggested_fixes, config_info)
        
        assert str(error) == message
        assert error.issue_type == issue_type
        assert error.suggested_fixes == suggested_fixes
        assert error.config_info == config_info
    
    def test_exception_initialization_no_config_info(self):
        """Test DatabaseDiagnosticError initialization without config_info"""
        message = "Test error message"
        issue_type = "test_issue"
        suggested_fixes = ["Fix 1", "Fix 2"]
        
        error = DatabaseDiagnosticError(message, issue_type, suggested_fixes)
        
        assert str(error) == message
        assert error.issue_type == issue_type
        assert error.suggested_fixes == suggested_fixes
        assert error.config_info == {}