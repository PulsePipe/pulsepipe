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

import time
import socket
import os
import logging
from typing import Dict, List, Tuple, Optional
from pulsepipe.persistence.factory import get_database_connection, get_sql_dialect
from pulsepipe.persistence.database.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class DatabaseDiagnosticError(Exception):
    """Raised when database diagnostics detect a specific issue"""
    def __init__(self, message: str, issue_type: str, suggested_fixes: List[str], config_info: Dict = None):
        super().__init__(message)
        self.issue_type = issue_type
        self.suggested_fixes = suggested_fixes
        self.config_info = config_info or {}


def diagnose_database_connection(config: dict, timeout: int = 5) -> Tuple[str, List[str], Dict]:
    """
    Systematic diagnosis of database connection issues.
    
    Returns:
        - issue_type: Specific type of issue detected
        - suggested_fixes: List of actionable solutions
        - diagnostic_info: Additional details for troubleshooting
    """
    start_time = time.time()
    diagnostic_info = {
        "config_type": None,
        "connection_timeout": None,
        "total_diagnostic_time": None,
        "network_accessible": False,
        "auth_attempted": False,
        "database_exists": False
    }
    
    try:
        # 1. Validate configuration structure
        persistence_config = config.get("persistence", {})
        if not persistence_config:
            return "missing_persistence_config", [
                "Add persistence configuration to your pulsepipe.yaml",
                "Example: persistence: { database: { type: postgresql, host: localhost, ... } }",
                "Run 'pulsepipe config init --database postgresql' to generate template"
            ], diagnostic_info
        
        db_config = persistence_config.get("database", {})
        db_type = db_config.get("type")
        diagnostic_info["config_type"] = db_type
        
        if not db_type:
            return "missing_database_type", [
                "Set persistence.database.type in configuration",
                "Supported types: postgresql, mongodb, sqlite",
                "Example: persistence: { database: { type: postgresql } }"
            ], diagnostic_info
        
        # 2. Test network connectivity with timeout tracking
        network_start = time.time()
        is_network_accessible = _test_network_connectivity(db_config, timeout)
        network_elapsed = time.time() - network_start
        diagnostic_info["network_accessible"] = is_network_accessible
        
        if not is_network_accessible:
            if network_elapsed > (timeout - 1):
                return "network_timeout", [
                    f"Network connection timed out after {network_elapsed:.1f}s",
                    "Check if database server is running and accessible",
                    "Verify host/port configuration is correct",
                    "Test connectivity: telnet <host> <port>",
                    "Consider firewall or DNS resolution issues"
                ], diagnostic_info
            else:
                return "network_unreachable", [
                    "Database server is not accessible",
                    "Verify database server is running",
                    "Check host and port configuration",
                    "Confirm network connectivity and firewall rules"
                ], diagnostic_info
        
        # 3. Test database connection (authentication + basic query)
        auth_start = time.time()
        try:
            connection = get_database_connection(config)
            auth_elapsed = time.time() - auth_start
            diagnostic_info["auth_attempted"] = True
            diagnostic_info["connection_timeout"] = auth_elapsed
            
            if auth_elapsed > (timeout - 1):
                return "database_connection_timeout", [
                    f"Database connection timed out after {auth_elapsed:.1f}s",
                    "Check if database is overloaded or slow to respond",
                    "Verify connection pool settings",
                    "Consider reducing connection timeout in config",
                    "Monitor database performance metrics"
                ], diagnostic_info
            
            # Test basic database operations
            dialect = get_sql_dialect(config)
            
            # Try a simple query to validate connection
            if hasattr(connection, 'execute'):
                connection.execute("SELECT 1")
                diagnostic_info["database_exists"] = True
            
            # If we reach here, connection is working
            total_elapsed = time.time() - start_time
            diagnostic_info["total_diagnostic_time"] = total_elapsed
            
            if total_elapsed > 2:
                return "connection_slow_but_working", [
                    f"Database connection is slow ({total_elapsed:.1f}s) but functional",
                    "Consider optimizing database performance",
                    "Review connection pool configuration",
                    "Monitor database server resources"
                ], diagnostic_info
            
            return "connection_working", [
                "Database connection is functional",
                "No action required"
            ], diagnostic_info
            
        except Exception as e:
            auth_elapsed = time.time() - auth_start
            diagnostic_info["connection_timeout"] = auth_elapsed
            
            error_str = str(e).lower()
            
            # Classify authentication errors
            if any(term in error_str for term in ["auth", "password", "credential", "login", "permission"]):
                return "authentication_failed", [
                    f"Database authentication failed: {e}",
                    "Verify username and password are correct",
                    "Check database user permissions",
                    "Confirm user exists in database system",
                    "Review connection string format"
                ], diagnostic_info
            
            # Classify database existence errors
            elif any(term in error_str for term in ["database", "schema", "not found", "does not exist"]):
                return "database_not_found", [
                    f"Database or schema not found: {e}",
                    "Create the target database/schema",
                    "Verify database name in configuration",
                    "Run database initialization scripts",
                    "Check database server logs"
                ], diagnostic_info
            
            # Classify timeout errors
            elif auth_elapsed > (timeout - 1) or any(term in error_str for term in ["timeout", "time out"]):
                return "authentication_timeout", [
                    f"Database authentication timed out after {auth_elapsed:.1f}s",
                    "Database server may be overloaded",
                    "Check connection pool configuration",
                    "Verify database server performance",
                    "Consider increasing timeout values"
                ], diagnostic_info
            
            # Generic database error
            else:
                return "database_connection_error", [
                    f"Database connection failed: {e}",
                    "Check database server logs for details",
                    "Verify all connection parameters",
                    "Test connection manually with database client",
                    f"Review {db_type} specific configuration requirements"
                ], diagnostic_info
    
    except Exception as e:
        total_elapsed = time.time() - start_time
        diagnostic_info["total_diagnostic_time"] = total_elapsed
        
        return "diagnostic_error", [
            f"Diagnostic process failed: {e}",
            "Check configuration file syntax",
            "Verify all required parameters are present",
            "Review application logs for additional details"
        ], diagnostic_info


def _test_network_connectivity(db_config: dict, timeout: int) -> bool:
    """Test basic network connectivity to database server"""
    try:
        db_type = db_config.get("type", "").lower()
        
        if db_type in ["postgresql", "postgres"]:
            host = db_config.get("host", "localhost")
            port = db_config.get("port", 5432)
        elif db_type == "mongodb":
            host = db_config.get("host", "localhost")
            port = db_config.get("port", 27017)
        elif db_type == "sqlite":
            # SQLite is file-based, always "accessible"
            return True
        else:
            # Unknown database type, assume accessible
            return True
        
        # Test TCP connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        
        return result == 0
        
    except Exception:
        return False


def create_detailed_error_message(issue_type: str, suggested_fixes: List[str], config: dict, diagnostic_info: Dict) -> str:
    """Create a comprehensive error message with troubleshooting guidance"""
    
    db_type = diagnostic_info.get("config_type", "unknown")
    connection_timeout = diagnostic_info.get("connection_timeout")
    
    error_msg = f"""
🔴 Database Connection Failed: {issue_type.replace('_', ' ').title()}

Configuration: {db_type} database
"""
    
    if connection_timeout is not None:
        error_msg += f"Connection attempt duration: {connection_timeout:.2f}s\n"
    
    error_msg += f"""
Suggested fixes:
"""
    for i, fix in enumerate(suggested_fixes, 1):
        error_msg += f"  {i}. {fix}\n"
    
    # Add database-specific troubleshooting
    if db_type == "postgresql":
        error_msg += f"""
PostgreSQL Troubleshooting:
  • Test manually: psql -h {config.get('persistence', {}).get('database', {}).get('host', 'localhost')} -U {config.get('persistence', {}).get('database', {}).get('user', 'username')}
  • Check server: pg_isready -h <host> -p <port>
  • Review logs: tail -f /var/log/postgresql/postgresql-*.log
"""
    elif db_type == "mongodb":
        error_msg += f"""
MongoDB Troubleshooting:
  • Test manually: mongosh "mongodb://<host>:<port>/<database>"
  • Check server: nc -zv <host> <port>
  • Review logs: tail -f /var/log/mongodb/mongod.log
"""
    
    error_msg += f"""
To resolve this issue:
  1. Fix the database connection using the suggested steps above
  2. Verify your database server is running and accessible
  3. Run 'pulsepipe database health-check' for detailed diagnostics
"""
    
    return error_msg


def raise_database_diagnostic_error(config: dict, timeout: int = 5):
    """
    Diagnose database connection and raise appropriate error.
    This replaces silent SQLite fallback behavior.
    """
    issue_type, suggested_fixes, diagnostic_info = diagnose_database_connection(config, timeout)
    
    if issue_type in ["connection_working", "connection_slow_but_working"]:
        # Connection is actually working, don't raise error
        if issue_type == "connection_slow_but_working":
            logger.warning(f"Database connection is slow ({diagnostic_info.get('total_diagnostic_time', 0):.1f}s) but functional")
        return
    
    error_message = create_detailed_error_message(issue_type, suggested_fixes, config, diagnostic_info)
    
    raise DatabaseDiagnosticError(
        error_message,
        issue_type,
        suggested_fixes,
        diagnostic_info
    )