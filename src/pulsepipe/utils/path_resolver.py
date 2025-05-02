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

# src/pulsepipe/config/path_resolver.py

"""
Path resolution utilities for PulsePipe.

Helps resolve platform-specific paths and environment variables.
"""

import os
import sys
import re
from pathlib import Path


def expand_path(path_str):
    """
    Expands a path string, handling environment variables and user home directory.
    
    Supports both Unix-style variables ($VAR, ${VAR}) and Windows-style (%VAR%).
    
    Args:
        path_str: A string representing a file path, possibly containing 
                environment variables or ~ for home directory
    
    Returns:
        An expanded absolute path
    """
    if not path_str:
        return path_str
    
    # First, handle the ~ for home directory
    if path_str.startswith('~'):
        path_str = os.path.expanduser(path_str)
        
    # Handle Windows-style environment variables (%VAR%)
    if '%' in path_str:
        def replace_windows_env(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
            
        path_str = re.sub(r'%([^%]+)%', replace_windows_env, path_str)
    
    # Handle Unix-style environment variables (${VAR} or $VAR)
    if '$' in path_str:
        # First handle ${VAR} format
        def replace_unix_brace_env(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
            
        path_str = re.sub(r'\${([^}]+)}', replace_unix_brace_env, path_str)
        
        # Then handle $VAR format
        def replace_unix_env(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
            
        path_str = re.sub(r'\$([a-zA-Z0-9_]+)', replace_unix_env, path_str)
    
    # Convert to absolute path if not already
    if not os.path.isabs(path_str):
        # If it's not already absolute, make it relative to current directory
        path_str = os.path.abspath(path_str)
    
    return path_str


def get_app_data_dir(app_name="PulsePipe"):
    """
    Returns an appropriate directory for application data storage.
    
    This will be platform-specific:
    - Windows: %APPDATA%\app_name
    - macOS: ~/Library/Application Support/app_name
    - Linux: ~/.local/share/app_name
    
    Args:
        app_name: Name of the application
        
    Returns:
        Path object pointing to the application data directory
    """
    if sys.platform == 'win32':
        # Get APPDATA environment variable
        appdata = os.environ.get('APPDATA', '')
        # Only use APPDATA if it's a non-empty string
        if appdata:
            app_dir = Path(appdata) / app_name
        else:
            # Fall back to user's home directory
            app_dir = Path(os.path.expanduser('~')) / app_name
    elif sys.platform == 'darwin':  # macOS
        app_dir = Path(os.path.expanduser('~')) / 'Library' / 'Application Support' / app_name
    else:  # Linux and other Unix-like
        app_dir = Path(os.path.expanduser('~')) / '.local' / 'share' / app_name
    
    # Create the directory if it doesn't exist
    os.makedirs(app_dir, exist_ok=True)
    
    return app_dir


def ensure_directory_exists(path_str):
    """
    Ensures that a directory exists for a given file path.
    
    Args:
        path_str: Path to a file or directory
        
    Returns:
        True if the directory exists or was created, False on failure
    """
    try:
        dir_path = os.path.dirname(path_str)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        return True
    except Exception:
        return False


def get_default_log_path():
    """
    Returns a platform-appropriate default log file path.
    
    Returns:
        String path to default log file location
    """
    app_dir = get_app_data_dir("PulsePipe")
    log_dir = app_dir / "logs"
    os.makedirs(log_dir, exist_ok=True)
    return str(log_dir / "pulsepipe.log")
