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
# ------------------------------------------------------------------------------
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

# src/pulsepipe/config/config_loader.py

import os
import yaml
from pathlib import Path
import sys
import re
from typing import Optional, Any, Dict
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


def expand_environment_variables(config: Any) -> Any:
    """
    Recursively expand environment variables in configuration values.
    
    Supports ${VAR_NAME} and ${VAR_NAME:-default_value} syntax.
    
    Args:
        config: Configuration object (dict, list, str, or other)
        
    Returns:
        Configuration object with environment variables expanded
    """
    if isinstance(config, dict):
        return {key: expand_environment_variables(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [expand_environment_variables(item) for item in config]
    elif isinstance(config, str):
        # Pattern to match ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r'\$\{([^}]+)\}'
        
        def replace_env_var(match):
            var_expr = match.group(1)
            
            # Check if there's a default value (VAR_NAME:-default)
            if ':-' in var_expr:
                var_name, default_value = var_expr.split(':-', 1)
                return os.getenv(var_name.strip(), default_value)
            else:
                # No default value, just get the environment variable
                var_name = var_expr.strip()
                env_value = os.getenv(var_name)
                if env_value is None:
                    logger.warning(f"Environment variable '{var_name}' not found, keeping original value")
                    return match.group(0)  # Return original ${VAR_NAME} if not found
                return env_value
        
        return re.sub(pattern, replace_env_var, config)
    else:
        return config


def get_config_dir() -> str:
    """Locate the config directory relative to the PulsePipe binary"""
    # First, try to find the config directory next to the binary
    # For installed packages, this will be in the same directory as the executable
    if getattr(sys, 'frozen', False):  # Check if running as compiled executable
        base_dir = os.path.dirname(sys.executable)
    else:
        # For development, check relative to the script location
        base_dir = os.getcwd()  # Default to current working directory
    
    # Check if config directory exists next to binary/cwd
    primary_config_dir = os.path.join(base_dir, "config")
    if os.path.isdir(primary_config_dir):
        return primary_config_dir
    
    # Fallback to the package config directory
    package_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(package_dir, "..", "config")
    
    # For testing environments, ensure consistent path separators
    if 'PYTEST_CURRENT_TEST' in os.environ:
        # In test environments, always normalize paths to use forward slashes
        # This ensures cross-platform test consistency
        return config_dir.replace('\\', '/')


def find_config_file(filename: str) -> str:
    """Find a configuration file in various possible locations."""
    # Check if the file path is absolute or explicitly relative
    if os.path.isabs(filename) or filename.startswith('./') or filename.startswith('../'):
        if os.path.exists(filename):
            return filename
        return None
    
    # Define possible locations in priority order
    possible_locations = [
        # 1. Check in ./config/ next to the binary/cwd
        os.path.join("config", filename),
        # 2. Check in the current directory
        filename,
        # 3. Check in the default package config location
        os.path.join(get_config_dir(), filename),
        # 4. As a fallback, try the src path
        os.path.join("src", "pulsepipe", "config", filename),
    ]
    
    # Try each location
    for location in possible_locations:
        if os.path.exists(location):
            # For testing environments, ensure consistent path separators
            if 'PYTEST_CURRENT_TEST' in os.environ:
                return location.replace('\\', '/')
            else:
                return location
    
    # If still not found, return None
    return None


def load_mapping_config(filename: str) -> dict:
    """Load YAML config file for mapper overrides"""
    config_path = find_config_file(filename)
    if not config_path:
        logger.warning(f"Mapping config file not found: {filename}, using empty configuration")
        return {}  # Safe fallback if config is missing

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except UnicodeDecodeError as e:
        logger.warning(f"Encoding issue when reading {config_path}: {str(e)}. Trying different encodings.")
        # Try with different encodings
        for encoding in ['utf-8-sig', 'latin-1', 'ascii', 'cp1252']:
            try:
                with open(config_path, "r", encoding=encoding) as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                continue
        
        # If all attempts fail, return empty config
        logger.error(f"Could not read {config_path} with any encoding. Using empty configuration.")
        return {}
    except Exception as e:
        logger.error(f"Error loading mapping config {config_path}: {str(e)}")
        return {}


def load_config(path: str = "pulsepipe.yaml") -> dict:
    """Load configuration from a YAML file."""
    # First, check if the provided path exists
    if os.path.exists(path):
        config_path = path
    else:
        # Try to find the file in standard locations
        config_path = find_config_file(path)
        if not config_path:
            raise FileNotFoundError(f"❌ Config file not found: {path}")
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
            return expand_environment_variables(config)
    except UnicodeDecodeError as e:
        logger.warning(f"Encoding issue when reading {config_path}: {str(e)}. Trying different encodings.")
        # Try with different encodings
        for encoding in ['utf-8-sig', 'latin-1', 'ascii', 'cp1252']:
            try:
                with open(config_path, "r", encoding=encoding) as f:
                    config = yaml.safe_load(f) or {}
                    return expand_environment_variables(config)
            except Exception:
                continue
        
        # If all attempts fail, raise the original error
        raise FileNotFoundError(f"❌ Could not read config file with any encoding: {path}")
    except Exception as e:
        raise FileNotFoundError(f"❌ Error loading config file {path}: {str(e)}")


def load_data_intelligence_config(config_dict: Optional[dict] = None) -> 'DataIntelligenceConfig':
    """
    Load and validate data intelligence configuration.
    
    Args:
        config_dict: Optional config dictionary. If None, loads from pulsepipe.yaml
        
    Returns:
        Validated DataIntelligenceConfig instance
    """
    if config_dict is None:
        config_dict = load_config("pulsepipe.yaml")
    
    # Import here to avoid circular imports
    from pulsepipe.config.data_intelligence_config import load_data_intelligence_config as _load_di_config
    
    return _load_di_config(config_dict)
