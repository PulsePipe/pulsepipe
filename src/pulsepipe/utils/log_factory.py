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

# src/pulsepipe/config/log_factory.py

"""
Enhanced logging factory for PulsePipe.

Provides context-aware, domain-specific logging with emoji support
and configurable output formats (rich console, JSON, etc).
"""
import sys
import os
import io
import codecs
import logging
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme

# Import path resolver if available
try:
    from pulsepipe.utils.path_resolver import expand_path, get_default_log_path, ensure_directory_exists
    have_path_resolver = True
except ImportError:
    have_path_resolver = False
    print("Warning: path_resolver module not found, using basic path handling")

# Domain emoji mappings for more intuitive logs
DOMAIN_EMOJI = {
    # Clinical domains
    "patient": "P",
    "demographics": "D",
    "lab": "L",
    "laboratory": "L",
    "labs": "L",
    "medication": "M",
    "medications": "M",
    "prescription": "M",
    "allergy": "A",
    "allergies": "A",
    "imaging": "I",
    "radiology": "I",
    "vital": "V",
    "vitals": "V",
    "vital_signs": "V",
    "condition": "C",
    "diagnosis": "C",
    "diagnoses": "C",
    "procedure": "PR",
    "procedures": "PR",
    "encounter": "E",
    "immunization": "IM",
    "immunizations": "IM",
    "document": "DOC",
    "observation": "OBS",
    "result": "RES",
    "genomics": "GEN",
    "genetic": "GEN",
    "social": "SOC",
    "family": "FAM",
    
    # Extended clinical domains
    "problem": "PB",
    "problem_list": "PB",
    "payor": "PAY",
    "payors": "PAY",
    "mar": "MAR",
    "note": "N",
    "notes": "N",
    "pathology": "PATH",
    "diagnostic_test": "DX",
    "microbiology": "MB",
    "blood_bank": "BB",
    "family_history": "FH",
    "social_history": "SH",
    "advance_directive": "AD",
    "advance_directives": "AD",
    "functional_status": "FS",
    "order": "ORD",
    "orders": "ORD",
    "implant": "IMP",
    "implants": "IMP",
    
    # Operational domains
    "claim": "CLM",
    "claims": "CLM",
    "charge": "CHG",
    "charges": "CHG",
    "payment": "PMT",
    "payments": "PMT",
    "adjustment": "ADJ",
    "adjustments": "ADJ",
    "prior_auth": "AUTH",
    "prior_authorization": "AUTH",
    "prior_authorizations": "AUTH",
    "operational": "OP",
    "transaction": "TRX",
    "billing": "BILL",
    "remittance": "REM",
    "eligibility": "ELIG",
    "enrollment": "ENR",
    "premium": "PREM",
    "subscriber": "SUB",
    "provider": "PROV",
    "service_line": "SVC",
    "procedure_code": "CODE",
    "diagnosis_code": "DX",
    "revenue_code": "REV",
    "modifier": "MOD",
    "insurance": "INS",
    "group": "GRP",
    "member": "MBR",
    "explanation": "EOB",
    "eob": "EOB",
    "x12": "X12",
    "837": "837",
    "835": "835",
    "270": "270",
    "271": "271",
    "276": "276",
    "277": "277",
    "278": "278",
    "834": "834",
    "820": "820",
    
    # System domains
    "adapter": "ADPT",
    "ingester": "ING",
    "pipeline": "PIPE",
    "persistence": "DB",
    "database": "DB",
    "file": "FILE",
    "http": "HTTP",
    "socket": "SOCK",
    "webhook": "HOOK",
    "config": "CFG",
    "log": "LOG",
    "error": "ERR",
    "warning": "WARN",
    "info": "INFO",
    "debug": "DBG",
}

# Text-only version of domain emojis for use with Windows console
NON_EMOJI_DOMAIN_PREFIXES = {
    # Clinical domains
    "patient": "[PT]",
    "demographics": "[DEMO]",
    "lab": "[LAB]",
    "laboratory": "[LAB]",
    "labs": "[LAB]",
    "medication": "[MED]",
    "medications": "[MED]",
    "prescription": "[MED]",
    "allergy": "[ALG]",
    "allergies": "[ALG]",
    "imaging": "[IMG]",
    "radiology": "[IMG]",
    "vital": "[VS]",
    "vitals": "[VS]",
    "vital_signs": "[VS]",
    "condition": "[DX]",
    "diagnosis": "[DX]",
    "diagnoses": "[DX]",
    "procedure": "[PROC]",
    "procedures": "[PROC]",
    "encounter": "[ENC]",
    "immunization": "[IMM]",
    "immunizations": "[IMM]",
    "document": "[DOC]",
    "observation": "[OBS]",
    "result": "[RES]",
    "genomics": "[GEN]",
    "genetic": "[GEN]",
    "social": "[SOC]",
    "family": "[FAM]",
    
    # Extended clinical domains
    "problem": "[PROB]",
    "problem_list": "[PROB]",
    "payor": "[PAY]",
    "payors": "[PAY]",
    "mar": "[MAR]",
    "note": "[NOTE]",
    "notes": "[NOTE]",
    "pathology": "[PATH]",
    "diagnostic_test": "[DX]",
    "microbiology": "[MICRO]",
    "blood_bank": "[BB]",
    "family_history": "[FAM-H]",
    "social_history": "[SOC-H]",
    "advance_directive": "[AD]",
    "advance_directives": "[AD]",
    "functional_status": "[FUNC]",
    "order": "[ORD]",
    "orders": "[ORD]",
    "implant": "[IMP]",
    "implants": "[IMP]",
    
    # Operational domains
    "claim": "[CLAIM]",
    "claims": "[CLAIM]",
    "charge": "[CHG]",
    "charges": "[CHG]",
    "payment": "[PMT]",
    "payments": "[PMT]",
    "adjustment": "[ADJ]",
    "adjustments": "[ADJ]",
    "prior_auth": "[AUTH]",
    "prior_authorization": "[AUTH]",
    "prior_authorizations": "[AUTH]",
    "operational": "[OP]",
    "transaction": "[TRX]",
    "billing": "[BILL]",
    "remittance": "[REM]",
    "eligibility": "[ELIG]",
    "enrollment": "[ENRL]",
    "premium": "[PREM]",
    "subscriber": "[SUB]",
    "provider": "[PROV]",
    "service_line": "[SVC]",
    "procedure_code": "[CODE]",
    "diagnosis_code": "[DX]",
    "revenue_code": "[REV]",
    "modifier": "[MOD]",
    "insurance": "[INS]",
    "group": "[GRP]",
    "member": "[MBR]",
    "explanation": "[EOB]",
    "eob": "[EOB]",
    "x12": "[X12]",
    "837": "[837]",
    "835": "[835]",
    "270": "[270]",
    "271": "[271]",
    "276": "[276]",
    "277": "[277]",
    "278": "[278]",
    "834": "[834]",
    "820": "[820]",
    
    # System domains
    "adapter": "[ADPT]",
    "ingester": "[ING]",
    "pipeline": "[PIPE]",
    "persistence": "[DB]",
    "database": "[DB]",
    "file": "[FILE]",
    "http": "[HTTP]",
    "socket": "[SOCK]",
    "webhook": "[HOOK]",
    "config": "[CFG]",
    "log": "[LOG]",
    "error": "[ERR]",
    "warning": "[WARN]",
    "info": "[INFO]",
    "debug": "[DBG]",
}


def add_emoji_to_log_message(logger_name: str, message: str, use_emoji: bool = True) -> str:
    """Add appropriate prefix to log messages based on domain."""
    # Get the most specific domain from the logger name
    parts = logger_name.split('.')
    domain = parts[-1].lower() if parts else ""
    
    # Find domain prefix (emoji or text version)
    prefix = None
    prefix_map = DOMAIN_EMOJI if use_emoji else NON_EMOJI_DOMAIN_PREFIXES
    
    if domain in prefix_map:
        prefix = prefix_map[domain]
    else:
        # Try to find partial matches
        for key, value in prefix_map.items():
            if key in logger_name.lower():
                prefix = value
                break
    
    if prefix:
        return f"{prefix} {message}"
    return message


class WindowsSafeStreamHandler(logging.StreamHandler):
    """Stream handler that safely handles Unicode characters on Windows."""
    
    def __init__(self, stream=None):
        if stream is None:
            stream = sys.stdout
        
        # On Windows, wrap stdout with a UTF-8 encoder
        if sys.platform == "win32" and stream in (sys.stdout, sys.stderr):
            # Force UTF-8 encoding for console output
            stream = io.TextIOWrapper(
                stream.buffer, 
                encoding='utf-8', 
                errors='backslashreplace'
            )
        
        super().__init__(stream)


class WindowsSafeFileHandler(logging.FileHandler):
    """File handler that safely handles Unicode characters on Windows."""
    
    def __init__(self, filename, mode='a', encoding='utf-8', delay=False):
        # Ensure directory exists
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)


class DomainAwareJsonFormatter:
    """JSON formatter that adds domain-specific context to logs."""
    
    def __init__(self, include_emoji: bool = True):
        self.include_emoji = include_emoji
    
    def format(self, logger_name: str, level: str, message: str, 
              context: Optional[Dict[str, Any]] = None) -> str:
        """Format log message as JSON with domain-specific additions."""
        import json
        
        log_data = {
            "timestamp": "{{timestamp}}",  # Placeholder to be filled by LogFactory
            "level": level,
            "logger": logger_name,
            "message": message
        }
        
        # Add domain information
        domain = logger_name.split('.')[-1].lower() if '.' in logger_name else logger_name.lower()
        log_data["domain"] = domain
        
        # Add emoji code for visual scanning in JSON logs
        if self.include_emoji:
            for key, emoji in DOMAIN_EMOJI.items():
                if key in logger_name.lower():
                    log_data["emoji"] = emoji
                    break
        
        # Add context if provided
        if context:
            log_data["context"] = context
        
        return json.dumps(log_data)


class DomainAwareTextFormatter:
    """Text formatter that adds domain-specific prefixes to logs."""
    
    def __init__(self, use_emoji: bool = False):
        self.use_emoji = use_emoji
    
    def format(self, logger_name: str, level: str, message: str,
              context: Optional[Dict[str, Any]] = None) -> str:
        """Format log message with domain-specific additions."""
        # Add emoji based on domain
        formatted_message = add_emoji_to_log_message(
            logger_name, message, use_emoji=self.use_emoji
        )
        
        # Add context if available
        if context:
            context_str = " ".join(f"[{k}={v}]" for k, v in context.items())
            formatted_message = f"{formatted_message} {context_str}"
        
        return formatted_message


class LogFactory:
    """Factory class for creating and managing loggers."""
    
    # Class-level configuration
    _config = {
        "level": "INFO",
        "format": "rich",  # "rich", "text", or "json"
        "include_emoji": False,  # Default to no emoji for Windows compatibility
        "include_timestamp": True,
        "log_file": None,
        "console_width": 120,
    }
    
    # Global context to be included in all logs
    _context = {}
    
    # Rich console instance for rich logging
    _console = None
    
    # Root logger reference
    _root_logger = None
    
    # Logger cache to avoid creating duplicate loggers
    _logger_cache = {}
    
    @classmethod
    def init_from_config(cls, config: Dict[str, Any], context: Optional[Dict[str, Any]] = None):
        """Initialize the LogFactory from configuration dict."""
        # Update config
        if config:
            cls._config.update(config)
        
        # Set global context
        if context:
            cls._context = context
        
        # Detect Windows platform and adjust emoji usage
        # Override include_emoji to False on Windows unless explicitly set to True in config
        if sys.platform == "win32" and "include_emoji" not in config:
            cls._config["include_emoji"] = False
            print("Windows detected: disabling emoji in logs by default")
        
        # Create rich console if using rich format
        if cls._config["format"] == "rich":
            theme = Theme({
                "info": "cyan",
                "warning": "yellow",
                "error": "bold red",
                "debug": "dim white",
                "patient": "cyan",
                "lab": "green",
                "medication": "magenta",
                "allergy": "yellow",
                "imaging": "blue",
                "procedure": "red",
            })
            cls._console = Console(
                width=cls._config.get("console_width", 120),
                theme=theme,
                highlight=True
            )
        
        # Configure root logger
        log_level = cls._config["level"]
        
        # Handle case where log_level is not a string or int
        if callable(log_level) or not isinstance(log_level, (str, int)):
            print(f"Warning: Invalid log level type: {type(log_level)}. Using default INFO level.")
            log_level = "INFO"
        
        # Convert string to logging level
        if isinstance(log_level, str):
            level = getattr(logging, log_level.upper(), logging.INFO)
        else:
            level = log_level
        
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        
        # Remove existing handlers to avoid duplication
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create the appropriate handler based on format
        if cls._config["format"] == "rich":
            handler = RichHandler(
                console=cls._console,
                rich_tracebacks=True,
                tracebacks_show_locals=True,
                omit_repeated_times=False,
            )
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
        elif cls._config["format"] == "json":
            handler = WindowsSafeStreamHandler(sys.stdout)
            handler.setLevel(level)
            root_logger.addHandler(handler)
        else:  # text format
            # Use Windows-safe handler
            handler = WindowsSafeStreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%y/%m/%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
        
        # Add file handler if needed
        destination = config.get("destination", "stdout")
        if "file" in destination or destination == "both":
            # Get file path, expand variables, and resolve to absolute path
            if "file_path" in config:
                file_path = config["file_path"]
                
                # Handle environment variables if path_resolver is available
                if have_path_resolver:
                    file_path = expand_path(file_path)
                elif '%' in file_path:
                    # Basic environment variable replacement for Windows
                    import re
                    def replace_env_var(match):
                        var_name = match.group(1)
                        return os.environ.get(var_name, '')
                    file_path = re.sub(r'%([^%]+)%', replace_env_var, file_path)
            else:
                # Use default log path if path_resolver is available
                if have_path_resolver:
                    file_path = get_default_log_path()
                else:
                    # Fallback to a reasonable default
                    if sys.platform == 'win32':
                        appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
                        file_path = os.path.join(appdata, 'PulsePipe', 'logs', 'pulsepipe.log')
                    else:
                        file_path = os.path.expanduser('~/.pulsepipe/logs/pulsepipe.log')
            
            # Ensure log directory exists
            log_dir = os.path.dirname(file_path)
            try:
                if log_dir:
                    os.makedirs(log_dir, exist_ok=True)
                    print(f"Created log directory: {log_dir}")
            except Exception as e:
                print(f"Warning: Could not create log directory {log_dir}: {str(e)}")
                print("Will try to log to the specified file anyway")
                
            try:
                # Try to create a file handler with UTF-8 encoding
                print(f"Attempting to create log file at: {file_path}")
                file_handler = WindowsSafeFileHandler(file_path, encoding='utf-8')
                file_handler.setLevel(level)
                
                # Use a standard format for file logs unless JSON is specified
                if config.get("file_format") == "json":
                    formatter = logging.Formatter("%(message)s")
                else:
                    # Use a no-emoji formatter for file logging on Windows
                    use_emoji = cls._config["include_emoji"] and sys.platform != "win32"
                    text_formatter = DomainAwareTextFormatter(use_emoji=use_emoji)
                    
                    class CustomFormatter(logging.Formatter):
                        def format(self, record):
                            # Format timestamp and level
                            timestamp = self.formatTime(record, self.datefmt)
                            # Format message with domain-specific prefixes
                            message = text_formatter.format(
                                record.name, record.levelname, record.message, {}
                            )
                            return f"{timestamp} [{record.levelname}] {record.name}: {message}"
                    
                    formatter = CustomFormatter(
                        datefmt="%y/%m/%d %H:%M:%S"
                    )
                    
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
                
                # Log successful creation of log file
                print(f"✓ Log file created at: {os.path.abspath(file_path)}")
                
            except Exception as e:
                print(f"Error setting up file logging to {file_path}: {str(e)}")
                print(f"Will continue with console logging only")
                
                # Try to log to a fallback location
                try:
                    fallback_dir = os.path.join(os.path.expanduser('~'), 'pulsepipe_logs')
                    os.makedirs(fallback_dir, exist_ok=True)
                    fallback_path = os.path.join(fallback_dir, 'pulsepipe_fallback.log')
                    
                    print(f"Attempting to log to fallback location: {fallback_path}")
                    fallback_handler = WindowsSafeFileHandler(fallback_path, encoding='utf-8')
                    fallback_handler.setLevel(level)
                    fallback_handler.setFormatter(formatter)
                    root_logger.addHandler(fallback_handler)
                    
                    print(f"✓ Using fallback log file: {fallback_path}")
                except Exception as e2:
                    print(f"Could not create fallback log file: {str(e2)}")
        
        cls._root_logger = root_logger
        return root_logger
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get a logger with the specified name."""
        if name in cls._logger_cache:
            return cls._logger_cache[name]
        
        # Get or create logger
        logger = logging.getLogger(name)
        
        # Store in cache
        cls._logger_cache[name] = logger
        
        # Return enhanced logger with domain-specific methods
        return cls._enhance_logger(logger)
    
    @classmethod
    def _enhance_logger(cls, logger: logging.Logger) -> logging.Logger:
        """Enhance logger with domain-specific formatting and context."""
        # Create wrapped log methods to include domain-specific formatting
        original_debug = logger.debug
        original_info = logger.info
        original_warning = logger.warning
        original_error = logger.error
        original_critical = logger.critical
        
        def wrap_log_method(method, level: str):
            def wrapped(msg, *args, **kwargs):
                context = kwargs.pop("context", {})
                # Combine global and call-specific contexts
                combined_context = cls._context.copy()
                combined_context.update(context)
                
                # Determine if we should use emoji
                use_emoji = cls._config["include_emoji"] and not (
                    sys.platform == "win32" and level != "rich"
                )
                
                if cls._config["format"] == "rich":
                    # For rich formatting, add domain prefix
                    msg = add_emoji_to_log_message(
                        logger.name, msg, use_emoji=use_emoji
                    )
                    return method(msg, *args, **kwargs)
                elif cls._config["format"] == "json":
                    # For JSON, create JSON structure
                    json_formatter = DomainAwareJsonFormatter(include_emoji=use_emoji)
                    json_log = json_formatter.format(
                        logger_name=logger.name,
                        level=level,
                        message=msg % args if args else msg,
                        context=combined_context if combined_context else None
                    )
                    # Replace timestamp placeholder
                    timestamp = datetime.datetime.now().isoformat()
                    json_log = json_log.replace('"timestamp": "{{timestamp}}"', f'"timestamp": "{timestamp}"')
                    return method(json_log)
                else:
                    # For text, add domain-specific prefix
                    text_formatter = DomainAwareTextFormatter(use_emoji=use_emoji)
                    formatted_msg = text_formatter.format(
                        logger_name=logger.name,
                        level=level,
                        message=msg % args if args else msg,
                        context=combined_context if combined_context else None
                    )
                    return method(formatted_msg, *([]) if args else None, **kwargs)
            
            return wrapped
        
        # Replace logger methods with wrapped versions
        logger.debug = wrap_log_method(original_debug, "DEBUG")
        logger.info = wrap_log_method(original_info, "INFO")
        logger.warning = wrap_log_method(original_warning, "WARNING")
        logger.error = wrap_log_method(original_error, "ERROR")
        logger.critical = wrap_log_method(original_critical, "CRITICAL")
        
        return logger
