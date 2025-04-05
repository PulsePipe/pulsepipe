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
import logging
import datetime
from typing import Dict, Any, Optional

from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme

# Domain emoji mappings for more intuitive logs
DOMAIN_EMOJI = {
    # Clinical domains
    "patient": "👤",
    "demographics": "📋",
    "lab": "🧪",
    "laboratory": "🧪",
    "labs": "🧪",
    "medication": "💊",
    "medications": "💊",
    "prescription": "💊",
    "allergy": "⚠️",
    "allergies": "⚠️",
    "imaging": "📷",
    "radiology": "📷",
    "vital": "🌡️",
    "vitals": "🌡️",
    "vital_signs": "🌡️",
    "condition": "🩺",
    "diagnosis": "🩺",
    "diagnoses": "🩺",
    "procedure": "🔪",
    "procedures": "🔪",
    "encounter": "🏥",
    "immunization": "💉",
    "immunizations": "💉",
    "document": "📄",
    "observation": "📊",
    "result": "📊",
    "genomics": "🧬",
    "genetic": "🧬",
    "social": "🧑‍🤝‍🧑",
    "family": "👪",
    
    # Extended clinical domains
    "problem": "📋",
    "problem_list": "📋",
    "payor": "💰",
    "payors": "💰",
    "mar": "⏱️",
    "note": "📝",
    "notes": "📝",
    "pathology": "🔬",
    "diagnostic_test": "📊",
    "microbiology": "🦠",
    "blood_bank": "🩸",
    "family_history": "👪",
    "social_history": "🏠",
    "advance_directive": "📜",
    "advance_directives": "📜",
    "functional_status": "🚶",
    "order": "✅",
    "orders": "✅",
    "implant": "🦿",
    "implants": "🦿",
    
    # Operational domains
    "claim": "💼",
    "claims": "💼",
    "charge": "🧾",
    "charges": "🧾",
    "payment": "💵",
    "payments": "💵",
    "adjustment": "📝",
    "adjustments": "📝",
    "prior_auth": "🔐",
    "prior_authorization": "🔐",
    "prior_authorizations": "🔐",
    "operational": "📊",
    "transaction": "📊",
    "billing": "💰",
    "remittance": "💵",
    "eligibility": "✅",
    "enrollment": "👥",
    "premium": "💰",
    "subscriber": "👤",
    "provider": "👨‍⚕️",
    "service_line": "📋",
    "procedure_code": "🔢",
    "diagnosis_code": "🩺",
    "revenue_code": "💲",
    "modifier": "✏️",
    "insurance": "🛡️",
    "group": "👥",
    "member": "👤",
    "explanation": "📄",
    "eob": "📄",
    "x12": "🔄",
    "837": "📤",
    "835": "💵",
    "270": "❓",
    "271": "✅",
    "276": "❓",
    "277": "📋",
    "278": "🔐",
    "834": "👥",
    "820": "💰",
    
    # System domains
    "adapter": "🔌",
    "ingester": "📥",
    "pipeline": "⚙️",
    "persistence": "🗃️",
    "database": "🗄️",
    "file": "📁",
    "http": "🌐",
    "socket": "🧷",
    "webhook": "🪝",
    "config": "🛠️",
    "log": "📜",
    "error": "🛑",
    "warning": "⚠️",
    "info": "💡",
    "debug": "🕵️",
}


def add_emoji_to_log_message(logger_name: str, message: str) -> str:
    """Add appropriate emoji prefix to log messages based on domain."""
    # Get the most specific domain from the logger name
    parts = logger_name.split('.')
    domain = parts[-1].lower() if parts else ""
    
    # Find domain emoji
    emoji = None
    if domain in DOMAIN_EMOJI:
        emoji = DOMAIN_EMOJI[domain]
    else:
        # Try to find partial matches
        for key, value in DOMAIN_EMOJI.items():
            if key in logger_name.lower():
                emoji = value
                break
    
    if emoji:
        return f"{emoji} {message}"
    return message


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
    """Text formatter that adds domain-specific emojis to logs."""
    
    def format(self, logger_name: str, level: str, message: str,
              context: Optional[Dict[str, Any]] = None) -> str:
        """Format log message with domain-specific additions."""
        # Add emoji based on domain
        formatted_message = add_emoji_to_log_message(logger_name, message)
        
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
        "include_emoji": True,
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
        level = getattr(logging, cls._config["level"], logging.INFO)
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
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            root_logger.addHandler(handler)
        else:  # text format
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
        
        # Add file handler if log_file is specified
        if cls._config.get("log_file"):
            file_handler = logging.FileHandler(cls._config["log_file"])
            file_handler.setLevel(level)
            
            # Use JSON format for file logs if specified
            if cls._config.get("file_format") == "json":
                # File handler will be handled in log method
                pass
            else:
                formatter = logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S"
                )
                file_handler.setFormatter(formatter)
            
            root_logger.addHandler(file_handler)
        
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
                
                if cls._config["format"] == "rich":
                    # For rich formatting, just pass to original method
                    msg = add_emoji_to_log_message(logger.name, msg) if cls._config["include_emoji"] else msg
                    return method(msg, *args, **kwargs)
                elif cls._config["format"] == "json":
                    # For JSON, create JSON structure
                    json_formatter = DomainAwareJsonFormatter(include_emoji=cls._config["include_emoji"])
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
                    # For text, add domain-specific emoji
                    text_formatter = DomainAwareTextFormatter()
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