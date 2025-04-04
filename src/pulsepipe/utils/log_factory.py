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

import logging
import os
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

console = Console()

# Vitals	        🌡️
# Labs	            🧬
# Medications	    💊
# Imaging	        📸
# Pathology	        🔬
# Orders	        📋
# Errors	        ❌
# Success	        ✅

class LogFactory:
    @staticmethod
    def init_from_config(config: Optional[dict] = None):
        config = config or {}
        log_type = config.get("type", "rich")
        level = config.get("level", "INFO").upper()
        destination = config.get("destination", "stdout")
        file_path = config.get("file_path", "logs/pulsepipe.log")

        handlers = []

        # ✅ stdout handler
        if destination in {"stdout", "both"}:
            if log_type == "rich":
                handlers.append(
                    RichHandler(console=console, show_time=True, show_path=False)
                )
            elif log_type == "json":
                try:
                    from pythonjsonlogger import jsonlogger
                except ImportError:
                    raise ImportError("Please install python-json-logger for JSON log support.")
                import sys
                stream_handler = logging.StreamHandler(sys.stdout)
                stream_handler.setFormatter(jsonlogger.JsonFormatter())
                handlers.append(stream_handler)

        # ✅ file handler
        if destination in {"file", "both"}:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            file_handler = logging.FileHandler(file_path)
            if log_type == "json":
                from pythonjsonlogger import jsonlogger
                file_handler.setFormatter(jsonlogger.JsonFormatter())
            else:
                file_handler.setFormatter(logging.Formatter(
                    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
                ))
            handlers.append(file_handler)

        # ✅ fallback or disable
        if destination == "none":
            logging.disable(logging.CRITICAL)
            return

        logging.basicConfig(level=level, handlers=handlers, force=True)
        logging.getLogger("rich").setLevel(logging.WARNING)

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)

    