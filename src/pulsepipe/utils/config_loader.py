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

# src/pulsepipe/config/config_loader.py

import os
import yaml
from pathlib import Path

def get_config_dir() -> str:
    """Locate the config directory relative to the PulsePipe binary"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "..", "..", "config")


def load_mapping_config(filename: str) -> dict:
    """Load YAML config file for mapper overrides"""
    config_path = os.path.join(get_config_dir(), filename)
    if not os.path.exists(config_path):
        return {}  # Safe fallback if config is missing

    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}

def load_config(path: str = "pulsepipe.yaml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"❌ Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
