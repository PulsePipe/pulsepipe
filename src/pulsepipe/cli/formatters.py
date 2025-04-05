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
# 
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------
"""
Custom formatters for CLI output.

This module provides CLI-specific formatting functions that don't rely on the LogFactory.
"""
from typing import Dict, Any, List, Optional
import re
import json

# Import the emoji constants from LogFactory
from pulsepipe.utils.log_factory import DOMAIN_EMOJI


def format_model_summary(model_data: Dict[str, Any]) -> str:
    """Format a summary of the clinical content model."""
    summary_parts = []
    
    # Process patient info
    if "patient" in model_data and model_data["patient"]:
        patient = model_data["patient"]
        name = f"{patient.get('name', {}).get('given', [''])[0]} {patient.get('name', {}).get('family', '')}"
        name = name.strip() or "Unknown Patient"
        gender = patient.get("gender", "unknown")
        dob = patient.get("birthDate", "unknown DOB")
        
        summary_parts.append(f"👤 {name} ({gender}, {dob})")
    
    # Count clinical entities
    entity_counts = {}
    for key, value in model_data.items():
        if isinstance(value, list):
            if len(value) > 0:
                entity_counts[key] = len(value)
    
    # Add entity counts with emojis
    for entity, count in entity_counts.items():
        emoji = DOMAIN_EMOJI.get(entity, "📄")
        summary_parts.append(f"{emoji} {count} {entity.capitalize()}")
    
    if not summary_parts:
        return "No clinical content found"
    
    return "✅ " + " | ".join(summary_parts)