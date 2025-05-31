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

# src/pulsepipe/config/factory.py

# Import only lightweight modules at startup
from pulsepipe.utils.log_factory import LogFactory
from .config_loader import load_config

# Lazy import functions for heavy modules
def _get_adapter_classes():
    """Lazy import adapter classes."""
    from pulsepipe.adapters.file_watcher import FileWatcherAdapter
    return FileWatcherAdapter

def _get_ingester_classes():
    """Lazy import ingester classes."""
    from pulsepipe.ingesters.fhir_ingester import FHIRIngester
    from pulsepipe.ingesters.hl7v2_ingester import HL7v2Ingester
    from pulsepipe.ingesters.x12_ingester import X12Ingester
    from pulsepipe.ingesters.plaintext_ingester import PlainTextIngester
    return FHIRIngester, HL7v2Ingester, X12Ingester, PlainTextIngester

def create_adapter(config: dict, **kwargs):
    log_config = load_config()

    adapter_type = config["type"]

    if adapter_type == "file_watcher":
        # Lazy load adapter classes only when creating
        FileWatcherAdapter = _get_adapter_classes()
        adapter = FileWatcherAdapter(config)
        
        # Check for special flags 
        if kwargs.get('single_scan'):
            # Add a flag to do a single scan and then exit
            adapter.single_scan_mode = True
        
        return adapter
    
    raise ValueError(f"Unsupported adapter type: {adapter_type}")

def create_ingester(config: dict):
    # Lazy load ingester classes only when creating
    FHIRIngester, HL7v2Ingester, X12Ingester, PlainTextIngester = _get_ingester_classes()
    
    ingester_type = config["type"]

    if ingester_type == "fhir":
        return FHIRIngester()
    elif ingester_type == "hl7v2":
        return HL7v2Ingester()
    elif ingester_type == "x12":
        return X12Ingester()
    elif ingester_type == "plaintext":
        return PlainTextIngester()

    raise ValueError(f"Unsupported ingester type: {ingester_type}")
