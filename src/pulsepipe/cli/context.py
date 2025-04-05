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

# src/pulsepipe/cli/context.py

"""
Pipeline context management for PulsePipe CLI.

This manages contextual information for pipeline runs including:
- pipeline_id: Unique identifier for tracking a specific pipeline run
- user_id: User identifier for PulsePilot enterprise deployments
- org_id: Organization identifier for multi-tenant deployments
- Additional metadata for pipeline execution
"""
import uuid
import time
import socket
import getpass
from typing import Dict, Optional, Any
from dataclasses import dataclass, field, asdict


@dataclass
class PipelineContext:
    """Context information for a pipeline run."""
    
    # Primary identifiers
    pipeline_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    profile: Optional[str] = None
    
    # Enterprise/PulsePilot fields
    user_id: Optional[str] = None
    org_id: Optional[str] = None
    
    # Execution metadata - automatically populated
    hostname: str = field(default_factory=socket.gethostname)
    username: str = field(default_factory=getpass.getuser)
    start_time: float = field(default_factory=time.time)
    
    # Runtime flags
    is_dry_run: bool = False
    
    def __post_init__(self):
        """Validate and normalize context after initialization."""
        # Generate pipeline_id if not provided
        if not self.pipeline_id:
            self.pipeline_id = str(uuid.uuid4())
    
    def as_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        context_dict = asdict(self)
        # Filter out None values for cleaner logs
        return {k: v for k, v in context_dict.items() if v is not None}
    
    def get_log_prefix(self) -> str:
        """Get a prefix string for log messages."""
        parts = []
        if self.pipeline_id:
            parts.append(f"[{self.pipeline_id[:8]}]")
        if self.profile:
            parts.append(f"[{self.profile}]")
        if self.user_id and self.org_id:  # PulsePilot mode
            parts.append(f"[{self.user_id}@{self.org_id}]")
        return " ".join(parts)