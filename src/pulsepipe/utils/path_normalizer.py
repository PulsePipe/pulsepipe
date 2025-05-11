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

"""Platform-aware path handling for cross-platform compatibility."""

import os
import platform
import re
from pathlib import Path


class PlatformPath:
    """
    Normalizes file paths based on the detected operating system.
    Handles conversion between Unix-style paths and Windows paths.
    """
    
    def __init__(self):
        """Initialize the path normalizer with OS detection."""
        self.is_windows = platform.system() == "Windows"
        
    def normalize_path(self, path):
        """
        Normalize a file path to be compatible with the current OS.
        
        Args:
            path (str): The file path to normalize
            
        Returns:
            str: Normalized path appropriate for the current OS
        """
        if not path:
            return path
            
        # Convert path to a standard form
        norm_path = os.path.normpath(path)
        
        if self.is_windows:
            # Handle Unix-style paths on Windows
            if norm_path.startswith('/'):
                # Check if this is a typical Unix root path like /tmp, /var, etc.
                match = re.match(r'^/(?:tmp|var|etc|home|usr|opt)(?:/|$)', norm_path)
                if match:
                    # Map Unix root paths to Windows temp directory
                    tmp_dir = os.environ.get('TEMP', 'C:\\Temp')
                    norm_path = os.path.join(tmp_dir, norm_path.lstrip('/'))
                else:
                    # For other paths starting with /, assume they're relative to current drive
                    norm_path = os.path.join(os.path.splitdrive(os.getcwd())[0], 
                                          norm_path.lstrip('/'))
            
            # Ensure Windows path separators
            norm_path = norm_path.replace('/', '\\')
        else:
            # On Unix systems, ensure forward slashes
            norm_path = norm_path.replace('\\', '/')
            
        return norm_path
    
    @staticmethod
    def get_instance():
        """
        Get a singleton instance of PlatformPath.
        
        Returns:
            PlatformPath: Singleton instance
        """
        if not hasattr(PlatformPath, "_instance"):
            PlatformPath._instance = PlatformPath()
        return PlatformPath._instance
