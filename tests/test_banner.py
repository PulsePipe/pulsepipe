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

# tests/banner.py

import pytest
from pulsepipe.cli.banner import get_banner, EMOJI_SLOGAN, BANNER

def test_get_banner_full():
    banner = get_banner()
    print("\n" + banner)  # Print with leading newline for clean separation
    assert "PulsePipe CLI" in banner
    assert EMOJI_SLOGAN in banner
    assert BANNER in banner  # Check that the ASCII banner content is present
    assert "Smart healthcare data pipelines" in banner

def test_get_banner_minimal():
    banner = get_banner(theme="minimal")
    print("\n" + banner)
    assert banner == f"PulsePipe CLI {EMOJI_SLOGAN}"

def test_banner_respects_config():
    config = {"logging": {"show_banner": False}}
    assert get_banner(theme="default", config=config) == ""
