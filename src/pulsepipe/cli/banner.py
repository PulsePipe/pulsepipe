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


BANNER = """
██████╗ ██╗   ██╗██╗     ███████╗███████╗   ██████╗ ██╗ ██████╗ ███████╗
██╔══██╗██║   ██║██║     ██╔════╝██╔════╝   ██╔══██╗██║ ██╔══██╗██╔════╝
██████╔╝██║   ██║██║     ███████╗█████╗     ██████╔╝██║ ██████╔╝█████╗  
██╔═══╝ ██║   ██║██║     ╚════██║██╔══╝     ██╔═══╝ ██║ ██╔═══╝ ██╔══╝  
██║     ╚██████╔╝███████╗███████║███████╗   ██║     ██║ ██║     ███████╗
╚═╝      ╚═════╝ ╚══════╝╚══════╝╚══════╝   ╚═╝     ╚═╝ ╚═╝     ╚══════╝
"""

EMOJI_SLOGAN = "🔌 ⚙️  📥 🧠"

def get_banner(theme: str = "default", config: dict = None) -> str:
    show = True if config is None else config.get("logging", {}).get("show_banner", True)
    
    if not show:
        return ""
    
    if theme == "minimal":
        return "PulsePipe CLI " + EMOJI_SLOGAN
    
    return f"{BANNER}\nPulsePipe CLI  {EMOJI_SLOGAN}  — Smart healthcare data pipelines"