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

# src/pulsepipe/models/social_history.py

from typing import Optional
from pydantic import BaseModel

class SocialHistory(BaseModel):
    """
    Represents social, behavioral, and environmental factors affecting a patient's health.
    
    Social history documents aspects of a patient's life that may impact their
    health status or treatment planning. This includes information about
    substance use (tobacco, alcohol, drugs), occupation, living situation,
    education, and other social determinants of health.
    
    These factors play a crucial role in holistic healthcare delivery and
    are essential for understanding risk factors, tailoring treatments, and
    addressing barriers to care.
    """
    description: Optional[str]                  # "Smoker", "Alcohol Use", "Homelessness"
    code: Optional[str]                         # SNOMED or local
    coding_method: Optional[str]
    status: Optional[str]                       # current, former, never
    start_date: Optional[str]
    end_date: Optional[str]
