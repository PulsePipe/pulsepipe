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
import uuid
from datetime import datetime
from typing import Optional

from .pulse_canonical_message import PulseCanonicalMessage
from pulsepipe.models.clinical_content import PulseClinicalContent

class CanonicalBuilder:
    @staticmethod
    def build(
        content: PulseClinicalContent,
        source_system: str,
        ingestor: str,
        deidentified: bool = False,
        metadata: Optional[dict] = None
    ) -> PulseCanonicalMessage:
        
        return PulseCanonicalMessage(
            id=str(uuid.uuid4()),
            source_system=source_system,
            ingestor=ingestor,
            received_at=datetime.utcnow(),
            processed_at=datetime.utcnow(),
            deidentified=deidentified,
            content=content,
            metadata=metadata or {},
        )
