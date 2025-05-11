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

# src/pulsepipe/canonical/builder.py

import uuid
from datetime import datetime
from typing import Optional, Union

from .pulse_canonical_message import PulseCanonicalMessage
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.operational_content import PulseOperationalContent

class CanonicalBuilder:
    @staticmethod
    def build_clinical(
        content: PulseClinicalContent,
        source_system: str,
        ingestor: str,
        deidentified: bool = False,
        metadata: Optional[dict] = None
    ) -> PulseCanonicalMessage:
        """
        Build a canonical message with clinical content
        """
        return PulseCanonicalMessage(
            id=str(uuid.uuid4()),
            source_system=source_system,
            ingestor=ingestor,
            received_at=datetime.utcnow(),
            processed_at=datetime.utcnow(),
            deidentified=deidentified,
            clinical_content=content,
            operational_content=None,
            metadata=metadata or {},
        )
    
    @staticmethod
    def build_operational(
        content: PulseOperationalContent,
        source_system: str,
        ingestor: str,
        deidentified: bool = False,
        metadata: Optional[dict] = None
    ) -> PulseCanonicalMessage:
        """
        Build a canonical message with operational content
        """
        return PulseCanonicalMessage(
            id=str(uuid.uuid4()),
            source_system=source_system,
            ingestor=ingestor,
            received_at=datetime.utcnow(),
            processed_at=datetime.utcnow(),
            deidentified=deidentified,
            clinical_content=None,
            operational_content=content,
            metadata=metadata or {},
        )

    @staticmethod
    def build(
        content: Union[PulseClinicalContent, PulseOperationalContent],
        source_system: str,
        ingestor: str,
        deidentified: bool = False,
        metadata: Optional[dict] = None
    ) -> PulseCanonicalMessage:
        """
        Smart builder that detects content type and builds the appropriate message
        """
        if isinstance(content, PulseClinicalContent):
            return CanonicalBuilder.build_clinical(content, source_system, ingestor, deidentified, metadata)
        elif isinstance(content, PulseOperationalContent):
            return CanonicalBuilder.build_operational(content, source_system, ingestor, deidentified, metadata)
        else:
            raise TypeError(f"Unsupported content type: {type(content)}")