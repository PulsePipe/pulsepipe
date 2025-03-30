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
