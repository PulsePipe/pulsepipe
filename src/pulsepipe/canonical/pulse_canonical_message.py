from datetime import datetime
from typing import Optional, Dict
from pydantic import BaseModel
from .schema_version import CANONICAL_SCHEMA_VERSION
from pulsepipe.models.clinical_content import PulseClinicalContent

class PulseCanonicalMessage(BaseModel):
    id: Optional[str]                                # UUID or system-generated ID
    source_system: Optional[str]                     # e.g., "Epic", "Cerner", "HL7 Gateway"
    ingestor: Optional[str]                          # e.g., "HL7v2Ingestor", "FHIRIngestor"
    received_at: Optional[datetime]                  # Timestamp of ingestion
    processed_at: Optional[datetime]                 # When normalization completed
    deidentified: Optional[bool] = False             # If de-identified
    schema_version: str = CANONICAL_SCHEMA_VERSION   # Versioning
    content: PulseClinicalContent                    # The actual clinical content
    metadata: Optional[Dict[str, str]] = {}          # Extra metadata (batch, file_id, etc.)
