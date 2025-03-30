from enum import Enum

class IngestorType(str, Enum):
    HL7V2 = "HL7v2"
    FHIR = "FHIR"
    CDA = "CDA"
    PLAINTEXT = "PlainText"
    CUSTOM = "Custom"
