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

# src/pulsepipe/pipelines/deid/config.py


"""
Configuration constants for de-identification.

This file contains sensitive configuration parameters that should be
kept separate from the main codebase and potentially customized in
production environments.
"""

# Default salt used for deterministic hashing
# This should be changed in production environments and kept secure
DEFAULT_SALT = "PulsePipe-2025-DEID"

# Hash length configurations
PATIENT_ID_HASH_LENGTH = 16
MRN_HASH_LENGTH = 16
GENERAL_ID_HASH_LENGTH = 12
ACCOUNT_HASH_LENGTH = 8

# Default redaction markers
REDACTION_MARKERS = {
    "name": "[REDACTED-NAME]",
    "mrn": "[REDACTED-MRN]",
    "ssn": "[REDACTED-SSN]",
    "date": "[REDACTED-DATE]",
    "address": "[REDACTED-ADDRESS]",
    "phone": "[REDACTED-PHONE]",
    "email": "[REDACTED-EMAIL]",
    "id": "[REDACTED-ID]"
}