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

# src/pulsepipe/ingesters/x12_utils/decimal_utils.py

from decimal import Decimal, InvalidOperation
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)

def parse_x12_decimal(value: str, implied_decimal_places: int = 2) -> Decimal:
    """
    Parses an X12 numeric value with implied decimals.

    Example:
        '1500' -> Decimal('15.00') if implied_decimal_places=2
        '15.00' -> Decimal('15.00') if decimal point present
    """
    try:
        if not value or value.strip() == '':
            return Decimal("0.00")

        value = value.strip()

        if '.' in value:
            return Decimal(value)

        return Decimal(value) / (10 ** implied_decimal_places)

    except InvalidOperation:
        logger.warning(f"Invalid decimal value encountered: '{value}'")
        return Decimal("0.00")
