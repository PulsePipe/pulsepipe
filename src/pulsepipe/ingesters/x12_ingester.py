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

# src/pulsepipe/ingesters/x12_ingester.py

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models import PulseOperationalContent, MessageCache
from .x12_utils import base_mapper
from .x12_utils import (
    clp_mapper,
    svc_mapper,
    cas_mapper,
    nm1_mapper,
    plb_mapper,
    svc_mapper,
)

class X12Ingester:
    def __init__(self):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing X12Ingester")

    def parse(self, raw_data: str) -> PulseOperationalContent:
        if not raw_data or not raw_data.strip():
            self.logger.warning("Empty X12 data received, returning empty model")
            return PulseOperationalContent(
                transaction_type="UNKNOWN",
                interchange_control_number="UNKNOWN",
                functional_group_control_number="UNKNOWN",
                organization_id="UNKNOWN",
                claims=[], 
                charges=[], 
                payments=[], 
                adjustments=[], 
                prior_authorizations=[]
            )

        try:
            segments = [line.strip() for line in raw_data.strip().split('~') if line.strip()]
            if not segments:
                raise ValueError("No segments found in X12 data")

            meta = self._detect_transaction_type(segments)

            cache: MessageCache = {"claim_id": None, "patient_id": None, "encounter_id": None}
            content = PulseOperationalContent(
                transaction_type=meta.get("transaction_type", "UNKNOWN"),
                interchange_control_number=meta.get("interchange_control_number", "UNKNOWN"),
                functional_group_control_number=meta.get("functional_group_control_number", "UNKNOWN"),
                organization_id="UNKNOWN",  # For now unless you pass it externally
                claims=[], charges=[], payments=[], adjustments=[], prior_authorizations=[]
            )

            for segment_text in segments:
                elements = segment_text.split('*')
                segment_id = elements[0]
                elements = elements[1:]
                self._map_segment(segment_id, elements, content, cache)

            self.logger.info(f"Successfully parsed X12 message with {len(segments)} segments")

            return content

        except Exception as e:
            self.logger.exception(f"X12 parsing error: {str(e)}")
            # Return empty model instead of raising exception
            return PulseOperationalContent(
                transaction_type="ERROR",
                interchange_control_number="ERROR",
                functional_group_control_number="ERROR",
                organization_id="UNKNOWN",
                claims=[], 
                charges=[], 
                payments=[], 
                adjustments=[], 
                prior_authorizations=[]
            )


    def _detect_transaction_type(self, segments: list) -> dict:
        meta = {
            "transaction_type": "UNKNOWN",
            "interchange_control_number": "UNKNOWN",
            "functional_group_control_number": "UNKNOWN"
        }

        for seg in segments:
            if seg.startswith('ISA'):
                parts = seg.split('*')
                if len(parts) > 13:
                    meta["interchange_control_number"] = parts[13]
            elif seg.startswith('GS'):
                parts = seg.split('*')
                if len(parts) > 1:
                    meta["transaction_type"] = {
                        'HC': '837',  # Health Care Claim
                        'HP': '835',  # Health Care Claim Payment/Advice
                        'HR': '834',  # Benefit Enrollment and Maintenance
                        'HI': '270',  # Eligibility, Coverage or Benefit Inquiry
                        'HJ': '271',  # Eligibility, Coverage or Benefit Information
                        'HB': '276',  # Health Care Claim Status Request
                        'HN': '277',  # Health Care Claim Status Notification
                        'HS': '278',  # Health Care Services Review Information
                        'RT': '820',  # Payroll Deducted and Other Group Premium Payment
                        'FA': '999',  # Implementation Acknowledgment
                        'TA': '999',  # Implementation Acknowledgment (alternate code)
                        'RA': '277CA', # Claims Acknowledgement (used post-837)
                    }.get(parts[1], 'UNKNOWN')
                if len(parts) > 6:
                    meta["functional_group_control_number"] = parts[6]
                break  # we only expect one GS segment

        return meta


    def _map_segment(self, segment_id: str, elements: list, content: PulseOperationalContent, cache: dict):
        for mapper in base_mapper.MAPPER_REGISTRY:
            if mapper.accepts(segment_id):
                try:
                    mapper.map(segment_id, elements, content, cache)
                    self.logger.debug(f"Mapped segment {segment_id} using {mapper.__class__.__name__}")
                except Exception as e:
                    self.logger.exception(f"Error mapping segment {segment_id} with {mapper.__class__.__name__}")
                break  # Only one mapper should handle a segment
