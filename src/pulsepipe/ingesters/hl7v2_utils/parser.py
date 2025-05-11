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

# src/pulsepipe/ingesters/hl7v2_utils/hl7_parser.py

import re
from typing import List, Optional
from .message import Segment, Field, Component, Subcomponent

class HL7Message:
    """
    Represents a single HL7v2 message, parsed into structured segments.
    Provides accessors like 'PID.3.1.2' to retrieve field/component/subcomponent values.
    """

    def __init__(self, hl7_text: str):
        self.segments: List[Segment] = []
        self.segment_map: dict[str, List[List[List[List[str]]]]] = {}
        self.encoding_chars: dict[str, str] = {}
        self._parse(hl7_text)

    def _parse(self, text: str):
        text = text.replace('\r\n', '\r').replace('\n', '\r')
        raw_segments = [line for line in text.strip().split('\r') if line]

        if not raw_segments or not raw_segments[0].startswith("MSH"):
            raise ValueError("HL7 must start with an MSH segment")

        self.field_sep = raw_segments[0][3]
        enc = raw_segments[0].split(self.field_sep)[1]
        self.encoding_chars = {
            'component': enc[0] if len(enc) > 0 else '^',
            'repetition': enc[1] if len(enc) > 1 else '~',
            'escape': enc[2] if len(enc) > 2 else '\\',
            'subcomponent': enc[3] if len(enc) > 3 else '&'
        }
        self.comp_sep = self.encoding_chars['component']
        self.repetition_sep = self.encoding_chars['repetition']
        self.escape_char = self.encoding_chars['escape']
        self.subcomp_sep = self.encoding_chars['subcomponent']

        for raw_segment in raw_segments:
            seg_id = raw_segment[:3]
            rest = raw_segment[4:]
            fields = [seg_id] + rest.split(self.field_sep)

            field_objs: List[Field] = []

            for i, field in enumerate(fields):
                if seg_id == "MSH" and i == 1:
                    field_objs.append(Field([Subcomponent([Component([enc])])]))
                    continue

                repetitions = field.split(self.repetition_sep)
                rep_objs = []
                for rep in repetitions:
                    components = rep.split(self.comp_sep)
                    comp_objs = [Component(c.split(self.subcomp_sep)) for c in components]
                    rep_objs.append(Subcomponent(comp_objs))
                field_objs.append(Field(rep_objs))

            segment = Segment(seg_id, field_objs)
            self.segments.append(segment)
            self.segment_map.setdefault(seg_id, []).append(segment)

    def get(self, accessor: str, occurrence: int = 0) -> Optional[str]:
        """
        Access HL7 fields using a string accessor (e.g., "PID.5.1.2").

        Args:
            accessor (str): Field accessor like 'PID.3.1.2'
            occurrence (int): Zero-based occurrence of the segment

        Returns:
            Optional[str]: The extracted value, or None if not found
        """
        match = re.match(r"([A-Z]{2,3})\.(\d+)(?:\.(\d+))?(?:\.(\d+))?", accessor)
        if not match:
            raise ValueError("Accessor must be in the form SEG.F[.C[.S]]")

        seg_id, field_idx, comp_idx, sub_idx = match.groups()
        field_idx = int(field_idx)
        comp_idx = int(comp_idx) if comp_idx else 1
        sub_idx = int(sub_idx) if sub_idx else 1

        segs = self.segment_map.get(seg_id)
        if not segs or len(segs) <= occurrence:
            return None

        segment = segs[occurrence]
        if len(segment) <= field_idx:
            return None

        field = segment[field_idx]
        if not field or not field[0]:
            return None

        try:
            return field[0][comp_idx - 1][sub_idx - 1]
        except IndexError:
            return None

    def __str__(self) -> str:
        """Reconstructs the HL7 message as a string."""
        lines = []
        for segment in self.segments:
            field_strings = []
            for field in segment:
                rep_strings = []
                for repetition in field:
                    comp_strings = [self.subcomp_sep.join(sub) for sub in repetition]
                    rep_strings.append(self.comp_sep.join(comp_strings))
                field_strings.append(self.repetition_sep.join(rep_strings))
            lines.append(self.field_sep.join(field_strings))

        return '\r'.join(lines)
