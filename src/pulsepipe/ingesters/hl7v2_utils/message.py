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

# src/pulsepipe/ingesters/hl7v2_utils/message.py

from typing import List

class Subcomponent:
    def __init__(self, subcomponents: List[str]):
        self.subcomponents = subcomponents

    def __len__(self):
        return len(self.subcomponents)

    def __getitem__(self, index):
        return self.subcomponents[index]

    def get(self, sub_idx: int = 1) -> str:
        try:
            return self.subcomponents[sub_idx - 1]
        except IndexError:
            return None
    def __str__(self):
        return "&".join(str(s) if s is not None else "" for s in self.subcomponents)

class Component:
    def __init__(self, components: List[Subcomponent]):
        self.components = components

    def __len__(self):
        return len(self.components)

    def __getitem__(self, index):
        return self.components[index]

    def get(self, comp_idx: int = 1, sub_idx: int = 1) -> str:
        try:
            return self.components[comp_idx - 1].get(sub_idx)
        except IndexError:
            return None

    def __str__(self):
        return "^".join(str(sub) for sub in self.components)

class Field:
    def __init__(self, repetitions: List[Component]):
        self.repetitions = repetitions

    def __len__(self):
        return len(self.repetitions)

    def __getitem__(self, index):
        return self.repetitions[index]

    def get(self, rep_idx: int = 0, comp_idx: int = 1, sub_idx: int = 1) -> str:
        try:
            return self.repetitions[rep_idx].get(comp_idx, sub_idx)
        except IndexError:
            return None

    def __str__(self):
        return "~".join(str(rep) for rep in self.repetitions)

class Segment:
    def __init__(self, id: str, fields: List[Field]):
        self.id = id
        self.fields = fields

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, index):
        return self.fields[index]

    def get(self, field: int, component: int = 1, subcomponent: int = 1, repetition: int = 0) -> str:
        try:
            reps = self.fields[field]
            comp = reps[repetition][component - 1]
            return comp[subcomponent - 1]
        except (IndexError, TypeError):
            return None

    def raw_field(self, field: int) -> List[List[List[str]]]:
        return self.fields[field] if field < len(self.fields) else []

    def __str__(self):
        field_strs = [str(field) for field in self.fields]
        return "".join(field_strs)

class Message:
    def __init__(self, id: str, segments: List[Segment]):
        self.id = id
        self.segments = segments

    def get(self, segment: int, field: int = 1, component: int = 1, subcomponent: int = 1, repetition: int = 0) -> str:
        try:
            fields = self.segments[segment]
            field = fields.get(component, subcomponent, repetition)
            comp = field.get(component)

            return comp[subcomponent - 1]
        except (IndexError, TypeError):
            return None

    def raw_field(self, segment: int) -> List[List[List[str]]]:
        return self.segments[segment] if segment < len(self.segments) else []

    def __str__(self):
        return "\r".join(str(segment) for segment in self.segments)
