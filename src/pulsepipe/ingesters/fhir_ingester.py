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

import json
import logging
from pulsepipe.models import PulseClinicalContent
from .fhir_utils.base_mapper import MAPPER_REGISTRY
from pulsepipe.utils.xml_to_json import xml_to_json

logger = logging.getLogger(__name__)

class FHIRIngester:
    def parse(self, raw_data: str) -> PulseClinicalContent:
        if not raw_data.strip():
            raise ValueError("Empty data received")

        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            data = xml_to_json(raw_data)

        if "resourceType" not in data:
            raise ValueError("Missing resourceType, not a valid FHIR resource.")

        content = PulseClinicalContent(
            patient=None,
            encounter=None,
            vital_signs=[],
            allergies=[],
            immunizations=[],
            diagnoses=[],
            problem_list=[],
            procedures=[],
            medications=[],
            payors=[],
            mar=[],
            notes=[],
            imaging=[],
            lab=[],
            pathology=[],
            diagnostic_test=[],
            microbiology=[],
            blood_bank=[],
            family_history=[],
            social_history=[],
            advance_directives=[],
            functional_status=[],
            order=[],
            implant=[],
        )

        if data["resourceType"] == "Bundle":
            for entry in data.get("entry", []):
                self._map_resource(entry.get("resource", {}), content)
        else:
            self._map_resource(data, content)

        return content

    def _map_resource(self, resource: dict, content: PulseClinicalContent):
        for mapper in MAPPER_REGISTRY:
            if mapper.accepts(resource):
                mapper.map(resource, content)
                break
