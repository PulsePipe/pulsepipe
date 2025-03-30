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
from .base import Ingester
from pulsepipe.models import PulseClinicalContent
from pulsepipe.utils.xml_to_json import xml_to_json
from pulsepipe.ingesters.fhir_utils.registry import get_resource_handlers

logger = logging.getLogger(__name__)


class FHIRIngester(Ingester):
    def __init__(self):
        self.resource_handlers = get_resource_handlers()

    def parse(self, raw_data: str) -> PulseClinicalContent:
        raw_data = raw_data.strip()

        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            data = xml_to_json(raw_data)

        if "resourceType" not in data:
            raise ValueError("Missing resourceType, not a valid FHIR resource.")

        content = PulseClinicalContent()

        if data["resourceType"] == "Bundle":
            self._parse_bundle(data, content)
        else:
            self._dispatch(data, content)

        return content

    def _parse_bundle(self, bundle: dict, content: PulseClinicalContent):
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if not resource or "resourceType" not in resource:
                logger.warning("Skipping entry with missing resource or resourceType.")
                continue
            self._dispatch(resource, content)

    def _dispatch(self, resource: dict, content: PulseClinicalContent):
        resource_type = resource["resourceType"]
        handler = self.resource_handlers.get(resource_type)
        if handler:
            handler(resource, content)
        else:
            logger.info(f"Skipping unsupported resourceType: {resource_type}")
