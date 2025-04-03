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
from pulsepipe.models import PulseClinicalContent, MessageCache
from .fhir_utils.base_mapper import MAPPER_REGISTRY
from pulsepipe.utils.xml_to_json import xml_to_json

logger = logging.getLogger(__name__)

class FHIRIngester:
    def parse(self, raw_data: str) -> PulseClinicalContent:
        if not raw_data.strip():
            raise ValueError("Empty data received")
        
        print("💡 Registered mappers:")
        for m in MAPPER_REGISTRY:
            print(m)

        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            data = xml_to_json(raw_data)

        if "resourceType" not in data:
            raise ValueError("Missing resourceType, not a valid FHIR resource.")

        cache: MessageCache = {"patient_id": None, "encounter_id": None, "reference_id": None}

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

        # 🔵 First Pass - Cache Important References + Build Index
        if data["resourceType"] == "Bundle":
            for entry in data.get("entry", []):
                res = entry.get("resource", {})
                if not res: continue

                rtype = res.get("resourceType")
                rid = res.get("id")

                # cache first found patient, encounter, order
                if rtype == "Patient" and not cache["patient_id"]:
                    cache["patient_id"] = rid
                if rtype == "Encounter" and not cache["encounter_id"]:
                    cache["encounter_id"] = rid
                if rtype in {"ServiceRequest", "Order"} and not cache["order_id"]:
                    cache["order_id"] = rid

                # optional index for deferred linking
                #if rtype and rid:
                #    cache["resource_index"][f"{rtype}/{rid}"] = res

        # 🟣 Second Pass - Map normally
        if data["resourceType"] == "Bundle":
            for entry in data.get("entry", []):
                self._map_resource(entry.get("resource", {}), content, cache)
        else:
            self._map_resource(data, content, cache)

        # 🟢 Optional Third Pass - Fix up resources (cross-references)
        self._link_missing_references(content, cache)

        return content

    def _map_resource(self, resource: dict, content: PulseClinicalContent, cache: dict):
        for mapper in MAPPER_REGISTRY:
            if mapper.accepts(resource):
                mapper.map(resource, content, cache)
                break

    def _link_missing_references(self, content: PulseClinicalContent, cache: dict):
        # For example, link imaging reports to orders, or missing patient_id to labs
        # Not required unless you want advanced reference fixing
        pass
