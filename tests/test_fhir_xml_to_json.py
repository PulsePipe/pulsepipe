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

# tests/test_fhir_xml_to_json.py

from pulsepipe.utils.xml_to_json import xml_to_json

def test_xml_to_json_patient():
    xml = '''
    <Patient xmlns="http://hl7.org/fhir">
        <id value="pat-001"/>
        <gender value="female"/>
        <birthDate value="1930-01-01"/>
    </Patient>
    '''

    json_data = xml_to_json(xml)

    # Strip namespaces if needed
    resource = json_data.get("Patient") or json_data.get("{http://hl7.org/fhir}Patient")

    assert resource is not None, "FHIR Patient resource not detected"
    assert resource["id"]["@value"] == "pat-001"
    assert resource["gender"]["@value"] == "female"
    assert resource["birthDate"]["@value"] == "1930-01-01"
