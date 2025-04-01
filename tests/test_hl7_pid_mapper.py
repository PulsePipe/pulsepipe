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

# tests/test_pid_mapper.py

import unittest
from hl7apy.parser import parse_message
from pulsepipe.ingesters.hl7v2_utils.pid_mapper import PIDMapper
from pulsepipe.models import PulseClinicalContent

class TestPIDMapper(unittest.TestCase):
    def test_pid_mapper_basic(self):
        hl7_message = "MSH|^~\\&|HOSPITAL|HOSPITAL|||202503311200||ADT^A01|MSG00001|P|2.5\r" \
              "EVN|A01|202503311200\r" \
              "PID|1||123456^^^HOSP^MR||DOE^JOHN||19320101|M|||123 Main St^^Boston^MA^02115^USA||(555)555-1212|||EN|M|Catholic|MR|123456"

        message = parse_message(hl7_message)
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

        pid_segment = next(s for s in message.children if s.name == "PID")
        mapper = PIDMapper()
        self.assertTrue(mapper.accepts(pid_segment))

        mapper.map(pid_segment, content)

        # ✅ Assertions
        self.assertIsNotNone(content.patient)
        self.assertEqual(content.patient.id, "123456")
        self.assertEqual(content.patient.gender, "M")
        self.assertEqual(content.patient.dob_year, 1932)
        self.assertTrue(content.patient.over_90)
        self.assertEqual(content.patient.geographic_area, "USA 02115")  # Country & ZIP code
        self.assertEqual(content.patient.identifiers.get("MR"), "123456")
        
        # ✅ Preferences
        prefs = content.patient.preferences[0] if content.patient.preferences else None
        self.assertIsNotNone(prefs)
        self.assertEqual(prefs.preferred_language, "EN")
        self.assertEqual(prefs.communication_method, "Phone")

if __name__ == "__main__":
    unittest.main()
