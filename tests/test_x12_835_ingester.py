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

import unittest
from pathlib import Path
from pulsepipe.ingesters.x12_ingester import X12Ingester
from pulsepipe.models import PulseOperationalContent

class TestX12Ingester(unittest.TestCase):

    def setUp(self):
        bundle_path = Path(__file__).parent / "fixtures" / "test_x12_835.txt"
        with open(bundle_path, 'r') as f:
            self.raw_data = f.read()

    def test_parse_uhc_835(self):
        ingester = X12Ingester()
        content: PulseOperationalContent = ingester.parse(self.raw_data)

        # Basic assertions
        self.assertEqual(content.transaction_type, "835")
        self.assertEqual(content.interchange_control_number, "000000905")
        self.assertEqual(len(content.claims), 1)
        self.assertEqual(len(content.charges), 1)
        self.assertEqual(len(content.adjustments), 2)

        # Validate claim
        claim = content.claims[0]
        self.assertEqual(claim.claim_id, "PCN123456789")
        self.assertEqual(claim.total_charge_amount, 15)  # implied decimal handling test
        self.assertEqual(claim.total_payment_amount, 10)

        # Validate charge
        charge = content.charges[0]
        self.assertEqual(charge.charge_code, "HC:99213")
        self.assertEqual(charge.charge_amount, 15)
        self.assertEqual(charge.quantity, 1)

        # Validate adjustment
        adj = content.adjustments[0]
        self.assertEqual(adj.adjustment_reason_code, "45")
        self.assertEqual(adj.adjustment_amount, 5)

if __name__ == '__main__':
    unittest.main()
