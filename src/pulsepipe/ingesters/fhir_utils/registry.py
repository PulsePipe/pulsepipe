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

from pulsepipe.ingesters.fhir_utils import (
    patient_mapper,
    encounter_mapper,
    allergy_mapper,
    observation_mapper,
    immunization_mapper
)


def get_resource_handlers():
    return {
        "Patient": lambda r, c: setattr(c, "patient", patient_mapper.map_patient(r)),
        "Encounter": lambda r, c: setattr(c, "encounter", encounter_mapper.map_encounter(r)),
        "AllergyIntolerance": lambda r, c: c.allergies.append(allergy_mapper.map_allergy(r)),
        "Observation": lambda r, c: observation_mapper.map_observation(r, c),
        "Immunization": lambda r, c: c.immunizations.append(immunization_mapper.map_immunization(r)),
    }
