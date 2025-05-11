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

# --------------------------------------------------------------------
# PulsePipe - FHIR Mappers Auto-registration
# --------------------------------------------------------------------
# This file ensures all mappers are loaded and registered via @fhir_mapper
# --------------------------------------------------------------------

# src/pulsepipe/ingesters/fhir_utils/__init__.py

from . import (
    patient_mapper,
    encounter_mapper,
    allergy_mapper,
    immunization_mapper,
    observation_mapper,
    base_mapper,
    extractors,
    observation_helpers,
    condition_mapper,
    medication_mapper,
    diagnostic_report_mapper,
    problem_list_mapper,
    claim_mapper,
    device_mapper,
    document_reference_mapper,
    explanation_of_benefit_mapper,
    imaging_study_mapper,
    medication_administration_mapper,
    medication_request_mapper,
    procedure_mapper,
    supply_delivery_mapper,
    care_plan_mapper,
    care_team_mapper,
    provenance_mapper,
    location_mapper,
    organization_mapper,
    practitioner_mapper,
    practitioner_role_mapper,
)
