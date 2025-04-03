
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

from pulsepipe.models import (
    ImagingReport, PathologyReport, DiagnosticTest, MicrobiologyReport, PulseClinicalContent,
    ImagingFinding, PathologyFinding, LabReport, LabObservation, BloodBankReport, BloodBankFinding, MessageCache
)
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import (
    extract_patient_reference,
    extract_encounter_reference,
    get_system,
    get_code,
    get_display,
)

def map_abnormal_flag(resource: dict) -> str:
    """
    Extracts FHIR interpretation.code and maps it to an abnormal flag.
    Defaults to 'N' if not present.
    """
    interp = resource.get("interpretation", [{}])[0]
    code = interp.get("coding", [{}])[0].get("code", "").upper()

    flag_map = {
        "H": "H",
        "L": "L",
        "HH": "HH",
        "LL": "LL",
        "A": "A",
        "N": "N",
        "U": "U",
    }

    return flag_map.get(code, "N")  # Default to Normal if unknown or missing

@fhir_mapper("DiagnosticReport")
class DiagnosticReportMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "DiagnosticReport"

    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        print("🔥 DiagnosticReportMapper running for resource ID:", resource.get("id"))
        category = self.get_category(resource)
        print("🔥 DiagnosticReport Detected category:", category)

        if category == "laboratory":
            print("🔥 Adding Laboratory")
            content.lab.append(self.parse_lab(resource, cache))

        elif category == "pathology":
            print("🔥 Adding PathologyReport")
            content.pathology.append(self.parse_pathology(resource, cache))

        elif category == "imaging":
            print("🔥 Adding ImagingReport")
            content.imaging.append(self.parse_imaging(resource, cache))

        elif category == "microbiology":
            print("🔥 Adding MicrobiologyReport")
            content.microbiology.append(self.parse_microbiology(resource, cache))

        elif category == "blood bank":
            print("❤️  Adding BloodBankReport")
            content.blood_bank.append(self.parse_blood_bank(resource, cache))
   
        elif category == "cardiology":
            print("❤️  Adding Cardiology")
            content.diagnostic_test.append(self.parse_cardiology(resource, cache))

        else:
            print("🔥 Adding Generic DiagnosticTest")
            content.diagnostic_test.append(self.parse_generic_diagnostic(resource, cache))

    def get_category(self, resource: dict) -> str:
        categories = resource.get("category", [])
        for cat in categories:
            for coding in cat.get("coding", []):
                code = (coding.get("code") or "").lower()
                print("   ↳ Found category code:", code)
                if code in {"pat", "path", "pathology"}:
                    return "pathology"
                if code.startswith("rad"):
                    return "imaging"
                if code.startswith("mic"):
                    return "microbiology"
                if code.startswith("card"):
                    return "cardiology"
                if code.startswith("lab"):
                    return "laboratory"
                if code.startswith("blood"):
                    return "blood bank"
                if code.startswith("card"):
                    return "cardiology"
        return "other"

    def parse_lab(self, resource: dict, cache: MessageCache) -> LabReport:
        observations = []

        for obs_ref in resource.get("result", []):
            ref_id = obs_ref.get("reference", "").lstrip("#")
            contained_obs = next(
                (c for c in resource.get("contained", []) if c.get("id") == ref_id and c.get("resourceType") == "Observation"),
                None
            )
            if contained_obs:
                observations.append(self.parse_lab_observation(contained_obs, cache))

        return LabReport(
            report_id=resource.get("id"),
            lab_type=None,  # optional, you can later map category codes to LabType
            code=get_code(resource),
            coding_method=get_system(resource),
            panel_name=resource.get("code", {}).get("text"),
            panel_code=None,
            panel_code_method=None,
            is_panel=len(observations) > 1,
            ordering_provider_id=None,
            performing_lab=None,
            report_type=None,
            collection_date=None,
            report_date=resource.get("effectiveDateTime"),
            observations=observations or [],
            note=resource.get("conclusion"),
            patient_id=extract_patient_reference(resource) or cache.get("patient_id"),
            encounter_id=extract_encounter_reference(resource) or cache.get("encounter_id"),
        )

    def parse_lab_observation(self, resource: dict, cache: MessageCache) -> LabObservation:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        ref_range_str = None
        if resource.get("referenceRange"):
            ref_range = resource["referenceRange"][0]
            low = ref_range.get("low", {}).get("value")
            high = ref_range.get("high", {}).get("value")
            units = ref_range.get("low", {}).get("unit") or ref_range.get("high", {}).get("unit")
            if low is not None and high is not None:
                ref_range_str = f"{low} - {high} {units or ''}".strip()
            elif low is not None:
                ref_range_str = f">= {low} {units or ''}".strip()
            elif high is not None:
                ref_range_str = f"<= {high} {units or ''}".strip()

        return LabObservation(
            observation_id=resource.get("id"),
            name=resource.get("code", {}).get("text") or get_display(resource) or "Unknown",
            code=get_code(resource),
            coding_method=get_system(resource),
            description=resource.get("code", {}).get("text") or get_display(resource) or "Unknown",
            value=str(resource.get("valueQuantity", {}).get("value")) if resource.get("valueQuantity", {}).get("value") is not None else None,
            unit=resource.get("valueQuantity", {}).get("unit"),
            interpretation=resource.get("interpretation", [{}])[0].get("text") if resource.get("interpretation") else None,
            abnormal_flag=map_abnormal_flag(resource),
            result_date=resource.get("effectiveDateTime") or "Unknown",
            reference_range=ref_range_str,
            status=resource.get("status"),
            timestamp=resource.get("effectiveDateTime"),
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

    def parse_imaging(self, resource: dict, cache: MessageCache) -> ImagingReport:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        finding = ImagingFinding(
            code=get_code(resource),
            coding_method=get_system(resource),
            description=resource.get("conclusion") or "No conclusion provided",
            impression="",
            abnormal_flag="",
            result_date=resource.get("effectiveDateTime") or ""
        )

        return ImagingReport(
            report_id=resource.get("id"),
            image_type=resource.get("code", {}).get("text"),
            coding_method=get_system(resource),
            modality=resource.get("modality", {}).get("coding", [{}])[0].get("code"),
            acquisition_date=resource.get("effectiveDateTime"),
            findings=[finding],
            ordering_provider_id=None,
            performing_facility=None,
            narrative=resource.get("text", {}).get("div"),
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

    def parse_pathology(self, resource: dict, cache: MessageCache) -> PathologyReport:
        findings = []
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        # ✅ Extract contained observations into findings
        for contained in resource.get("contained", []):
            if contained.get("resourceType") == "Observation":
                findings.append(PathologyFinding(
                    code=get_code(contained),
                    coding_method=get_system(contained),
                    description=contained.get("valueString") or contained.get("code", {}).get("text") or "Unknown",
                    comment=None
                ))

        # Fallback if no contained
        if not findings:
            findings.append(PathologyFinding(
                code=get_code(resource),
                coding_method=get_system(resource),
                description=resource.get("conclusion") or "No conclusion",
                comment=None
            ))

        return PathologyReport(
            report_id=resource.get("id"),
            patient_id=patient_id,
            encounter_id=encounter_id,
            test_name=resource.get("code", {}).get("text"),
            specimen=resource.get("bodySite", {}).get("text"),
            procedure=resource.get("code", {}).get("text"),
            collection_date=None,
            report_date=resource.get("effectiveDateTime"),
            performing_lab=None,
            ordering_provider_id=None,
            findings=findings,
            diagnosis=resource.get("conclusion"),
            staging=None,
            grade=None,
            narrative=resource.get("text", {}).get("div"),
            note=None,
        )


    def parse_microbiology(self, resource: dict, cache: MessageCache) -> MicrobiologyReport:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        return MicrobiologyReport(
            report_id=resource.get("id"),
            collection_date=resource.get("effectiveDateTime"),
            comment=resource.get("conclusion"),
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

    def parse_generic_diagnostic(self, resource: dict, cache: MessageCache) -> DiagnosticTest:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        return DiagnosticTest(
            test_id=resource.get("id"),
            test_type=resource.get("code", {}).get("text") or "Unknown",
            code=get_code(resource),
            coding_method=get_system(resource),
            result_date=resource.get("effectiveDateTime"),
            result_summary=resource.get("conclusion"),
            ordering_provider_id=None,
            performing_facility=None,
            findings="",   # ToDo -> parse the findings. Need an example
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

    def parse_blood_bank(self, resource: dict, cache: MessageCache) -> BloodBankReport:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")

        findings = []

        for contained in resource.get("contained", []):
            if contained.get("resourceType") == "Observation":
                findings.append(BloodBankFinding(
                    code=get_code(contained),
                    coding_method=get_system(contained),
                    test_name=contained.get("code", {}).get("text"),
                    result=contained.get("valueString"),
                    interpretation=contained.get("interpretation", [{}])[0].get("text"),
                    comment=contained.get("note", [{}])[0].get("text") if contained.get("note") else None
                ))

        return BloodBankReport(
            report_id=resource.get("id"),
            collection_date=resource.get("effectiveDateTime"),
            result_date=resource.get("effectiveDateTime"),
            findings=findings or [],
            comment=resource.get("conclusion"),
            patient_id=patient_id,
            encounter_id=encounter_id,
        )

    def parse_cardiology(self, resource: dict, cache: MessageCache) -> DiagnosticTest:
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")

        # Simple approach: treat first contained observation's valueString as the "findings"
        findings = ""
        if resource.get("contained"):
            obs = next((c for c in resource["contained"] if c.get("resourceType") == "Observation"), None)
            if obs:
                findings = obs.get("valueString") or ""

        return DiagnosticTest(
            test_id=resource.get("id"),
            test_type=resource.get("code", {}).get("text") or "Unknown",
            code=get_code(resource),
            coding_method=get_system(resource),
            result_date=resource.get("effectiveDateTime"),
            result_summary=resource.get("conclusion"),
            findings=findings,
            ordering_provider_id=None,
            performing_facility=None,
            patient_id=patient_id,
            encounter_id=encounter_id,
        )
