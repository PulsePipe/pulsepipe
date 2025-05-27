# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Chunk, Embed. Healthcare Data, AI-Ready with RAG.
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

# src/pulsepipe/pipelines/quality/scoring_engine.py

"""
Comprehensive data quality scoring engine for healthcare data.

Provides completeness scoring, consistency checks, outlier detection,
data usage analysis, and aggregate quality scoring.
"""

import re
import statistics
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, Counter
from pydantic import BaseModel

from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


class QualityDimension(str, Enum):
    """Data quality dimensions."""
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    ACCURACY = "accuracy"
    OUTLIER_DETECTION = "outlier_detection"
    DATA_USAGE = "data_usage"


class Severity(str, Enum):
    """Issue severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class QualityIssue:
    """Individual quality issue found during assessment."""
    dimension: QualityDimension
    severity: Severity
    field_name: str
    issue_type: str
    description: str
    suggested_fix: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityScore:
    """Complete quality assessment results."""
    record_id: str
    record_type: str
    completeness_score: float
    consistency_score: float
    validity_score: float
    accuracy_score: float
    outlier_score: float
    data_usage_score: float
    overall_score: float
    issues: List[QualityIssue] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    invalid_fields: List[str] = field(default_factory=list)
    outlier_fields: List[str] = field(default_factory=list)
    unused_fields: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "record_id": self.record_id,
            "record_type": self.record_type,
            "completeness_score": round(self.completeness_score, 3),
            "consistency_score": round(self.consistency_score, 3),
            "validity_score": round(self.validity_score, 3),
            "accuracy_score": round(self.accuracy_score, 3),
            "outlier_score": round(self.outlier_score, 3),
            "data_usage_score": round(self.data_usage_score, 3),
            "overall_score": round(self.overall_score, 3),
            "issues_count": len(self.issues),
            "missing_fields": self.missing_fields,
            "invalid_fields": self.invalid_fields,
            "outlier_fields": self.outlier_fields,
            "unused_fields": self.unused_fields,
            "issues": [
                {
                    "dimension": issue.dimension.value,
                    "severity": issue.severity.value,
                    "field_name": issue.field_name,
                    "issue_type": issue.issue_type,
                    "description": issue.description,
                    "suggested_fix": issue.suggested_fix
                }
                for issue in self.issues
            ],
            "metadata": self.metadata
        }


class CompletenessScorer:
    """Evaluates data completeness across required and optional fields."""
    
    def __init__(self, required_fields: Optional[Dict[str, List[str]]] = None,
                 optional_fields: Optional[Dict[str, List[str]]] = None):
        """
        Initialize completeness scorer.
        
        Args:
            required_fields: Dict mapping record types to required field lists
            optional_fields: Dict mapping record types to optional field lists
        """
        self.required_fields = required_fields or self._get_default_required_fields()
        self.optional_fields = optional_fields or self._get_default_optional_fields()
    
    def _get_default_required_fields(self) -> Dict[str, List[str]]:
        """Get default required fields for common healthcare record types."""
        return {
            "Patient": ["id", "name", "birth_date"],
            "Observation": ["id", "subject", "code", "value"],
            "Encounter": ["id", "subject", "status", "class"],
            "Medication": ["id", "code", "status"],
            "Procedure": ["id", "subject", "code", "status"],
            "Condition": ["id", "subject", "code"],
            "DiagnosticReport": ["id", "subject", "code", "status"],
            "Immunization": ["id", "patient", "vaccine_code", "status"],
            "AllergyIntolerance": ["id", "patient", "substance", "reaction"],
            "default": ["id"]
        }
    
    def _get_default_optional_fields(self) -> Dict[str, List[str]]:
        """Get default optional fields for common healthcare record types."""
        return {
            "Patient": ["gender", "address", "phone", "email", "marital_status"],
            "Observation": ["effective_date", "performer", "category", "method"],
            "Encounter": ["period", "reason_code", "location", "participant"],
            "Medication": ["dosage", "route", "frequency", "prescriber"],
            "Procedure": ["performed_date", "performer", "location", "reason"],
            "Condition": ["onset_date", "severity", "stage", "evidence"],
            "DiagnosticReport": ["effective_date", "performer", "result", "conclusion"],
            "Immunization": ["occurrence_date", "performer", "lot_number", "route"],
            "AllergyIntolerance": ["onset_date", "severity", "category", "criticality"],
            "default": []
        }
    
    def score(self, data: Dict[str, Any], record_type: str) -> Tuple[float, List[QualityIssue]]:
        """
        Calculate completeness score for a record.
        
        Args:
            data: Record data to evaluate
            record_type: Type of the record
            
        Returns:
            Tuple of (score, issues_list)
        """
        issues = []
        
        # Get field requirements for this record type
        required = self.required_fields.get(record_type, self.required_fields.get("default", ["id"]))
        optional = self.optional_fields.get(record_type, self.optional_fields.get("default", []))
        
        # Check required fields
        missing_required = []
        present_required = 0
        
        for field in required:
            if self._is_field_present(data, field):
                present_required += 1
            else:
                missing_required.append(field)
                issues.append(QualityIssue(
                    dimension=QualityDimension.COMPLETENESS,
                    severity=Severity.HIGH,
                    field_name=field,
                    issue_type="missing_required",
                    description=f"Required field '{field}' is missing",
                    suggested_fix=f"Provide value for required field '{field}'"
                ))
        
        # Check optional fields
        present_optional = 0
        for field in optional:
            if self._is_field_present(data, field):
                present_optional += 1
        
        # Calculate score
        # Required fields are weighted 70%, optional fields 30%
        required_score = (present_required / len(required)) if required else 1.0
        optional_score = (present_optional / len(optional)) if optional else 1.0
        
        completeness_score = (required_score * 0.7) + (optional_score * 0.3)
        
        # Check for empty strings, None values in present fields
        self._check_field_quality(data, issues)
        
        return completeness_score, issues
    
    def _is_field_present(self, data: Dict[str, Any], field_path: str) -> bool:
        """Check if a field is present and has meaningful content."""
        try:
            # Handle nested field paths like "patient.name"
            value = data
            for part in field_path.split('.'):
                if isinstance(value, dict) and part in value:
                    value = value[part]
                elif isinstance(value, list) and part.isdigit():
                    idx = int(part)
                    value = value[idx] if 0 <= idx < len(value) else None
                else:
                    return False
            
            # Check if value is meaningful
            if value is None:
                return False
            if isinstance(value, str) and not value.strip():
                return False
            if isinstance(value, (list, dict)) and len(value) == 0:
                return False
            
            return True
        except (KeyError, IndexError, TypeError):
            return False
    
    def _check_field_quality(self, data: Dict[str, Any], issues: List[QualityIssue]) -> None:
        """Check quality of present fields."""
        for key, value in data.items():
            if isinstance(value, str):
                if not value.strip():
                    issues.append(QualityIssue(
                        dimension=QualityDimension.COMPLETENESS,
                        severity=Severity.MEDIUM,
                        field_name=key,
                        issue_type="empty_string",
                        description=f"Field '{key}' contains empty string",
                        suggested_fix=f"Provide meaningful value for '{key}' or remove field"
                    ))
                elif value.strip().lower() in ['null', 'none', 'n/a', 'unknown', '']:
                    issues.append(QualityIssue(
                        dimension=QualityDimension.COMPLETENESS,
                        severity=Severity.MEDIUM,
                        field_name=key,
                        issue_type="placeholder_value",
                        description=f"Field '{key}' contains placeholder value: '{value}'",
                        suggested_fix=f"Replace placeholder with actual value for '{key}'"
                    ))


class ConsistencyScorer:
    """Evaluates data consistency across fields and records."""
    
    def __init__(self):
        """Initialize consistency scorer with validation rules."""
        self.validation_patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'phone': re.compile(r'^[\+]?[1-9]?[\d\s\-\(\)\.]{7,15}$'),
            'ssn': re.compile(r'^\d{3}-?\d{2}-?\d{4}$'),
            'mrn': re.compile(r'^[A-Z0-9]{6,20}$'),
            'date': re.compile(r'^\d{4}-\d{2}-\d{2}'),
            'datetime': re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'),
            'icd10': re.compile(r'^[A-Z]\d{2}\.?[\dA-Z]*$'),
            'cpt': re.compile(r'^\d{5}$'),
            'loinc': re.compile(r'^\d{4,5}-\d$'),
            'snomed': re.compile(r'^\d{6,18}$')
        }
        
        self.reference_ranges = {
            'age': (0, 150),
            'heart_rate': (40, 200),
            'blood_pressure_systolic': (60, 250),
            'blood_pressure_diastolic': (30, 150),
            'temperature_celsius': (30, 45),
            'temperature_fahrenheit': (85, 115),
            'weight_kg': (0.5, 500),
            'height_cm': (20, 250),
            'bmi': (10, 80)
        }
    
    def score(self, data: Dict[str, Any], record_type: str,
              context_data: Optional[List[Dict[str, Any]]] = None) -> Tuple[float, List[QualityIssue]]:
        """
        Calculate consistency score for a record.
        
        Args:
            data: Record data to evaluate
            record_type: Type of the record
            context_data: Other records for cross-record consistency checks
            
        Returns:
            Tuple of (score, issues_list)
        """
        issues = []
        total_checks = 0
        passed_checks = 0
        
        # Format consistency checks
        format_score, format_issues = self._check_format_consistency(data)
        issues.extend(format_issues)
        total_checks += 1
        passed_checks += format_score
        
        # Logical consistency checks
        logic_score, logic_issues = self._check_logical_consistency(data, record_type)
        issues.extend(logic_issues)
        total_checks += 1
        passed_checks += logic_score
        
        # Cross-field consistency
        cross_field_score, cross_field_issues = self._check_cross_field_consistency(data)
        issues.extend(cross_field_issues)
        total_checks += 1
        passed_checks += cross_field_score
        
        # Temporal consistency
        temporal_score, temporal_issues = self._check_temporal_consistency(data)
        issues.extend(temporal_issues)
        total_checks += 1
        passed_checks += temporal_score
        
        # Cross-record consistency (if context provided)
        if context_data:
            cross_record_score, cross_record_issues = self._check_cross_record_consistency(
                data, context_data
            )
            issues.extend(cross_record_issues)
            total_checks += 1
            passed_checks += cross_record_score
        
        consistency_score = passed_checks / total_checks if total_checks > 0 else 1.0
        return consistency_score, issues
    
    def _check_format_consistency(self, data: Dict[str, Any]) -> Tuple[float, List[QualityIssue]]:
        """Check format consistency against expected patterns."""
        issues = []
        total_fields = 0
        valid_fields = 0
        
        for field_name, value in data.items():
            if not isinstance(value, str) or not value.strip():
                continue
                
            total_fields += 1
            field_type = self._infer_field_type(field_name.lower())
            
            if field_type and field_type in self.validation_patterns:
                pattern = self.validation_patterns[field_type]
                if pattern.match(value.strip()):
                    valid_fields += 1
                else:
                    issues.append(QualityIssue(
                        dimension=QualityDimension.CONSISTENCY,
                        severity=Severity.MEDIUM,
                        field_name=field_name,
                        issue_type="format_mismatch",
                        description=f"Field '{field_name}' value '{value}' doesn't match expected {field_type} format",
                        suggested_fix=f"Ensure '{field_name}' follows {field_type} format"
                    ))
            else:
                valid_fields += 1  # No specific format requirement
        
        score = valid_fields / total_fields if total_fields > 0 else 1.0
        return score, issues
    
    def _check_logical_consistency(self, data: Dict[str, Any], record_type: str) -> Tuple[float, List[QualityIssue]]:
        """Check logical consistency within the record."""
        issues = []
        checks_passed = 0
        total_checks = 0
        
        # Check numeric ranges
        for field_name, value in data.items():
            if isinstance(value, (int, float)):
                field_key = field_name.lower().replace('_', '_')
                if field_key in self.reference_ranges:
                    total_checks += 1
                    min_val, max_val = self.reference_ranges[field_key]
                    if min_val <= value <= max_val:
                        checks_passed += 1
                    else:
                        severity = Severity.HIGH if value < 0 or value > max_val * 2 else Severity.MEDIUM
                        issues.append(QualityIssue(
                            dimension=QualityDimension.CONSISTENCY,
                            severity=severity,
                            field_name=field_name,
                            issue_type="out_of_range",
                            description=f"Value {value} for '{field_name}' is outside expected range ({min_val}-{max_val})",
                            suggested_fix=f"Verify '{field_name}' value is correct"
                        ))
        
        # Record type specific checks
        if record_type == "Patient":
            total_checks += self._check_patient_logic(data, issues)
            checks_passed += total_checks - len([i for i in issues if i.issue_type.startswith('patient_')])
        elif record_type == "Observation":
            total_checks += self._check_observation_logic(data, issues)
            checks_passed += total_checks - len([i for i in issues if i.issue_type.startswith('observation_')])
        
        score = checks_passed / total_checks if total_checks > 0 else 1.0
        return score, issues
    
    def _check_cross_field_consistency(self, data: Dict[str, Any]) -> Tuple[float, List[QualityIssue]]:
        """Check consistency between related fields."""
        issues = []
        checks_passed = 0
        total_checks = 0
        
        # Birth date vs age consistency
        if 'birth_date' in data and 'age' in data:
            total_checks += 1
            try:
                birth_date = datetime.fromisoformat(data['birth_date'].replace('Z', '+00:00'))
                calculated_age = (datetime.now() - birth_date).days // 365
                stated_age = int(data['age'])
                
                if abs(calculated_age - stated_age) <= 1:  # Allow 1 year tolerance
                    checks_passed += 1
                else:
                    issues.append(QualityIssue(
                        dimension=QualityDimension.CONSISTENCY,
                        severity=Severity.MEDIUM,
                        field_name="age",
                        issue_type="age_birth_date_mismatch",
                        description=f"Age {stated_age} doesn't match birth date {data['birth_date']} (calculated: {calculated_age})",
                        suggested_fix="Verify age and birth date consistency"
                    ))
            except (ValueError, TypeError):
                pass  # Skip if date parsing fails
        
        # Height vs weight vs BMI consistency
        if all(k in data for k in ['height_cm', 'weight_kg', 'bmi']):
            total_checks += 1
            try:
                height_m = float(data['height_cm']) / 100
                weight = float(data['weight_kg'])
                stated_bmi = float(data['bmi'])
                calculated_bmi = weight / (height_m ** 2)
                
                if abs(calculated_bmi - stated_bmi) <= 1:
                    checks_passed += 1
                else:
                    issues.append(QualityIssue(
                        dimension=QualityDimension.CONSISTENCY,
                        severity=Severity.MEDIUM,
                        field_name="bmi",
                        issue_type="bmi_calculation_mismatch",
                        description=f"BMI {stated_bmi} doesn't match calculated BMI {calculated_bmi:.1f}",
                        suggested_fix="Recalculate BMI or verify height/weight values"
                    ))
            except (ValueError, TypeError):
                pass
        
        score = checks_passed / total_checks if total_checks > 0 else 1.0
        return score, issues
    
    def _check_temporal_consistency(self, data: Dict[str, Any]) -> Tuple[float, List[QualityIssue]]:
        """Check temporal consistency between date fields."""
        issues = []
        checks_passed = 0
        total_checks = 0
        
        # Extract date fields
        date_fields = {}
        for field_name, value in data.items():
            if isinstance(value, str) and ('date' in field_name.lower() or 'time' in field_name.lower()):
                try:
                    date_fields[field_name] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    continue
        
        # Check logical date ordering
        if 'birth_date' in date_fields:
            for field_name, date_value in date_fields.items():
                if field_name != 'birth_date':
                    total_checks += 1
                    if date_value >= date_fields['birth_date']:
                        checks_passed += 1
                    else:
                        issues.append(QualityIssue(
                            dimension=QualityDimension.CONSISTENCY,
                            severity=Severity.HIGH,
                            field_name=field_name,
                            issue_type="temporal_order_violation",
                            description=f"Date {field_name} ({date_value}) is before birth date ({date_fields['birth_date']})",
                            suggested_fix=f"Verify {field_name} is after birth date"
                        ))
        
        score = checks_passed / total_checks if total_checks > 0 else 1.0
        return score, issues
    
    def _check_cross_record_consistency(self, data: Dict[str, Any],
                                      context_data: List[Dict[str, Any]]) -> Tuple[float, List[QualityIssue]]:
        """Check consistency across multiple records."""
        issues = []
        checks_passed = 0
        total_checks = 1
        
        # Check for conflicting patient information
        if 'patient_id' in data:
            patient_records = [r for r in context_data if r.get('patient_id') == data['patient_id']]
            
            # Check for consistent patient demographics
            patient_fields = ['name', 'birth_date', 'gender']
            for field in patient_fields:
                if field in data:
                    conflicting_values = set()
                    for record in patient_records:
                        if field in record and record[field] != data[field]:
                            conflicting_values.add(record[field])
                    
                    if conflicting_values:
                        issues.append(QualityIssue(
                            dimension=QualityDimension.CONSISTENCY,
                            severity=Severity.HIGH,
                            field_name=field,
                            issue_type="cross_record_inconsistency",
                            description=f"Inconsistent {field} across patient records: {conflicting_values}",
                            suggested_fix=f"Resolve {field} discrepancies across patient records"
                        ))
        
        if not issues:
            checks_passed = 1
        
        return checks_passed / total_checks, issues
    
    def _check_patient_logic(self, data: Dict[str, Any], issues: List[QualityIssue]) -> int:
        """Check patient-specific logical consistency."""
        checks = 0
        
        # Gender vs pregnancy status
        if data.get('gender', '').lower() == 'male' and 'pregnancy_status' in data:
            if data['pregnancy_status'] not in [None, '', 'Not applicable']:
                issues.append(QualityIssue(
                    dimension=QualityDimension.CONSISTENCY,
                    severity=Severity.HIGH,
                    field_name="pregnancy_status",
                    issue_type="patient_gender_pregnancy_mismatch",
                    description="Male patient has pregnancy status information",
                    suggested_fix="Remove pregnancy status for male patients"
                ))
            checks += 1
        
        return checks
    
    def _check_observation_logic(self, data: Dict[str, Any], issues: List[QualityIssue]) -> int:
        """Check observation-specific logical consistency."""
        checks = 0
        
        # Value vs unit consistency
        if 'value' in data and 'unit' in data:
            value = data['value']
            unit = data['unit']
            
            # Temperature units
            if 'temperature' in data.get('code', '').lower():
                if isinstance(value, (int, float)):
                    if unit.lower() in ['c', 'celsius'] and not (30 <= value <= 45):
                        issues.append(QualityIssue(
                            dimension=QualityDimension.CONSISTENCY,
                            severity=Severity.MEDIUM,
                            field_name="value",
                            issue_type="observation_value_unit_mismatch",
                            description=f"Temperature value {value}°C seems inconsistent with Celsius unit",
                            suggested_fix="Verify temperature value and unit"
                        ))
                    elif unit.lower() in ['f', 'fahrenheit'] and not (85 <= value <= 115):
                        issues.append(QualityIssue(
                            dimension=QualityDimension.CONSISTENCY,
                            severity=Severity.MEDIUM,
                            field_name="value",
                            issue_type="observation_value_unit_mismatch",
                            description=f"Temperature value {value}°F seems inconsistent with Fahrenheit unit",
                            suggested_fix="Verify temperature value and unit"
                        ))
                checks += 1
        
        return checks
    
    def _infer_field_type(self, field_name: str) -> Optional[str]:
        """Infer field type from field name."""
        field_name = field_name.lower()
        
        if 'email' in field_name:
            return 'email'
        elif 'phone' in field_name or 'tel' in field_name:
            return 'phone'
        elif 'ssn' in field_name or 'social_security' in field_name:
            return 'ssn'
        elif 'mrn' in field_name or 'medical_record' in field_name:
            return 'mrn'
        elif field_name.endswith('_date') or field_name == 'date':
            return 'date'
        elif 'datetime' in field_name or 'timestamp' in field_name:
            return 'datetime'
        elif 'icd' in field_name:
            return 'icd10'
        elif 'cpt' in field_name:
            return 'cpt'
        elif 'loinc' in field_name:
            return 'loinc'
        elif 'snomed' in field_name:
            return 'snomed'
        
        return None


class OutlierDetector:
    """Detects statistical outliers and anomalies in data."""
    
    def __init__(self, z_threshold: float = 3.0, iqr_multiplier: float = 1.5):
        """
        Initialize outlier detector.
        
        Args:
            z_threshold: Z-score threshold for outlier detection
            iqr_multiplier: IQR multiplier for outlier detection
        """
        self.z_threshold = z_threshold
        self.iqr_multiplier = iqr_multiplier
        self.field_distributions: Dict[str, Dict[str, float]] = {}
    
    def update_distributions(self, data_batch: List[Dict[str, Any]]) -> None:
        """Update statistical distributions from a batch of data."""
        field_values = defaultdict(list)
        
        # Collect all numeric values by field
        for record in data_batch:
            for field_name, value in record.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    field_values[field_name].append(value)
        
        # Calculate statistics for each field
        for field_name, values in field_values.items():
            if len(values) >= 3:  # Need minimum samples for meaningful statistics
                self.field_distributions[field_name] = {
                    'mean': statistics.mean(values),
                    'stdev': statistics.stdev(values) if len(values) > 1 else 0,
                    'median': statistics.median(values),
                    'q1': statistics.quantiles(values, n=4)[0] if len(values) >= 4 else min(values),
                    'q3': statistics.quantiles(values, n=4)[2] if len(values) >= 4 else max(values),
                    'min': min(values),
                    'max': max(values),
                    'count': len(values)
                }
    
    def score(self, data: Dict[str, Any], record_type: str) -> Tuple[float, List[QualityIssue]]:
        """
        Calculate outlier score for a record.
        
        Args:
            data: Record data to evaluate
            record_type: Type of the record
            
        Returns:
            Tuple of (score, issues_list)
        """
        issues = []
        total_fields = 0
        outlier_fields = 0
        
        for field_name, value in data.items():
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                continue
                
            total_fields += 1
            
            if field_name in self.field_distributions:
                stats = self.field_distributions[field_name]
                
                # Z-score method
                if stats['stdev'] > 0:
                    z_score = abs(value - stats['mean']) / stats['stdev']
                    if z_score > self.z_threshold:
                        outlier_fields += 1
                        issues.append(QualityIssue(
                            dimension=QualityDimension.OUTLIER_DETECTION,
                            severity=Severity.MEDIUM if z_score < 4 else Severity.HIGH,
                            field_name=field_name,
                            issue_type="statistical_outlier_zscore",
                            description=f"Value {value} for '{field_name}' is a statistical outlier (z-score: {z_score:.2f})",
                            suggested_fix=f"Verify '{field_name}' value is correct",
                            metadata={"z_score": z_score, "mean": stats['mean'], "stdev": stats['stdev']}
                        ))
                        continue
                
                # IQR method
                iqr = stats['q3'] - stats['q1']
                lower_bound = stats['q1'] - (self.iqr_multiplier * iqr)
                upper_bound = stats['q3'] + (self.iqr_multiplier * iqr)
                
                if value < lower_bound or value > upper_bound:
                    outlier_fields += 1
                    issues.append(QualityIssue(
                        dimension=QualityDimension.OUTLIER_DETECTION,
                        severity=Severity.MEDIUM,
                        field_name=field_name,
                        issue_type="statistical_outlier_iqr",
                        description=f"Value {value} for '{field_name}' is outside IQR bounds ({lower_bound:.2f} - {upper_bound:.2f})",
                        suggested_fix=f"Verify '{field_name}' value is correct",
                        metadata={"iqr_lower": lower_bound, "iqr_upper": upper_bound, "iqr": iqr}
                    ))
                    continue
            
            # Domain-specific outlier detection
            domain_outliers = self._detect_domain_outliers(field_name, value)
            if domain_outliers:
                outlier_fields += 1
                issues.extend(domain_outliers)
        
        # Score is percentage of non-outlier fields
        outlier_score = 1.0 - (outlier_fields / total_fields) if total_fields > 0 else 1.0
        return outlier_score, issues
    
    def _detect_domain_outliers(self, field_name: str, value: Union[int, float]) -> List[QualityIssue]:
        """Detect domain-specific outliers based on medical knowledge."""
        issues = []
        field_lower = field_name.lower()
        
        # Age outliers
        if 'age' in field_lower and (value < 0 or value > 150):
            issues.append(QualityIssue(
                dimension=QualityDimension.OUTLIER_DETECTION,
                severity=Severity.HIGH if value < 0 or value > 200 else Severity.MEDIUM,
                field_name=field_name,
                issue_type="domain_outlier",
                description=f"Age value {value} is outside realistic range",
                suggested_fix="Verify age value is correct"
            ))
        
        # Heart rate outliers
        elif 'heart_rate' in field_lower or 'pulse' in field_lower:
            if value < 20 or value > 300:
                issues.append(QualityIssue(
                    dimension=QualityDimension.OUTLIER_DETECTION,
                    severity=Severity.HIGH if value < 10 or value > 400 else Severity.MEDIUM,
                    field_name=field_name,
                    issue_type="domain_outlier",
                    description=f"Heart rate {value} is outside realistic range",
                    suggested_fix="Verify heart rate measurement"
                ))
        
        # Blood pressure outliers
        elif 'blood_pressure' in field_lower or 'systolic' in field_lower:
            if value < 50 or value > 300:
                issues.append(QualityIssue(
                    dimension=QualityDimension.OUTLIER_DETECTION,
                    severity=Severity.HIGH if value < 30 or value > 350 else Severity.MEDIUM,
                    field_name=field_name,
                    issue_type="domain_outlier",
                    description=f"Blood pressure {value} is outside realistic range",
                    suggested_fix="Verify blood pressure measurement"
                ))
        
        # Weight outliers
        elif 'weight' in field_lower:
            if 'kg' in field_lower and (value < 0.5 or value > 1000):
                issues.append(QualityIssue(
                    dimension=QualityDimension.OUTLIER_DETECTION,
                    severity=Severity.HIGH if value < 0 or value > 1500 else Severity.MEDIUM,
                    field_name=field_name,
                    issue_type="domain_outlier",
                    description=f"Weight {value} kg is outside realistic range",
                    suggested_fix="Verify weight measurement and unit"
                ))
            elif 'lb' in field_lower and (value < 1 or value > 2000):
                issues.append(QualityIssue(
                    dimension=QualityDimension.OUTLIER_DETECTION,
                    severity=Severity.HIGH if value < 0 or value > 3000 else Severity.MEDIUM,
                    field_name=field_name,
                    issue_type="domain_outlier",
                    description=f"Weight {value} lb is outside realistic range",
                    suggested_fix="Verify weight measurement and unit"
                ))
        
        # Temperature outliers
        elif 'temperature' in field_lower:
            if 'celsius' in field_lower and (value < 30 or value > 45):
                issues.append(QualityIssue(
                    dimension=QualityDimension.OUTLIER_DETECTION,
                    severity=Severity.HIGH if value < 20 or value > 50 else Severity.MEDIUM,
                    field_name=field_name,
                    issue_type="domain_outlier",
                    description=f"Temperature {value}°C is outside realistic range",
                    suggested_fix="Verify temperature measurement and unit"
                ))
            elif 'fahrenheit' in field_lower and (value < 85 or value > 115):
                issues.append(QualityIssue(
                    dimension=QualityDimension.OUTLIER_DETECTION,
                    severity=Severity.HIGH if value < 70 or value > 130 else Severity.MEDIUM,
                    field_name=field_name,
                    issue_type="domain_outlier",
                    description=f"Temperature {value}°F is outside realistic range",
                    suggested_fix="Verify temperature measurement and unit"
                ))
        
        return issues


class DataUsageAnalyzer:
    """Analyzes data presence vs actual usage in processing pipeline."""
    
    def __init__(self):
        """Initialize data usage analyzer."""
        self.field_usage_tracking: Dict[str, Set[str]] = defaultdict(set)
        self.processing_stages = [
            "ingestion", "validation", "normalization", "chunking", 
            "embedding", "storage", "retrieval", "analysis"
        ]
    
    def track_field_usage(self, stage: str, record_data: Dict[str, Any], 
                         used_fields: List[str]) -> None:
        """
        Track which fields are actually used in each processing stage.
        
        Args:
            stage: Processing stage name
            record_data: The record being processed
            used_fields: List of fields actually accessed/used
        """
        record_key = f"{stage}:{record_data.get('id', 'unknown')}"
        self.field_usage_tracking[record_key].update(used_fields)
    
    def score(self, data: Dict[str, Any], record_type: str,
              usage_context: Optional[Dict[str, List[str]]] = None) -> Tuple[float, List[QualityIssue]]:
        """
        Calculate data usage score for a record.
        
        Args:
            data: Record data to evaluate
            record_type: Type of the record
            usage_context: Dict mapping stages to lists of used fields
            
        Returns:
            Tuple of (score, issues_list)
        """
        issues = []
        
        if not usage_context:
            # Without usage context, we can only do basic analysis
            return self._basic_usage_analysis(data)
        
        all_fields = set(data.keys())
        used_fields = set()
        
        # Collect all used fields across stages
        for stage_fields in usage_context.values():
            used_fields.update(stage_fields)
        
        unused_fields = all_fields - used_fields
        
        # Analyze unused fields
        for field_name in unused_fields:
            value = data[field_name]
            
            # Skip empty/None values - not really "unused" if empty
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            
            # Determine severity based on field importance
            severity = self._assess_field_importance(field_name, record_type)
            
            issues.append(QualityIssue(
                dimension=QualityDimension.DATA_USAGE,
                severity=severity,
                field_name=field_name,
                issue_type="unused_data",
                description=f"Field '{field_name}' contains data but is not used in processing",
                suggested_fix=f"Consider utilizing '{field_name}' in processing pipeline or remove if unnecessary",
                metadata={"value_type": type(value).__name__, "value_length": len(str(value))}
            ))
        
        # Calculate usage score
        usage_score = len(used_fields) / len(all_fields) if all_fields else 1.0
        
        # Bonus for using important fields
        important_fields = self._get_important_fields(record_type)
        important_used = len(used_fields.intersection(important_fields))
        important_total = len(important_fields.intersection(all_fields))
        
        if important_total > 0:
            importance_bonus = (important_used / important_total) * 0.2
            usage_score = min(1.0, usage_score + importance_bonus)
        
        return usage_score, issues
    
    def _basic_usage_analysis(self, data: Dict[str, Any]) -> Tuple[float, List[QualityIssue]]:
        """Perform basic usage analysis without tracking context."""
        issues = []
        
        # Look for potentially redundant or unnecessary fields
        redundant_patterns = [
            r'_backup$', r'_old$', r'_temp$', r'_copy$',
            r'^debug_', r'^test_', r'^temp_'
        ]
        
        for field_name, value in data.items():
            # Skip empty values
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            
            # Check for redundant field patterns
            for pattern in redundant_patterns:
                if re.search(pattern, field_name.lower()):
                    issues.append(QualityIssue(
                        dimension=QualityDimension.DATA_USAGE,
                        severity=Severity.LOW,
                        field_name=field_name,
                        issue_type="potentially_redundant",
                        description=f"Field '{field_name}' appears to be temporary or redundant",
                        suggested_fix=f"Review necessity of field '{field_name}'"
                    ))
                    break
        
        # Base score assumes reasonable usage
        base_score = 0.8
        
        # Reduce score for each redundant field
        redundant_count = len(issues)
        total_fields = len([v for v in data.values() if v is not None])
        
        if total_fields > 0:
            redundancy_penalty = (redundant_count / total_fields) * 0.3
            usage_score = max(0.0, base_score - redundancy_penalty)
        else:
            usage_score = 1.0
        
        return usage_score, issues
    
    def _assess_field_importance(self, field_name: str, record_type: str) -> Severity:
        """Assess the importance of a field for determining issue severity."""
        field_lower = field_name.lower()
        
        # Critical fields
        critical_patterns = ['id', 'patient_id', 'subject', 'code', 'status']
        if any(pattern in field_lower for pattern in critical_patterns):
            return Severity.HIGH
        
        # Important fields
        important_patterns = ['name', 'date', 'value', 'result', 'diagnosis', 'medication']
        if any(pattern in field_lower for pattern in important_patterns):
            return Severity.MEDIUM
        
        # Record type specific importance
        if record_type == "Patient":
            patient_important = ['birth_date', 'gender', 'address', 'phone']
            if any(pattern in field_lower for pattern in patient_important):
                return Severity.MEDIUM
        
        # Default to low importance
        return Severity.LOW
    
    def _get_important_fields(self, record_type: str) -> Set[str]:
        """Get list of important fields for a record type."""
        important_fields = {'id', 'patient_id', 'subject_id', 'code', 'status'}
        
        type_specific = {
            "Patient": {'name', 'birth_date', 'gender', 'mrn'},
            "Observation": {'value', 'unit', 'code', 'effective_date'},
            "Medication": {'code', 'dosage', 'route', 'frequency'},
            "Procedure": {'code', 'performed_date', 'status'},
            "Condition": {'code', 'onset_date', 'severity'},
            "Encounter": {'class', 'period', 'reason_code'},
            "DiagnosticReport": {'code', 'result', 'effective_date'}
        }
        
        if record_type in type_specific:
            important_fields.update(type_specific[record_type])
        
        return important_fields


class DataQualityScorer:
    """Main data quality scoring engine that coordinates all quality assessments."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize data quality scorer.
        
        Args:
            config: Configuration for quality scoring
        """
        self.config = config or {}
        self.weights = self.config.get('weights', {
            'completeness': 0.25,
            'consistency': 0.25,
            'validity': 0.15,
            'accuracy': 0.15,
            'outlier_detection': 0.10,
            'data_usage': 0.10
        })
        
        # Initialize individual scorers
        self.completeness_scorer = CompletenessScorer(
            required_fields=self.config.get('required_fields'),
            optional_fields=self.config.get('optional_fields')
        )
        self.consistency_scorer = ConsistencyScorer()
        self.outlier_detector = OutlierDetector(
            z_threshold=self.config.get('z_threshold', 3.0),
            iqr_multiplier=self.config.get('iqr_multiplier', 1.5)
        )
        self.data_usage_analyzer = DataUsageAnalyzer()
        
        # Tracking for batch updates
        self.processed_records: List[Dict[str, Any]] = []
        self.batch_size = self.config.get('batch_size', 100)
    
    def score_record(self, data: Dict[str, Any], record_type: str,
                    record_id: Optional[str] = None,
                    context_data: Optional[List[Dict[str, Any]]] = None,
                    usage_context: Optional[Dict[str, List[str]]] = None) -> QualityScore:
        """
        Calculate comprehensive quality score for a single record.
        
        Args:
            data: Record data to evaluate
            record_type: Type of the record
            record_id: Unique identifier for the record
            context_data: Other records for cross-record consistency
            usage_context: Field usage information by processing stage
            
        Returns:
            Complete quality score with all dimensions
        """
        record_id = record_id or data.get('id', f"unknown_{len(self.processed_records)}")
        all_issues = []
        
        # Completeness scoring
        completeness_score, completeness_issues = self.completeness_scorer.score(data, record_type)
        all_issues.extend(completeness_issues)
        
        # Consistency scoring
        consistency_score, consistency_issues = self.consistency_scorer.score(
            data, record_type, context_data
        )
        all_issues.extend(consistency_issues)
        
        # Outlier detection
        outlier_score, outlier_issues = self.outlier_detector.score(data, record_type)
        all_issues.extend(outlier_issues)
        
        # Data usage analysis
        data_usage_score, usage_issues = self.data_usage_analyzer.score(
            data, record_type, usage_context
        )
        all_issues.extend(usage_issues)
        
        # Validity and accuracy scores (basic implementation)
        validity_score = self._calculate_validity_score(data, all_issues)
        accuracy_score = self._calculate_accuracy_score(data, record_type)
        
        # Calculate weighted overall score
        overall_score = (
            completeness_score * self.weights['completeness'] +
            consistency_score * self.weights['consistency'] +
            validity_score * self.weights['validity'] +
            accuracy_score * self.weights['accuracy'] +
            outlier_score * self.weights['outlier_detection'] +
            data_usage_score * self.weights['data_usage']
        )
        
        # Collect field lists for summary
        missing_fields = [issue.field_name for issue in completeness_issues 
                         if issue.issue_type == "missing_required"]
        invalid_fields = [issue.field_name for issue in all_issues 
                         if issue.dimension == QualityDimension.CONSISTENCY]
        outlier_fields = [issue.field_name for issue in outlier_issues]
        unused_fields = [issue.field_name for issue in usage_issues]
        
        quality_score = QualityScore(
            record_id=record_id,
            record_type=record_type,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            validity_score=validity_score,
            accuracy_score=accuracy_score,
            outlier_score=outlier_score,
            data_usage_score=data_usage_score,
            overall_score=overall_score,
            issues=all_issues,
            missing_fields=missing_fields,
            invalid_fields=invalid_fields,
            outlier_fields=outlier_fields,
            unused_fields=unused_fields,
            metadata={
                "weights": self.weights,
                "total_fields": len(data),
                "total_issues": len(all_issues),
                "high_severity_issues": len([i for i in all_issues if i.severity == Severity.HIGH]),
                "critical_issues": len([i for i in all_issues if i.severity == Severity.CRITICAL])
            }
        )
        
        # Add to batch for distribution updates
        self.processed_records.append(data)
        if len(self.processed_records) >= self.batch_size:
            self._update_distributions()
        
        return quality_score
    
    def score_batch(self, records: List[Dict[str, Any]], record_type: str) -> List[QualityScore]:
        """Score a batch of records efficiently."""
        # Update outlier detection distributions first
        self.outlier_detector.update_distributions(records)
        
        scores = []
        for i, record in enumerate(records):
            record_id = record.get('id', f"{record_type}_{i}")
            context_data = records[:i] + records[i+1:]  # All other records as context
            
            score = self.score_record(
                data=record,
                record_type=record_type,
                record_id=record_id,
                context_data=context_data
            )
            scores.append(score)
        
        return scores
    
    def get_aggregate_score(self, scores: List[QualityScore]) -> Dict[str, Any]:
        """Calculate aggregate quality metrics across multiple records."""
        if not scores:
            return {}
        
        total_records = len(scores)
        
        return {
            "total_records": total_records,
            "avg_completeness": sum(s.completeness_score for s in scores) / total_records,
            "avg_consistency": sum(s.consistency_score for s in scores) / total_records,
            "avg_validity": sum(s.validity_score for s in scores) / total_records,
            "avg_accuracy": sum(s.accuracy_score for s in scores) / total_records,
            "avg_outlier_score": sum(s.outlier_score for s in scores) / total_records,
            "avg_data_usage": sum(s.data_usage_score for s in scores) / total_records,
            "avg_overall_score": sum(s.overall_score for s in scores) / total_records,
            "min_overall_score": min(s.overall_score for s in scores),
            "max_overall_score": max(s.overall_score for s in scores),
            "total_issues": sum(len(s.issues) for s in scores),
            "high_severity_issues": sum(len([i for i in s.issues if i.severity == Severity.HIGH]) for s in scores),
            "critical_issues": sum(len([i for i in s.issues if i.severity == Severity.CRITICAL]) for s in scores),
            "records_with_issues": sum(1 for s in scores if s.issues),
            "issue_rate": sum(1 for s in scores if s.issues) / total_records,
            "common_issues": self._get_common_issues(scores),
            "quality_distribution": self._get_quality_distribution(scores)
        }
    
    def _calculate_validity_score(self, data: Dict[str, Any], issues: List[QualityIssue]) -> float:
        """Calculate validity score based on format and constraint violations."""
        validity_issues = [i for i in issues if 'format' in i.issue_type or 'constraint' in i.issue_type]
        total_fields = len([v for v in data.values() if v is not None])
        
        if total_fields == 0:
            return 1.0
        
        invalid_fields = len(set(issue.field_name for issue in validity_issues))
        return max(0.0, 1.0 - (invalid_fields / total_fields))
    
    def _calculate_accuracy_score(self, data: Dict[str, Any], record_type: str) -> float:
        """Calculate accuracy score based on domain knowledge and expected values."""
        # Basic accuracy scoring - in practice this would involve external validation
        score = 1.0
        
        # Check for obviously inaccurate values
        for field_name, value in data.items():
            if isinstance(value, str) and value.strip().lower() in [
                'test', 'dummy', 'fake', 'sample', 'example', 'lorem ipsum'
            ]:
                score -= 0.1
            elif isinstance(value, (int, float)) and value == 0 and 'id' in field_name.lower():
                score -= 0.2  # ID fields shouldn't be 0
        
        return max(0.0, score)
    
    def _update_distributions(self) -> None:
        """Update statistical distributions with processed records."""
        if self.processed_records:
            self.outlier_detector.update_distributions(self.processed_records)
            self.processed_records.clear()
    
    def _get_common_issues(self, scores: List[QualityScore]) -> List[Dict[str, Any]]:
        """Get most common quality issues across records."""
        issue_counter = Counter()
        
        for score in scores:
            for issue in score.issues:
                issue_key = f"{issue.dimension.value}:{issue.issue_type}"
                issue_counter[issue_key] += 1
        
        common_issues = []
        for (dimension_type, count) in issue_counter.most_common(10):
            dimension, issue_type = dimension_type.split(':', 1)
            common_issues.append({
                "dimension": dimension,
                "issue_type": issue_type,
                "count": count,
                "percentage": (count / len(scores)) * 100
            })
        
        return common_issues
    
    def _get_quality_distribution(self, scores: List[QualityScore]) -> Dict[str, int]:
        """Get distribution of records by quality score ranges."""
        distribution = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
        
        for score in scores:
            overall = score.overall_score
            if overall >= 0.9:
                distribution["excellent"] += 1
            elif overall >= 0.7:
                distribution["good"] += 1
            elif overall >= 0.5:
                distribution["fair"] += 1
            else:
                distribution["poor"] += 1
        
        return distribution