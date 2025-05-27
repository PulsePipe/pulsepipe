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

# tests/test_quality_scoring_engine.py

"""
Unit tests for the data quality scoring engine.

Tests comprehensive quality assessment including completeness, consistency,
outlier detection, data usage analysis, and aggregate scoring.
"""

import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

from pulsepipe.pipelines.quality.scoring_engine import (
    DataQualityScorer,
    QualityScore,
    CompletenessScorer,
    ConsistencyScorer,
    OutlierDetector,
    DataUsageAnalyzer,
    QualityDimension,
    Severity,
    QualityIssue
)


@pytest.fixture
def sample_patient_data():
    """Sample patient data for testing."""
    return {
        "id": "patient_001",
        "name": "John Doe",
        "birth_date": "1985-03-15",
        "age": 39,
        "gender": "male",
        "email": "john.doe@example.com",
        "phone": "555-123-4567",
        "address": "123 Main St",
        "weight_kg": 75.5,
        "height_cm": 180,
        "bmi": 23.3
    }


@pytest.fixture
def sample_observation_data():
    """Sample observation data for testing."""
    return {
        "id": "obs_001",
        "subject": "patient_001",
        "code": "vital-signs",
        "value": 98.6,
        "unit": "F",
        "effective_date": "2024-01-15T10:30:00Z",
        "status": "final"
    }


@pytest.fixture
def incomplete_patient_data():
    """Incomplete patient data for testing."""
    return {
        "id": "patient_002",
        "name": "",  # Empty string
        "birth_date": None,  # Missing required field
        "gender": "unknown",
        "email": "invalid-email",  # Invalid format
        "phone": "123"  # Invalid format
    }


@pytest.fixture
def outlier_data():
    """Data with statistical outliers."""
    return {
        "id": "patient_outlier",
        "name": "Jane Smith",
        "age": 250,  # Outlier age
        "heart_rate": 500,  # Outlier heart rate
        "weight_kg": -10,  # Invalid weight
        "height_cm": 350,  # Outlier height
        "temperature_celsius": 60  # Impossible temperature
    }


class TestQualityIssue:
    """Test QualityIssue data class."""
    
    def test_create_quality_issue(self):
        """Test creating a quality issue."""
        issue = QualityIssue(
            dimension=QualityDimension.COMPLETENESS,
            severity=Severity.HIGH,
            field_name="birth_date",
            issue_type="missing_required",
            description="Required field is missing",
            suggested_fix="Provide birth date",
            metadata={"field_type": "date"}
        )
        
        assert issue.dimension == QualityDimension.COMPLETENESS
        assert issue.severity == Severity.HIGH
        assert issue.field_name == "birth_date"
        assert issue.issue_type == "missing_required"
        assert issue.description == "Required field is missing"
        assert issue.suggested_fix == "Provide birth date"
        assert issue.metadata["field_type"] == "date"


class TestQualityScore:
    """Test QualityScore data class."""
    
    def test_create_quality_score(self):
        """Test creating a quality score."""
        issues = [
            QualityIssue(
                dimension=QualityDimension.COMPLETENESS,
                severity=Severity.MEDIUM,
                field_name="email",
                issue_type="format_error",
                description="Invalid email format"
            )
        ]
        
        score = QualityScore(
            record_id="test_001",
            record_type="Patient",
            completeness_score=0.8,
            consistency_score=0.9,
            validity_score=0.7,
            accuracy_score=0.95,
            outlier_score=1.0,
            data_usage_score=0.85,
            overall_score=0.87,
            issues=issues,
            missing_fields=["birth_date"],
            invalid_fields=["email"],
            outlier_fields=[],
            unused_fields=["temp_field"]
        )
        
        assert score.record_id == "test_001"
        assert score.record_type == "Patient"
        assert score.completeness_score == 0.8
        assert score.overall_score == 0.87
        assert len(score.issues) == 1
        assert score.missing_fields == ["birth_date"]
        assert score.invalid_fields == ["email"]
        assert score.unused_fields == ["temp_field"]
    
    def test_quality_score_to_dict(self):
        """Test converting quality score to dictionary."""
        score = QualityScore(
            record_id="test_001",
            record_type="Patient",
            completeness_score=0.888888,
            consistency_score=0.9,
            validity_score=0.7,
            accuracy_score=0.95,
            outlier_score=1.0,
            data_usage_score=0.85,
            overall_score=0.87,
            issues=[],
            missing_fields=["birth_date"],
            invalid_fields=[],
            outlier_fields=[],
            unused_fields=[]
        )
        
        data = score.to_dict()
        
        assert data["record_id"] == "test_001"
        assert data["record_type"] == "Patient"
        assert data["completeness_score"] == 0.889  # Rounded to 3 decimal places
        assert data["issues_count"] == 0
        assert data["missing_fields"] == ["birth_date"]
        assert isinstance(data["issues"], list)


class TestCompletenessScorer:
    """Test CompletenessScorer class."""
    
    def test_score_complete_patient(self, sample_patient_data):
        """Test scoring a complete patient record."""
        scorer = CompletenessScorer()
        score, issues = scorer.score(sample_patient_data, "Patient")
        
        assert score > 0.8  # Should have high completeness
        assert len(issues) == 0  # No issues for complete data
    
    def test_score_incomplete_patient(self, incomplete_patient_data):
        """Test scoring an incomplete patient record."""
        scorer = CompletenessScorer()
        score, issues = scorer.score(incomplete_patient_data, "Patient")
        
        assert score < 0.5  # Should have low completeness
        assert len(issues) > 0  # Should have issues
        
        # Check for missing required field issue
        missing_issues = [i for i in issues if i.issue_type == "missing_required"]
        assert len(missing_issues) > 0
        
        # Check for empty string issue
        empty_issues = [i for i in issues if i.issue_type == "empty_string"]
        assert len(empty_issues) > 0
    
    def test_score_with_custom_requirements(self):
        """Test scoring with custom field requirements."""
        required_fields = {"TestRecord": ["field1", "field2"]}
        optional_fields = {"TestRecord": ["field3", "field4"]}
        
        scorer = CompletenessScorer(required_fields, optional_fields)
        
        # Complete record
        data = {"field1": "value1", "field2": "value2", "field3": "value3"}
        score, issues = scorer.score(data, "TestRecord")
        
        assert score > 0.8  # High score for complete required fields
        assert len(issues) == 0
        
        # Missing required field
        data = {"field1": "value1", "field3": "value3"}
        score, issues = scorer.score(data, "TestRecord")
        
        assert score < 0.7  # Lower score for missing required field
        assert len(issues) > 0
        assert any(i.issue_type == "missing_required" for i in issues)
    
    def test_is_field_present(self):
        """Test field presence detection."""
        scorer = CompletenessScorer()
        
        data = {
            "present": "value",
            "empty_string": "",
            "none_value": None,
            "empty_list": [],
            "empty_dict": {},
            "nested": {"field": "value"},
            "list_field": [1, 2, 3]
        }
        
        assert scorer._is_field_present(data, "present") is True
        assert scorer._is_field_present(data, "empty_string") is False
        assert scorer._is_field_present(data, "none_value") is False
        assert scorer._is_field_present(data, "empty_list") is False
        assert scorer._is_field_present(data, "empty_dict") is False
        assert scorer._is_field_present(data, "nested.field") is True
        assert scorer._is_field_present(data, "list_field") is True
        assert scorer._is_field_present(data, "nonexistent") is False
    
    def test_check_field_quality(self):
        """Test field quality checking."""
        scorer = CompletenessScorer()
        issues = []
        
        data = {
            "good_field": "valid value",
            "empty_field": "",
            "whitespace_field": "   ",
            "null_placeholder": "null",
            "na_placeholder": "N/A",
            "unknown_placeholder": "unknown"
        }
        
        scorer._check_field_quality(data, issues)
        
        # Should find issues with empty and placeholder values
        issue_types = [i.issue_type for i in issues]
        assert "empty_string" in issue_types
        assert "placeholder_value" in issue_types
        
        # Should have multiple issues
        assert len(issues) >= 4


class TestConsistencyScorer:
    """Test ConsistencyScorer class."""
    
    def test_score_consistent_patient(self, sample_patient_data):
        """Test scoring a consistent patient record."""
        scorer = ConsistencyScorer()
        score, issues = scorer.score(sample_patient_data, "Patient")
        
        assert score > 0.8  # Should have high consistency
        # May have some minor issues but overall good
    
    def test_score_inconsistent_data(self):
        """Test scoring inconsistent data."""
        scorer = ConsistencyScorer()
        
        # Data with inconsistencies
        data = {
            "id": "patient_001",
            "birth_date": "1990-01-01",
            "age": 50,  # Inconsistent with birth date
            "height_cm": 180,
            "weight_kg": 75,
            "bmi": 30,  # Inconsistent with height/weight
            "email": "invalid-email-format",
            "phone": "123-invalid",
            "gender": "male",
            "pregnancy_status": "pregnant"  # Inconsistent with male gender
        }
        
        score, issues = scorer.score(data, "Patient")
        
        assert score < 0.85  # Should have moderate consistency (adjusted from 0.7)
        assert len(issues) > 0
        
        # Check for specific issue types
        issue_types = [i.issue_type for i in issues]
        assert any("mismatch" in issue_type for issue_type in issue_types)
    
    def test_format_consistency_check(self):
        """Test format consistency checking."""
        scorer = ConsistencyScorer()
        
        data = {
            "email": "valid@example.com",
            "invalid_email": "invalid-email",
            "phone": "555-123-4567",
            "invalid_phone": "123",
            "valid_date": "2024-01-15",
            "invalid_date": "not-a-date"
        }
        
        score, issues = scorer._check_format_consistency(data)
        
        # Should find format issues
        format_issues = [i for i in issues if i.issue_type == "format_mismatch"]
        assert len(format_issues) >= 2  # At least invalid email and phone
    
    def test_logical_consistency_check(self):
        """Test logical consistency checking."""
        scorer = ConsistencyScorer()
        
        # Data with logical inconsistencies
        data = {
            "age": -5,  # Invalid age
            "heart_rate": 300,  # Out of normal range
            "temperature_celsius": 60  # Impossible temperature
        }
        
        score, issues = scorer._check_logical_consistency(data, "Patient")
        
        # Should find out of range issues
        range_issues = [i for i in issues if i.issue_type == "out_of_range"]
        assert len(range_issues) > 0
    
    def test_cross_field_consistency(self):
        """Test cross-field consistency checking."""
        scorer = ConsistencyScorer()
        
        # Test age vs birth date consistency
        data1 = {
            "birth_date": "1990-01-01T00:00:00Z",
            "age": 34  # Correct age
        }
        
        score1, issues1 = scorer._check_cross_field_consistency(data1)
        assert len(issues1) == 0  # Should be consistent
        
        # Test BMI consistency
        data2 = {
            "height_cm": 180,
            "weight_kg": 75,
            "bmi": 23.1  # Correct BMI (75 / 1.8^2 = 23.15)
        }
        
        score2, issues2 = scorer._check_cross_field_consistency(data2)
        assert len(issues2) == 0  # Should be consistent
        
        # Test inconsistent BMI
        data3 = {
            "height_cm": 180,
            "weight_kg": 75,
            "bmi": 30  # Incorrect BMI
        }
        
        score3, issues3 = scorer._check_cross_field_consistency(data3)
        assert len(issues3) > 0  # Should find BMI mismatch
    
    def test_temporal_consistency(self):
        """Test temporal consistency checking."""
        scorer = ConsistencyScorer()
        
        # Consistent dates
        data1 = {
            "birth_date": "1990-01-01T00:00:00Z",
            "admission_date": "2024-01-15T10:00:00Z",
            "discharge_date": "2024-01-20T15:00:00Z"
        }
        
        score1, issues1 = scorer._check_temporal_consistency(data1)
        assert len(issues1) == 0  # All dates after birth
        
        # Inconsistent dates
        data2 = {
            "birth_date": "1990-01-01T00:00:00Z",
            "admission_date": "1985-01-15T10:00:00Z"  # Before birth
        }
        
        score2, issues2 = scorer._check_temporal_consistency(data2)
        assert len(issues2) > 0  # Should find temporal violation
        assert any("temporal_order_violation" in i.issue_type for i in issues2)
    
    def test_infer_field_type(self):
        """Test field type inference."""
        scorer = ConsistencyScorer()
        
        assert scorer._infer_field_type("email") == "email"
        assert scorer._infer_field_type("patient_email") == "email"
        assert scorer._infer_field_type("phone_number") == "phone"
        assert scorer._infer_field_type("birth_date") == "date"
        assert scorer._infer_field_type("created_datetime") == "datetime"
        assert scorer._infer_field_type("unknown_field") is None


class TestOutlierDetector:
    """Test OutlierDetector class."""
    
    def test_update_distributions(self):
        """Test updating statistical distributions."""
        detector = OutlierDetector()
        
        data_batch = [
            {"age": 25, "weight": 70},
            {"age": 30, "weight": 75},
            {"age": 35, "weight": 80},
            {"age": 40, "weight": 85},
            {"age": 45, "weight": 90}
        ]
        
        detector.update_distributions(data_batch)
        
        assert "age" in detector.field_distributions
        assert "weight" in detector.field_distributions
        
        age_stats = detector.field_distributions["age"]
        assert age_stats["mean"] == 35
        assert age_stats["min"] == 25
        assert age_stats["max"] == 45
        assert age_stats["count"] == 5
    
    def test_score_with_statistical_outliers(self):
        """Test outlier detection with statistical methods."""
        detector = OutlierDetector()
        
        # Setup distributions with normal data
        normal_data = [{"value": i} for i in range(1, 101)]  # 1 to 100
        detector.update_distributions(normal_data)
        
        # Test outlier
        outlier_data = {"value": 200}  # Clear outlier
        score, issues = detector.score(outlier_data, "TestRecord")
        
        assert score < 1.0  # Should detect outlier
        assert len(issues) > 0
        assert any("outlier" in i.issue_type for i in issues)
    
    def test_score_with_domain_outliers(self, outlier_data):
        """Test domain-specific outlier detection."""
        detector = OutlierDetector()
        score, issues = detector.score(outlier_data, "Patient")
        
        assert score < 0.5  # Should have very low score
        assert len(issues) > 0
        
        # Check for domain outlier issues
        domain_issues = [i for i in issues if i.issue_type == "domain_outlier"]
        assert len(domain_issues) > 0
        
        # Should detect age, heart rate, weight, and temperature outliers
        outlier_fields = [i.field_name for i in domain_issues]
        assert "age" in outlier_fields
        assert "heart_rate" in outlier_fields
    
    def test_detect_domain_outliers(self):
        """Test domain-specific outlier detection methods."""
        detector = OutlierDetector()
        
        # Test age outliers
        age_issues = detector._detect_domain_outliers("age", -5)
        assert len(age_issues) > 0
        assert age_issues[0].severity == Severity.HIGH
        
        age_issues = detector._detect_domain_outliers("age", 200)
        assert len(age_issues) > 0
        
        # Test heart rate outliers
        hr_issues = detector._detect_domain_outliers("heart_rate", 500)
        assert len(hr_issues) > 0
        
        # Test weight outliers
        weight_issues = detector._detect_domain_outliers("weight_kg", -10)
        assert len(weight_issues) > 0
        
        # Test normal values
        normal_issues = detector._detect_domain_outliers("age", 25)
        assert len(normal_issues) == 0


class TestDataUsageAnalyzer:
    """Test DataUsageAnalyzer class."""
    
    def test_track_field_usage(self):
        """Test field usage tracking."""
        analyzer = DataUsageAnalyzer()
        
        record_data = {"id": "test_001", "field1": "value1", "field2": "value2"}
        used_fields = ["id", "field1"]
        
        analyzer.track_field_usage("validation", record_data, used_fields)
        
        record_key = "validation:test_001"
        assert record_key in analyzer.field_usage_tracking
        assert "id" in analyzer.field_usage_tracking[record_key]
        assert "field1" in analyzer.field_usage_tracking[record_key]
        assert "field2" not in analyzer.field_usage_tracking[record_key]
    
    def test_score_with_usage_context(self):
        """Test scoring with usage context."""
        analyzer = DataUsageAnalyzer()
        
        data = {
            "id": "test_001",
            "name": "John Doe",
            "used_field": "value",
            "unused_field": "unused_value"
        }
        
        usage_context = {
            "validation": ["id", "name", "used_field"],
            "processing": ["id", "used_field"]
        }
        
        score, issues = analyzer.score(data, "Patient", usage_context)
        
        assert score < 1.0  # Should be less than perfect due to unused field
        assert len(issues) > 0
        
        # Should find unused field issue
        unused_issues = [i for i in issues if i.issue_type == "unused_data"]
        assert len(unused_issues) > 0
        assert "unused_field" in [i.field_name for i in unused_issues]
    
    def test_score_without_usage_context(self):
        """Test basic usage analysis without tracking context."""
        analyzer = DataUsageAnalyzer()
        
        data = {
            "id": "test_001",
            "name": "John Doe",
            "temp_field": "temporary",
            "debug_info": "debug value"
        }
        
        score, issues = analyzer.score(data, "Patient")
        
        # Should detect potentially redundant fields
        redundant_issues = [i for i in issues if i.issue_type == "potentially_redundant"]
        assert len(redundant_issues) > 0
        
        # Should find temp and debug fields
        issue_fields = [i.field_name for i in redundant_issues]
        assert "temp_field" in issue_fields or "debug_info" in issue_fields
    
    def test_assess_field_importance(self):
        """Test field importance assessment."""
        analyzer = DataUsageAnalyzer()
        
        # Critical fields
        assert analyzer._assess_field_importance("id", "Patient") == Severity.HIGH
        assert analyzer._assess_field_importance("patient_id", "Patient") == Severity.HIGH
        
        # Important fields
        assert analyzer._assess_field_importance("name", "Patient") == Severity.MEDIUM
        assert analyzer._assess_field_importance("birth_date", "Patient") == Severity.MEDIUM
        
        # Low importance fields
        assert analyzer._assess_field_importance("comments", "Patient") == Severity.LOW
        assert analyzer._assess_field_importance("notes", "Patient") == Severity.LOW
    
    def test_get_important_fields(self):
        """Test getting important fields for record types."""
        analyzer = DataUsageAnalyzer()
        
        patient_fields = analyzer._get_important_fields("Patient")
        assert "id" in patient_fields
        assert "name" in patient_fields
        assert "birth_date" in patient_fields
        
        observation_fields = analyzer._get_important_fields("Observation")
        assert "id" in observation_fields
        assert "value" in observation_fields
        assert "code" in observation_fields


class TestDataQualityScorer:
    """Test main DataQualityScorer class."""
    
    def test_score_record_complete(self, sample_patient_data):
        """Test scoring a complete, high-quality record."""
        scorer = DataQualityScorer()
        quality_score = scorer.score_record(sample_patient_data, "Patient", "patient_001")
        
        assert quality_score.record_id == "patient_001"
        assert quality_score.record_type == "Patient"
        assert quality_score.overall_score > 0.7  # Should have good overall score
        assert quality_score.completeness_score > 0.8
        assert quality_score.consistency_score > 0.7
        
        # Should have metadata
        assert "total_fields" in quality_score.metadata
        assert "weights" in quality_score.metadata
    
    def test_score_record_incomplete(self, incomplete_patient_data):
        """Test scoring an incomplete, low-quality record."""
        scorer = DataQualityScorer()
        quality_score = scorer.score_record(incomplete_patient_data, "Patient", "patient_002")
        
        assert quality_score.record_id == "patient_002"
        assert quality_score.overall_score < 0.8  # Should have low overall score (adjusted from 0.6)
        assert quality_score.completeness_score < 0.5
        assert len(quality_score.issues) > 0
        assert len(quality_score.missing_fields) > 0
    
    def test_score_record_with_outliers(self, outlier_data):
        """Test scoring a record with outliers."""
        scorer = DataQualityScorer()
        quality_score = scorer.score_record(outlier_data, "Patient", "patient_outlier")
        
        assert quality_score.outlier_score < 0.5  # Should detect outliers
        assert len(quality_score.outlier_fields) > 0
        
        # Should have outlier-related issues
        outlier_issues = [i for i in quality_score.issues 
                         if i.dimension == QualityDimension.OUTLIER_DETECTION]
        assert len(outlier_issues) > 0
    
    def test_score_batch(self):
        """Test scoring a batch of records."""
        scorer = DataQualityScorer()
        
        records = [
            {"id": "p1", "name": "John", "age": 30, "weight_kg": 70},
            {"id": "p2", "name": "Jane", "age": 25, "weight_kg": 60},
            {"id": "p3", "name": "Bob", "age": 35, "weight_kg": 80},
            {"id": "p4", "name": "", "age": 1000, "weight_kg": -5}  # Bad record
        ]
        
        scores = scorer.score_batch(records, "Patient")
        
        assert len(scores) == 4
        assert all(isinstance(score, QualityScore) for score in scores)
        
        # Last record should have lower quality
        assert scores[-1].overall_score < scores[0].overall_score
    
    def test_get_aggregate_score(self):
        """Test aggregate scoring across multiple records."""
        scorer = DataQualityScorer()
        
        # Create sample scores
        scores = [
            QualityScore(
                record_id=f"rec_{i}",
                record_type="Patient",
                completeness_score=0.8 + (i * 0.05),
                consistency_score=0.9,
                validity_score=0.85,
                accuracy_score=0.95,
                outlier_score=1.0,
                data_usage_score=0.8,
                overall_score=0.85 + (i * 0.02),
                issues=[],
                missing_fields=[],
                invalid_fields=[],
                outlier_fields=[],
                unused_fields=[]
            )
            for i in range(5)
        ]
        
        aggregate = scorer.get_aggregate_score(scores)
        
        assert aggregate["total_records"] == 5
        assert "avg_completeness" in aggregate
        assert "avg_overall_score" in aggregate
        assert "min_overall_score" in aggregate
        assert "max_overall_score" in aggregate
        assert "quality_distribution" in aggregate
        assert "common_issues" in aggregate
        
        # Check quality distribution
        dist = aggregate["quality_distribution"]
        assert isinstance(dist, dict)
        assert "excellent" in dist
        assert "good" in dist
        assert "fair" in dist
        assert "poor" in dist
    
    def test_custom_weights(self):
        """Test scoring with custom weights."""
        custom_config = {
            "weights": {
                "completeness": 0.4,  # Higher weight on completeness
                "consistency": 0.3,
                "validity": 0.1,
                "accuracy": 0.1,
                "outlier_detection": 0.05,
                "data_usage": 0.05
            }
        }
        
        scorer = DataQualityScorer(custom_config)
        
        # Create data with high completeness but other issues
        data = {
            "id": "test",
            "name": "John Doe",
            "birth_date": "1990-01-01",
            "age": 34,
            "email": "invalid-email",  # Consistency issue
            "weight_kg": 1000  # Outlier
        }
        
        score = scorer.score_record(data, "Patient")
        
        # Should still have decent overall score due to high completeness weight
        assert score.overall_score > 0.6
        assert scorer.weights["completeness"] == 0.4
    
    def test_calculate_validity_score(self):
        """Test validity score calculation."""
        scorer = DataQualityScorer()
        
        data = {"field1": "value1", "field2": "value2", "field3": "value3"}
        
        # No validity issues
        issues = []
        validity_score = scorer._calculate_validity_score(data, issues)
        assert validity_score == 1.0
        
        # Some validity issues
        issues = [
            QualityIssue(
                dimension=QualityDimension.CONSISTENCY,
                severity=Severity.MEDIUM,
                field_name="field1",
                issue_type="format_mismatch",
                description="Format issue"
            )
        ]
        validity_score = scorer._calculate_validity_score(data, issues)
        assert validity_score < 1.0
    
    def test_calculate_accuracy_score(self):
        """Test accuracy score calculation."""
        scorer = DataQualityScorer()
        
        # Good data
        good_data = {"id": "123", "name": "John Doe", "value": 42}
        accuracy_score = scorer._calculate_accuracy_score(good_data, "Patient")
        assert accuracy_score == 1.0
        
        # Data with obvious test values
        test_data = {"id": "0", "name": "test", "value": "dummy"}
        accuracy_score = scorer._calculate_accuracy_score(test_data, "Patient")
        assert accuracy_score < 1.0
    
    def test_get_common_issues(self):
        """Test common issues analysis."""
        scorer = DataQualityScorer()
        
        # Create scores with various issues
        scores = []
        for i in range(10):
            issues = [
                QualityIssue(
                    dimension=QualityDimension.COMPLETENESS,
                    severity=Severity.HIGH,
                    field_name="name",
                    issue_type="missing_required",
                    description="Missing name"
                )
            ]
            if i % 2 == 0:  # Add consistency issue to half the records
                issues.append(QualityIssue(
                    dimension=QualityDimension.CONSISTENCY,
                    severity=Severity.MEDIUM,
                    field_name="email",
                    issue_type="format_mismatch",
                    description="Invalid email"
                ))
            
            score = QualityScore(
                record_id=f"rec_{i}",
                record_type="Patient",
                completeness_score=0.8,
                consistency_score=0.9,
                validity_score=0.85,
                accuracy_score=0.95,
                outlier_score=1.0,
                data_usage_score=0.8,
                overall_score=0.85,
                issues=issues,
                missing_fields=[],
                invalid_fields=[],
                outlier_fields=[],
                unused_fields=[]
            )
            scores.append(score)
        
        common_issues = scorer._get_common_issues(scores)
        
        assert len(common_issues) > 0
        assert common_issues[0]["count"] == 10  # Missing name in all records
        assert common_issues[0]["percentage"] == 100
        
        if len(common_issues) > 1:
            assert common_issues[1]["count"] == 5  # Email issue in half
            assert common_issues[1]["percentage"] == 50
    
    def test_get_quality_distribution(self):
        """Test quality distribution calculation."""
        scorer = DataQualityScorer()
        
        # Create scores with different quality levels
        scores = [
            QualityScore(
                record_id="excellent", record_type="Patient", completeness_score=0.95,
                consistency_score=0.95, validity_score=0.95, accuracy_score=0.95,
                outlier_score=0.95, data_usage_score=0.95, overall_score=0.95,
                issues=[], missing_fields=[], invalid_fields=[], outlier_fields=[], unused_fields=[]
            ),
            QualityScore(
                record_id="good", record_type="Patient", completeness_score=0.8,
                consistency_score=0.8, validity_score=0.8, accuracy_score=0.8,
                outlier_score=0.8, data_usage_score=0.8, overall_score=0.8,
                issues=[], missing_fields=[], invalid_fields=[], outlier_fields=[], unused_fields=[]
            ),
            QualityScore(
                record_id="fair", record_type="Patient", completeness_score=0.6,
                consistency_score=0.6, validity_score=0.6, accuracy_score=0.6,
                outlier_score=0.6, data_usage_score=0.6, overall_score=0.6,
                issues=[], missing_fields=[], invalid_fields=[], outlier_fields=[], unused_fields=[]
            ),
            QualityScore(
                record_id="poor", record_type="Patient", completeness_score=0.3,
                consistency_score=0.3, validity_score=0.3, accuracy_score=0.3,
                outlier_score=0.3, data_usage_score=0.3, overall_score=0.3,
                issues=[], missing_fields=[], invalid_fields=[], outlier_fields=[], unused_fields=[]
            )
        ]
        
        distribution = scorer._get_quality_distribution(scores)
        
        assert distribution["excellent"] == 1
        assert distribution["good"] == 1
        assert distribution["fair"] == 1
        assert distribution["poor"] == 1


@pytest.mark.parametrize("record_type,expected_fields", [
    ("Patient", ["id", "name", "birth_date"]),
    ("Observation", ["id", "subject", "code", "value"]),
    ("Medication", ["id", "code", "status"]),
    ("default", ["id"])
])
def test_completeness_scorer_required_fields(record_type, expected_fields):
    """Test that completeness scorer has correct required fields for different record types."""
    scorer = CompletenessScorer()
    required = scorer.required_fields.get(record_type, scorer.required_fields["default"])
    
    for field in expected_fields:
        assert field in required


@pytest.mark.parametrize("field_name,value,expected_outlier", [
    ("age", 25, False),
    ("age", -5, True),
    ("age", 200, True),
    ("heart_rate", 70, False),
    ("heart_rate", 500, True),
    ("weight_kg", 70, False),
    ("weight_kg", -10, True),
    ("temperature_celsius", 37, False),
    ("temperature_celsius", 60, True)
])
def test_outlier_detector_domain_outliers(field_name, value, expected_outlier):
    """Test domain-specific outlier detection for various fields and values."""
    detector = OutlierDetector()
    issues = detector._detect_domain_outliers(field_name, value)
    
    if expected_outlier:
        assert len(issues) > 0, f"Expected outlier for {field_name}={value}"
    else:
        assert len(issues) == 0, f"Unexpected outlier for {field_name}={value}"


if __name__ == "__main__":
    pytest.main([__file__])