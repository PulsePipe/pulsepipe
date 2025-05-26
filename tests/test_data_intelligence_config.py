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

# tests/test_data_intelligence_config.py

"""
Unit tests for data intelligence configuration framework.

Tests configuration validation, default values, and configuration loading
for the data intelligence and quality assurance features.
"""

import pytest
from typing import Dict, Any
from pydantic import ValidationError

from pulsepipe.config.data_intelligence_config import (
    DataIntelligenceConfig,
    PerformanceMode,
    DetailLevel,
    SamplingConfig,
    IngestionTrackingConfig,
    AuditTrailConfig,
    QualityScoringConfig,
    ContentAnalysisConfig,
    TerminologyValidationConfig,
    PerformanceTrackingConfig,
    SystemMetricsConfig,
    DataIntelligenceFeaturesConfig,
    load_data_intelligence_config
)


class TestSamplingConfig:
    """Test SamplingConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for sampling configuration."""
        config = SamplingConfig()
        assert config.enabled is False
        assert config.rate == 0.1
        assert config.minimum_batch_size == 100
    
    def test_valid_sampling_rate(self):
        """Test valid sampling rates."""
        # Test boundary values
        config = SamplingConfig(rate=0.0)
        assert config.rate == 0.0
        
        config = SamplingConfig(rate=1.0)
        assert config.rate == 1.0
        
        config = SamplingConfig(rate=0.5)
        assert config.rate == 0.5
    
    def test_invalid_sampling_rate(self):
        """Test invalid sampling rates raise validation errors."""
        with pytest.raises(ValidationError):
            SamplingConfig(rate=-0.1)
        
        with pytest.raises(ValidationError):
            SamplingConfig(rate=1.1)
        
        with pytest.raises(ValidationError):
            SamplingConfig(rate=2.0)
    
    def test_minimum_batch_size_validation(self):
        """Test minimum batch size validation."""
        config = SamplingConfig(minimum_batch_size=1)
        assert config.minimum_batch_size == 1
        
        config = SamplingConfig(minimum_batch_size=1000)
        assert config.minimum_batch_size == 1000
        
        with pytest.raises(ValidationError):
            SamplingConfig(minimum_batch_size=0)


class TestIngestionTrackingConfig:
    """Test IngestionTrackingConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for ingestion tracking configuration."""
        config = IngestionTrackingConfig()
        assert config.enabled is True
        assert config.store_failed_records is True
        assert config.export_metrics is True
        assert config.export_formats == ["json", "csv"]
    
    def test_valid_export_formats(self):
        """Test valid export formats."""
        valid_formats = ["json", "csv", "xlsx", "yaml"]
        config = IngestionTrackingConfig(export_formats=valid_formats)
        assert config.export_formats == valid_formats
        
        # Test subset
        config = IngestionTrackingConfig(export_formats=["json"])
        assert config.export_formats == ["json"]
    
    def test_invalid_export_formats(self):
        """Test invalid export formats raise validation errors."""
        with pytest.raises(ValidationError) as exc_info:
            IngestionTrackingConfig(export_formats=["json", "invalid"])
        assert "Invalid export formats" in str(exc_info.value)
        
        with pytest.raises(ValidationError):
            IngestionTrackingConfig(export_formats=["xml", "pdf"])


class TestAuditTrailConfig:
    """Test AuditTrailConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for audit trail configuration."""
        config = AuditTrailConfig()
        assert config.enabled is True
        assert config.detail_level == DetailLevel.STANDARD
        assert config.record_level_tracking is True
        assert config.structured_errors is True
    
    def test_detail_levels(self):
        """Test all detail level options."""
        for level in [DetailLevel.MINIMAL, DetailLevel.STANDARD, DetailLevel.COMPREHENSIVE]:
            config = AuditTrailConfig(detail_level=level)
            assert config.detail_level == level
    
    def test_detail_level_from_string(self):
        """Test detail level assignment from string."""
        config = AuditTrailConfig(detail_level="minimal")
        assert config.detail_level == DetailLevel.MINIMAL
        
        config = AuditTrailConfig(detail_level="comprehensive")
        assert config.detail_level == DetailLevel.COMPREHENSIVE


class TestQualityScoringConfig:
    """Test QualityScoringConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for quality scoring configuration."""
        config = QualityScoringConfig()
        assert config.enabled is True
        assert config.sampling_rate == 1.0
        assert config.completeness_scoring is True
        assert config.consistency_checks is True
        assert config.outlier_detection is True
        assert config.aggregate_scoring is True
    
    def test_valid_sampling_rates(self):
        """Test valid sampling rates for quality scoring."""
        config = QualityScoringConfig(sampling_rate=0.0)
        assert config.sampling_rate == 0.0
        
        config = QualityScoringConfig(sampling_rate=0.5)
        assert config.sampling_rate == 0.5
        
        config = QualityScoringConfig(sampling_rate=1.0)
        assert config.sampling_rate == 1.0
    
    def test_invalid_sampling_rates(self):
        """Test invalid sampling rates raise validation errors."""
        with pytest.raises(ValidationError):
            QualityScoringConfig(sampling_rate=-0.1)
        
        with pytest.raises(ValidationError):
            QualityScoringConfig(sampling_rate=1.1)


class TestContentAnalysisConfig:
    """Test ContentAnalysisConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for content analysis configuration."""
        config = ContentAnalysisConfig()
        assert config.enabled is False  # Disabled by default due to compute cost
        assert config.phi_detection_only is False
        assert config.clinical_bert_analysis is True
        assert config.medication_extraction is True
        assert config.diagnosis_extraction is True
        assert config.standardization_gaps is True
    
    def test_all_features_configurable(self):
        """Test all content analysis features can be configured."""
        config = ContentAnalysisConfig(
            enabled=True,
            phi_detection_only=True,
            clinical_bert_analysis=False,
            medication_extraction=False,
            diagnosis_extraction=False,
            standardization_gaps=False
        )
        assert config.enabled is True
        assert config.phi_detection_only is True
        assert config.clinical_bert_analysis is False
        assert config.medication_extraction is False
        assert config.diagnosis_extraction is False
        assert config.standardization_gaps is False


class TestTerminologyValidationConfig:
    """Test TerminologyValidationConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for terminology validation configuration."""
        config = TerminologyValidationConfig()
        assert config.enabled is True
        assert config.code_systems == ["icd10", "snomed", "rxnorm", "loinc"]
        assert config.coverage_reporting is True
        assert config.unmapped_terms_collection is True
        assert config.compliance_reports is True
    
    def test_valid_code_systems(self):
        """Test valid code systems."""
        valid_systems = ["icd10", "icd9", "snomed", "rxnorm", "loinc", "cpt", "hcpcs"]
        config = TerminologyValidationConfig(code_systems=valid_systems)
        assert config.code_systems == valid_systems
        
        # Test subset
        config = TerminologyValidationConfig(code_systems=["icd10", "snomed"])
        assert config.code_systems == ["icd10", "snomed"]
    
    def test_invalid_code_systems(self):
        """Test invalid code systems raise validation errors."""
        with pytest.raises(ValidationError) as exc_info:
            TerminologyValidationConfig(code_systems=["icd10", "invalid"])
        assert "Invalid code systems" in str(exc_info.value)
        
        with pytest.raises(ValidationError):
            TerminologyValidationConfig(code_systems=["unknown", "fake"])


class TestPerformanceTrackingConfig:
    """Test PerformanceTrackingConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for performance tracking configuration."""
        config = PerformanceTrackingConfig()
        assert config.enabled is True
        assert config.step_timing is True
        assert config.resource_monitoring is True
        assert config.bottleneck_analysis is True
        assert config.optimization_recommendations is True
    
    def test_all_features_configurable(self):
        """Test all performance tracking features can be configured."""
        config = PerformanceTrackingConfig(
            enabled=False,
            step_timing=False,
            resource_monitoring=False,
            bottleneck_analysis=False,
            optimization_recommendations=False
        )
        assert config.enabled is False
        assert config.step_timing is False
        assert config.resource_monitoring is False
        assert config.bottleneck_analysis is False
        assert config.optimization_recommendations is False


class TestSystemMetricsConfig:
    """Test SystemMetricsConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for system metrics configuration."""
        config = SystemMetricsConfig()
        assert config.enabled is True
        assert config.hardware_detection is True
        assert config.resource_utilization is True
        assert config.gpu_detection is True
        assert config.os_detection is True
        assert config.infrastructure_recommendations is True
    
    def test_all_features_configurable(self):
        """Test all system metrics features can be configured."""
        config = SystemMetricsConfig(
            enabled=False,
            hardware_detection=False,
            resource_utilization=False,
            gpu_detection=False,
            os_detection=False,
            infrastructure_recommendations=False
        )
        assert config.enabled is False
        assert config.hardware_detection is False
        assert config.resource_utilization is False
        assert config.gpu_detection is False
        assert config.os_detection is False
        assert config.infrastructure_recommendations is False


class TestDataIntelligenceFeaturesConfig:
    """Test DataIntelligenceFeaturesConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for features configuration."""
        config = DataIntelligenceFeaturesConfig()
        
        # Verify all sub-configs are properly initialized
        assert isinstance(config.ingestion_tracking, IngestionTrackingConfig)
        assert isinstance(config.audit_trail, AuditTrailConfig)
        assert isinstance(config.quality_scoring, QualityScoringConfig)
        assert isinstance(config.content_analysis, ContentAnalysisConfig)
        assert isinstance(config.terminology_validation, TerminologyValidationConfig)
        assert isinstance(config.performance_tracking, PerformanceTrackingConfig)
        assert isinstance(config.system_metrics, SystemMetricsConfig)
    
    def test_custom_sub_configs(self):
        """Test features config with custom sub-configurations."""
        custom_config = DataIntelligenceFeaturesConfig(
            ingestion_tracking=IngestionTrackingConfig(enabled=False),
            content_analysis=ContentAnalysisConfig(enabled=True),
            quality_scoring=QualityScoringConfig(sampling_rate=0.5)
        )
        
        assert custom_config.ingestion_tracking.enabled is False
        assert custom_config.content_analysis.enabled is True
        assert custom_config.quality_scoring.sampling_rate == 0.5


class TestDataIntelligenceConfig:
    """Test main DataIntelligenceConfig validation and behavior."""
    
    def test_default_values(self):
        """Test default values for main configuration."""
        config = DataIntelligenceConfig()
        assert config.enabled is True
        assert config.performance_mode == PerformanceMode.STANDARD
        assert isinstance(config.sampling, SamplingConfig)
        assert isinstance(config.features, DataIntelligenceFeaturesConfig)
    
    def test_performance_modes(self):
        """Test all performance mode options."""
        for mode in [PerformanceMode.FAST, PerformanceMode.STANDARD, PerformanceMode.COMPREHENSIVE]:
            config = DataIntelligenceConfig(performance_mode=mode)
            assert config.performance_mode == mode
    
    def test_performance_mode_from_string(self):
        """Test performance mode assignment from string."""
        config = DataIntelligenceConfig(performance_mode="fast")
        assert config.performance_mode == PerformanceMode.FAST
        
        config = DataIntelligenceConfig(performance_mode="comprehensive")
        assert config.performance_mode == PerformanceMode.COMPREHENSIVE
    
    def test_is_feature_enabled_global_disabled(self):
        """Test feature checking when globally disabled."""
        config = DataIntelligenceConfig(enabled=False)
        assert config.is_feature_enabled("ingestion_tracking") is False
        assert config.is_feature_enabled("quality_scoring") is False
        assert config.is_feature_enabled("content_analysis") is False
    
    def test_is_feature_enabled_feature_disabled(self):
        """Test feature checking when specific feature is disabled."""
        config = DataIntelligenceConfig()
        config.features.ingestion_tracking.enabled = False
        
        assert config.is_feature_enabled("ingestion_tracking") is False
        assert config.is_feature_enabled("quality_scoring") is True  # Still enabled
    
    def test_is_feature_enabled_sub_features(self):
        """Test sub-feature checking."""
        config = DataIntelligenceConfig()
        
        # Test enabled sub-feature
        assert config.is_feature_enabled("ingestion_tracking", "store_failed_records") is True
        assert config.is_feature_enabled("quality_scoring", "completeness_scoring") is True
        
        # Test disabled sub-feature
        config.features.ingestion_tracking.store_failed_records = False
        assert config.is_feature_enabled("ingestion_tracking", "store_failed_records") is False
    
    def test_is_feature_enabled_nonexistent_feature(self):
        """Test feature checking for non-existent features."""
        config = DataIntelligenceConfig()
        assert config.is_feature_enabled("nonexistent_feature") is False
    
    def test_get_effective_sampling_rate_global_sampling(self):
        """Test effective sampling rate with global sampling enabled."""
        config = DataIntelligenceConfig()
        config.sampling.enabled = True
        config.sampling.rate = 0.2
        
        assert config.get_effective_sampling_rate("ingestion_tracking") == 0.2
    
    def test_get_effective_sampling_rate_feature_sampling(self):
        """Test effective sampling rate with feature-specific sampling."""
        config = DataIntelligenceConfig()
        config.sampling.enabled = True
        config.sampling.rate = 0.5
        config.features.quality_scoring.sampling_rate = 0.1
        
        # Should use the minimum of global and feature-specific rates
        assert config.get_effective_sampling_rate("quality_scoring") == 0.1
    
    def test_get_effective_sampling_rate_no_global_sampling(self):
        """Test effective sampling rate when global sampling is disabled."""
        config = DataIntelligenceConfig()
        config.sampling.enabled = False
        config.features.quality_scoring.sampling_rate = 0.3
        
        assert config.get_effective_sampling_rate("quality_scoring") == 0.3
    
    def test_should_apply_sampling(self):
        """Test sampling decision logic."""
        config = DataIntelligenceConfig()
        config.sampling.enabled = True
        config.sampling.minimum_batch_size = 100
        
        assert config.should_apply_sampling(50) is False  # Below threshold
        assert config.should_apply_sampling(100) is True  # At threshold
        assert config.should_apply_sampling(200) is True  # Above threshold
        
        config.sampling.enabled = False
        assert config.should_apply_sampling(200) is False  # Sampling disabled
    
    def test_from_dict(self):
        """Test configuration creation from dictionary."""
        config_dict = {
            "enabled": True,
            "performance_mode": "fast",
            "sampling": {
                "enabled": True,
                "rate": 0.1
            },
            "features": {
                "content_analysis": {
                    "enabled": True,
                    "phi_detection_only": True
                }
            }
        }
        
        config = DataIntelligenceConfig.from_dict(config_dict)
        assert config.enabled is True
        assert config.performance_mode == PerformanceMode.FAST
        assert config.sampling.enabled is True
        assert config.sampling.rate == 0.1
        assert config.features.content_analysis.enabled is True
        assert config.features.content_analysis.phi_detection_only is True
    
    def test_get_performance_settings_fast(self):
        """Test performance settings for fast mode."""
        config = DataIntelligenceConfig(performance_mode=PerformanceMode.FAST)
        settings = config.get_performance_settings()
        
        assert settings["sampling_enabled"] is True
        assert settings["sampling_rate"] == 0.1
        assert settings["content_analysis_enabled"] is False
        assert settings["quality_scoring_sampling"] == 0.1
        assert settings["resource_monitoring"] is False
        assert settings["os_detection"] is False
    
    def test_get_performance_settings_standard(self):
        """Test performance settings for standard mode."""
        config = DataIntelligenceConfig(performance_mode=PerformanceMode.STANDARD)
        settings = config.get_performance_settings()
        
        assert settings["sampling_enabled"] is False
        assert settings["sampling_rate"] == 1.0
        assert settings["content_analysis_enabled"] is False
        assert settings["quality_scoring_sampling"] == 1.0
        assert settings["resource_monitoring"] is True
        assert settings["os_detection"] is True
    
    def test_get_performance_settings_comprehensive(self):
        """Test performance settings for comprehensive mode."""
        config = DataIntelligenceConfig(performance_mode=PerformanceMode.COMPREHENSIVE)
        settings = config.get_performance_settings()
        
        assert settings["sampling_enabled"] is False
        assert settings["sampling_rate"] == 1.0
        assert settings["content_analysis_enabled"] is True
        assert settings["quality_scoring_sampling"] == 1.0
        assert settings["resource_monitoring"] is True
        assert settings["os_detection"] is True


class TestLoadDataIntelligenceConfig:
    """Test configuration loading function."""
    
    def test_load_empty_config(self):
        """Test loading with empty configuration."""
        config_dict = {}
        config = load_data_intelligence_config(config_dict)
        
        # Should return default configuration
        assert config.enabled is True
        assert config.performance_mode == PerformanceMode.STANDARD
    
    def test_load_partial_config(self):
        """Test loading with partial configuration."""
        config_dict = {
            "data_intelligence": {
                "enabled": False,
                "performance_mode": "fast",
                "features": {
                    "content_analysis": {
                        "enabled": True
                    }
                }
            }
        }
        
        config = load_data_intelligence_config(config_dict)
        assert config.enabled is False
        assert config.performance_mode == PerformanceMode.FAST
        assert config.features.content_analysis.enabled is True
        # Other features should have default values
        assert config.features.ingestion_tracking.enabled is True
    
    def test_load_full_config(self):
        """Test loading with full configuration."""
        config_dict = {
            "data_intelligence": {
                "enabled": True,
                "performance_mode": "comprehensive",
                "sampling": {
                    "enabled": True,
                    "rate": 0.05,
                    "minimum_batch_size": 50
                },
                "features": {
                    "ingestion_tracking": {
                        "enabled": True,
                        "export_formats": ["json", "yaml"]
                    },
                    "quality_scoring": {
                        "sampling_rate": 0.8
                    },
                    "terminology_validation": {
                        "code_systems": ["icd10", "snomed"]
                    }
                }
            }
        }
        
        config = load_data_intelligence_config(config_dict)
        assert config.enabled is True
        assert config.performance_mode == PerformanceMode.COMPREHENSIVE
        assert config.sampling.enabled is True
        assert config.sampling.rate == 0.05
        assert config.sampling.minimum_batch_size == 50
        assert config.features.ingestion_tracking.export_formats == ["json", "yaml"]
        assert config.features.quality_scoring.sampling_rate == 0.8
        assert config.features.terminology_validation.code_systems == ["icd10", "snomed"]
    
    def test_load_invalid_config(self):
        """Test loading with invalid configuration."""
        config_dict = {
            "data_intelligence": {
                "performance_mode": "invalid_mode",
                "sampling": {
                    "rate": 2.0  # Invalid rate > 1.0
                }
            }
        }
        
        with pytest.raises(ValueError) as exc_info:
            load_data_intelligence_config(config_dict)
        assert "Invalid data intelligence configuration" in str(exc_info.value)


class TestConfigIntegration:
    """Test integration scenarios and edge cases."""
    
    def test_complex_feature_interaction(self):
        """Test complex interactions between features and settings."""
        config = DataIntelligenceConfig(
            performance_mode=PerformanceMode.FAST,
            sampling=SamplingConfig(enabled=True, rate=0.2, minimum_batch_size=50)
        )
        
        # Fast mode should override content analysis even if explicitly enabled
        settings = config.get_performance_settings()
        assert settings["content_analysis_enabled"] is False
        
        # But feature-level checks should still work
        config.features.content_analysis.enabled = True
        assert config.is_feature_enabled("content_analysis") is True
    
    def test_yaml_structure_compatibility(self):
        """Test that config structure matches expected YAML structure."""
        # This simulates the structure in the actual pulsepipe.yaml
        yaml_structure = {
            "data_intelligence": {
                "enabled": True,
                "performance_mode": "standard",
                "sampling": {
                    "enabled": False,
                    "rate": 0.1,
                    "minimum_batch_size": 100
                },
                "features": {
                    "ingestion_tracking": {
                        "enabled": True,
                        "store_failed_records": True,
                        "export_metrics": True,
                        "export_formats": ["json", "csv"]
                    },
                    "audit_trail": {
                        "enabled": True,
                        "detail_level": "standard",
                        "record_level_tracking": True,
                        "structured_errors": True
                    },
                    "quality_scoring": {
                        "enabled": True,
                        "sampling_rate": 1.0,
                        "completeness_scoring": True,
                        "consistency_checks": True,
                        "outlier_detection": True,
                        "aggregate_scoring": True
                    },
                    "content_analysis": {
                        "enabled": False,
                        "phi_detection_only": False,
                        "clinical_bert_analysis": True,
                        "medication_extraction": True,
                        "diagnosis_extraction": True,
                        "standardization_gaps": True
                    },
                    "terminology_validation": {
                        "enabled": True,
                        "code_systems": ["icd10", "snomed", "rxnorm", "loinc"],
                        "coverage_reporting": True,
                        "unmapped_terms_collection": True,
                        "compliance_reports": True
                    },
                    "performance_tracking": {
                        "enabled": True,
                        "step_timing": True,
                        "resource_monitoring": True,
                        "bottleneck_analysis": True,
                        "optimization_recommendations": True
                    },
                    "system_metrics": {
                        "enabled": True,
                        "hardware_detection": True,
                        "resource_utilization": True,
                        "gpu_detection": True,
                        "os_detection": True,
                        "infrastructure_recommendations": True
                    }
                }
            }
        }
        
        # Should load without errors
        config = load_data_intelligence_config(yaml_structure)
        assert isinstance(config, DataIntelligenceConfig)
        
        # Verify key structure elements
        assert config.enabled is True
        assert config.performance_mode == PerformanceMode.STANDARD
        assert config.features.content_analysis.enabled is False
        assert config.features.system_metrics.os_detection is True