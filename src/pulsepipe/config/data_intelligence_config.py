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

# src/pulsepipe/config/data_intelligence_config.py

"""
Data Intelligence Configuration Models

Provides Pydantic models for validating and accessing data intelligence
configuration settings from the YAML configuration files.
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class PerformanceMode(str, Enum):
    """Performance modes for data intelligence features."""
    FAST = "fast"
    STANDARD = "standard"
    COMPREHENSIVE = "comprehensive"


class DetailLevel(str, Enum):
    """Detail levels for audit trails and reporting."""
    MINIMAL = "minimal"
    STANDARD = "standard"
    COMPREHENSIVE = "comprehensive"


class SamplingConfig(BaseModel):
    """Configuration for data sampling in high-volume scenarios."""
    enabled: bool = False
    rate: float = Field(default=0.1, ge=0.0, le=1.0, description="Sampling rate (0.0-1.0)")
    minimum_batch_size: int = Field(default=100, ge=1, description="Apply sampling only above this threshold")

    @field_validator('rate')
    def validate_rate(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError('Sampling rate must be between 0.0 and 1.0')
        return v


class IngestionTrackingConfig(BaseModel):
    """Configuration for ingestion success/failure tracking."""
    enabled: bool = True
    detailed_tracking: bool = True
    auto_persist: bool = True
    store_failed_records: bool = True
    export_metrics: bool = True
    export_formats: List[str] = Field(default_factory=lambda: ["json", "csv"])
    batch_size_threshold: int = Field(default=1000, ge=1, description="Records per batch for metrics tracking")
    retention_days: int = Field(default=30, ge=1, description="Days to retain detailed tracking data")

    @field_validator('export_formats')
    def validate_export_formats(cls, v):
        allowed_formats = {"json", "csv", "xlsx", "yaml"}
        invalid_formats = set(v) - allowed_formats
        if invalid_formats:
            raise ValueError(f'Invalid export formats: {invalid_formats}. Allowed: {allowed_formats}')
        return v


class AuditTrailConfig(BaseModel):
    """Configuration for audit trail and reporting."""
    enabled: bool = True
    detail_level: DetailLevel = DetailLevel.STANDARD
    record_level_tracking: bool = True
    structured_errors: bool = True


class QualityScoringConfig(BaseModel):
    """Configuration for data quality assessment."""
    enabled: bool = True
    sampling_rate: float = Field(default=1.0, ge=0.0, le=1.0, description="Quality check sampling rate")
    completeness_scoring: bool = True
    consistency_checks: bool = True
    outlier_detection: bool = True
    aggregate_scoring: bool = True

    @field_validator('sampling_rate')
    def validate_sampling_rate(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError('Quality scoring sampling rate must be between 0.0 and 1.0')
        return v


class ContentAnalysisConfig(BaseModel):
    """Configuration for clinical content analysis (AI-powered)."""
    enabled: bool = False  # Disabled by default due to compute cost
    phi_detection_only: bool = False
    clinical_bert_analysis: bool = True
    medication_extraction: bool = True
    diagnosis_extraction: bool = True
    standardization_gaps: bool = True


class TerminologyValidationConfig(BaseModel):
    """Configuration for healthcare terminology validation."""
    enabled: bool = True
    code_systems: List[str] = Field(default_factory=lambda: ["icd10", "snomed", "rxnorm", "loinc"])
    coverage_reporting: bool = True
    unmapped_terms_collection: bool = True
    compliance_reports: bool = True

    @field_validator('code_systems')
    def validate_code_systems(cls, v):
        allowed_systems = {"icd10", "icd9", "snomed", "rxnorm", "loinc", "cpt", "hcpcs"}
        invalid_systems = set(v) - allowed_systems
        if invalid_systems:
            raise ValueError(f'Invalid code systems: {invalid_systems}. Allowed: {allowed_systems}')
        return v


class PerformanceTrackingConfig(BaseModel):
    """Configuration for performance and system metrics."""
    enabled: bool = True
    step_timing: bool = True
    resource_monitoring: bool = True
    bottleneck_analysis: bool = True
    optimization_recommendations: bool = True


class SystemMetricsConfig(BaseModel):
    """Configuration for environmental system metrics."""
    enabled: bool = True
    hardware_detection: bool = True
    resource_utilization: bool = True
    gpu_detection: bool = True
    os_detection: bool = True
    infrastructure_recommendations: bool = True


class DataIntelligenceFeaturesConfig(BaseModel):
    """Configuration for all data intelligence features."""
    ingestion_tracking: IngestionTrackingConfig = Field(default_factory=IngestionTrackingConfig)
    audit_trail: AuditTrailConfig = Field(default_factory=AuditTrailConfig)
    quality_scoring: QualityScoringConfig = Field(default_factory=QualityScoringConfig)
    content_analysis: ContentAnalysisConfig = Field(default_factory=ContentAnalysisConfig)
    terminology_validation: TerminologyValidationConfig = Field(default_factory=TerminologyValidationConfig)
    performance_tracking: PerformanceTrackingConfig = Field(default_factory=PerformanceTrackingConfig)
    system_metrics: SystemMetricsConfig = Field(default_factory=SystemMetricsConfig)


class DataIntelligenceConfig(BaseModel):
    """Main configuration model for data intelligence and quality assurance framework."""
    enabled: bool = True
    performance_mode: PerformanceMode = PerformanceMode.STANDARD
    sampling: SamplingConfig = Field(default_factory=SamplingConfig)
    features: DataIntelligenceFeaturesConfig = Field(default_factory=DataIntelligenceFeaturesConfig)

    def is_feature_enabled(self, feature_name: str, sub_feature: Optional[str] = None) -> bool:
        """
        Check if a specific feature or sub-feature is enabled.
        
        Args:
            feature_name: Name of the main feature (e.g., 'ingestion_tracking')
            sub_feature: Optional sub-feature name (e.g., 'store_failed_records')
            
        Returns:
            True if the feature is enabled, False otherwise
        """
        # Check if data intelligence is globally disabled
        if not self.enabled:
            return False
            
        # Get the feature configuration
        feature_config = getattr(self.features, feature_name, None)
        if not feature_config:
            return False
            
        # Check if the main feature is disabled
        if not getattr(feature_config, 'enabled', True):
            return False
            
        # If checking a sub-feature, verify it's enabled
        if sub_feature:
            return getattr(feature_config, sub_feature, False)
            
        return True

    def get_effective_sampling_rate(self, feature_name: str) -> float:
        """
        Get the effective sampling rate for a feature, considering both global and feature-specific settings.
        
        Args:
            feature_name: Name of the feature to get sampling rate for
            
        Returns:
            Effective sampling rate (0.0-1.0)
        """
        # Start with global sampling rate
        if self.sampling.enabled:
            rate = self.sampling.rate
        else:
            rate = 1.0
            
        # Apply feature-specific sampling if available
        feature_config = getattr(self.features, feature_name, None)
        if feature_config and hasattr(feature_config, 'sampling_rate'):
            rate = min(rate, feature_config.sampling_rate)
            
        return rate

    def should_apply_sampling(self, batch_size: int) -> bool:
        """
        Determine if sampling should be applied based on batch size.
        
        Args:
            batch_size: Size of the current batch
            
        Returns:
            True if sampling should be applied, False otherwise
        """
        return (self.sampling.enabled and 
                batch_size >= self.sampling.minimum_batch_size)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DataIntelligenceConfig':
        """
        Create a DataIntelligenceConfig from a dictionary (typically loaded from YAML).
        
        Args:
            config_dict: Dictionary containing configuration data
            
        Returns:
            Validated DataIntelligenceConfig instance
        """
        return cls(**config_dict)

    def get_performance_settings(self) -> Dict[str, Any]:
        """
        Get performance-optimized settings based on the current performance mode.
        
        Returns:
            Dictionary with recommended settings for the current performance mode
        """
        if self.performance_mode == PerformanceMode.FAST:
            return {
                "sampling_enabled": True,
                "sampling_rate": 0.1,
                "content_analysis_enabled": False,
                "quality_scoring_sampling": 0.1,
                "resource_monitoring": False,
                "os_detection": False
            }
        elif self.performance_mode == PerformanceMode.COMPREHENSIVE:
            return {
                "sampling_enabled": False,
                "sampling_rate": 1.0,
                "content_analysis_enabled": True,
                "quality_scoring_sampling": 1.0,
                "resource_monitoring": True,
                "os_detection": True
            }
        else:  # STANDARD
            return {
                "sampling_enabled": False,
                "sampling_rate": 1.0,
                "content_analysis_enabled": False,
                "quality_scoring_sampling": 1.0,
                "resource_monitoring": True,
                "os_detection": True
            }


def load_data_intelligence_config(config_dict: Dict[str, Any]) -> DataIntelligenceConfig:
    """
    Load and validate data intelligence configuration from a configuration dictionary.
    
    Args:
        config_dict: Full configuration dictionary from YAML
        
    Returns:
        Validated DataIntelligenceConfig instance
        
    Raises:
        ValueError: If configuration is invalid
    """
    # Extract data intelligence config, providing sensible defaults
    di_config = config_dict.get('data_intelligence', {})
    
    try:
        return DataIntelligenceConfig.from_dict(di_config)
    except Exception as e:
        raise ValueError(f"Invalid data intelligence configuration: {e}")