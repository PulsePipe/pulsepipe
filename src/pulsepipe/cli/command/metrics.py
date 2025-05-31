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

# src/pulsepipe/cli/command/metrics.py

"""
Metrics management commands for PulsePipe CLI.

Provides commands for exporting and analyzing ingestion metrics.
"""

import os
import click
from datetime import datetime, timedelta
from typing import Optional

# Import only lightweight modules at startup
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)

# Lazy import functions for heavy modules
def _get_persistence_modules():
    """Lazy import persistence modules."""
    from pulsepipe.persistence import get_tracking_repository
    from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig
    from pulsepipe.audit import AuditReporter, IngestionTracker
    return get_tracking_repository, DataIntelligenceConfig, AuditReporter, IngestionTracker


@click.group()
def metrics():
    """Manage and export ingestion metrics."""
    pass


@metrics.command()
@click.option('--pipeline-run-id', '-p', help='Pipeline run ID to export metrics for')
@click.option('--format', '-f', default='json', type=click.Choice(['json', 'csv']), 
              help='Export format')
@click.option('--output', '-o', help='Output file path')
@click.option('--days', '-d', default=7, type=int, help='Number of days to include (if no specific run ID)')
@click.option('--include-details', '-D', is_flag=True, help='Include detailed record information')
def export(pipeline_run_id: Optional[str], format: str, output: Optional[str], 
           days: int, include_details: bool):
    """Export ingestion metrics to file."""
    try:
        # Lazy load heavy modules only when function is called
        get_tracking_repository, DataIntelligenceConfig, AuditReporter, IngestionTracker = _get_persistence_modules()
        
        # Initialize persistence - we need a config dict for this
        # For CLI usage, we'll use default configuration
        config = {}
        repository = get_tracking_repository(config)
        
        # Generate default output filename if not provided
        if not output:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if pipeline_run_id:
                output = f"ingestion_metrics_{pipeline_run_id}_{timestamp}.{format}"
            else:
                output = f"ingestion_metrics_summary_{timestamp}.{format}"
        
        if pipeline_run_id:
            # Export specific pipeline run metrics
            click.echo(f"Exporting metrics for pipeline run: {pipeline_run_id}")
            
            # Create a mock ingestion tracker to demonstrate export
            # In a real scenario, this would be loaded from the repository
            config = DataIntelligenceConfig()
            tracker = IngestionTracker(
                pipeline_run_id=pipeline_run_id,
                stage_name="ingestion",
                config=config,
                repository=repository
            )
            
            # Export metrics
            tracker.export_metrics(output, format, include_details)
            
        else:
            # Export summary report for recent runs
            click.echo(f"Exporting summary metrics for last {days} days")
            
            reporter = AuditReporter(repository)
            start_date = datetime.now() - timedelta(days=days)
            
            report = reporter.generate_summary_report(
                start_date=start_date,
                end_date=datetime.now()
            )
            
            reporter.export_report(report, output, format)
        
        click.echo(f"✅ Metrics exported to: {output}")
        
    except Exception as e:
        logger.error(f"Failed to export metrics: {e}")
        click.echo(f"❌ Export failed: {e}", err=True)
        raise click.Abort()


@metrics.command()
@click.option('--pipeline-run-id', '-p', help='Pipeline run ID to analyze')
@click.option('--days', '-d', default=7, type=int, help='Number of days to analyze (if no specific run ID)')
@click.option('--format', '-f', default='table', type=click.Choice(['table', 'json']), 
              help='Output format')
def analyze(pipeline_run_id: Optional[str], days: int, format: str):
    """Analyze ingestion metrics and show insights."""
    try:
        # Lazy load heavy modules only when function is called
        get_tracking_repository, DataIntelligenceConfig, AuditReporter, IngestionTracker = _get_persistence_modules()
        
        # Initialize persistence - use the same config discovery logic as run command
        from pulsepipe.utils.config_loader import load_config
        from pulsepipe.cli.command.run import find_profile_path
        
        # Try to find and load main pulsepipe config using same logic as run command
        main_config_path = find_profile_path("pulsepipe")
        if main_config_path:
            config = load_config(main_config_path)
        else:
            # Fall back to minimal SQLite config if main config not found
            config = {
                "persistence": {
                    "type": "sqlite",
                    "sqlite": {
                        "db_path": ".pulsepipe/state/ingestion.sqlite3",
                        "timeout": 30.0
                    }
                }
            }
        
        repository = get_tracking_repository(config)
        reporter = AuditReporter(repository)
        
        if pipeline_run_id:
            # Analyze specific pipeline run
            click.echo(f"Analyzing metrics for pipeline run: {pipeline_run_id}")
            report = reporter.generate_pipeline_report(pipeline_run_id)
            
        else:
            # Analyze recent runs
            click.echo(f"Analyzing metrics for last {days} days")
            start_date = datetime.now() - timedelta(days=days)
            report = reporter.generate_summary_report(
                start_date=start_date,
                end_date=datetime.now()
            )
        
        if format == 'json':
            click.echo(report.to_json())
        else:
            # Display table format
            _display_metrics_table(report)
        
    except Exception as e:
        logger.error(f"Failed to analyze metrics: {e}")
        click.echo(f"❌ Analysis failed: {e}", err=True)
        raise click.Abort()


@metrics.command()
@click.option('--days', '-d', default=30, type=int, help='Number of days of data to keep (0 = delete all)')
@click.confirmation_option(prompt='Are you sure you want to cleanup old metrics data?')
def cleanup(days: int):
    """Clean up old ingestion metrics data.
    
    Removes pipeline runs, audit events, ingestion statistics, and quality metrics
    older than the specified number of days. Use --days 0 to delete all data.
    
    Examples:
        pulsepipe metrics cleanup --days 30    # Keep last 30 days
        pulsepipe metrics cleanup --days 7     # Keep last 7 days  
        pulsepipe metrics cleanup --days 0     # Delete all data
    """
    try:
        # Lazy load heavy modules only when function is called
        get_tracking_repository, DataIntelligenceConfig, AuditReporter, IngestionTracker = _get_persistence_modules()
        
        # Initialize persistence - use the same config discovery logic as run command
        from pulsepipe.utils.config_loader import load_config
        from pulsepipe.cli.command.run import find_profile_path
        
        # Try to find and load main pulsepipe config using same logic as run command
        main_config_path = find_profile_path("pulsepipe")
        if main_config_path:
            config = load_config(main_config_path)
        else:
            # Fall back to minimal SQLite config if main config not found
            config = {
                "persistence": {
                    "type": "sqlite",
                    "sqlite": {
                        "db_path": ".pulsepipe/state/ingestion.sqlite3",
                        "timeout": 30.0
                    }
                }
            }
        
        repository = get_tracking_repository(config)
        
        deleted_count = repository.cleanup_old_data(days)
        
        click.echo(f"✅ Cleaned up {deleted_count} old metric records (older than {days} days)")
        
    except Exception as e:
        logger.error(f"Failed to cleanup metrics: {e}")
        click.echo(f"❌ Cleanup failed: {e}", err=True)
        raise click.Abort()


@metrics.command()
@click.option('--pipeline-run-id', '-p', help='Pipeline run ID to show status for')
@click.option('--tail', '-t', is_flag=True, help='Show real-time updates (if run is active)')
def status(pipeline_run_id: Optional[str], tail: bool):
    """Show current ingestion metrics status."""
    try:
        # Lazy load heavy modules only when function is called
        get_tracking_repository, DataIntelligenceConfig, AuditReporter, IngestionTracker = _get_persistence_modules()
        
        # Initialize persistence - use the same config discovery logic as run command
        from pulsepipe.utils.config_loader import load_config
        from pulsepipe.cli.command.run import find_profile_path
        
        # Try to find and load main pulsepipe config using same logic as run command
        main_config_path = find_profile_path("pulsepipe")
        if main_config_path:
            config = load_config(main_config_path)
        else:
            # Fall back to minimal SQLite config if main config not found
            config = {
                "persistence": {
                    "type": "sqlite",
                    "sqlite": {
                        "db_path": ".pulsepipe/state/ingestion.sqlite3",
                        "timeout": 30.0
                    }
                }
            }
        
        repository = get_tracking_repository(config)
        
        if pipeline_run_id:
            # Show specific pipeline run status
            pipeline_run = repository.get_pipeline_run(pipeline_run_id)
            if not pipeline_run:
                click.echo(f"❌ Pipeline run not found: {pipeline_run_id}")
                return
            
            click.echo(f"Pipeline Run: {pipeline_run.name} ({pipeline_run_id})")
            click.echo(f"Status: {pipeline_run.status}")
            click.echo(f"Started: {pipeline_run.started_at}")
            if pipeline_run.completed_at:
                click.echo(f"Completed: {pipeline_run.completed_at}")
            click.echo(f"Total Records: {pipeline_run.total_records:,}")
            click.echo(f"Successful: {pipeline_run.successful_records:,}")
            click.echo(f"Failed: {pipeline_run.failed_records:,}")
            
            if pipeline_run.total_records > 0:
                success_rate = (pipeline_run.successful_records / pipeline_run.total_records) * 100
                click.echo(f"Success Rate: {success_rate:.1f}%")
            
        else:
            # Show summary of recent runs
            recent_runs = repository.get_recent_pipeline_runs(10)
            
            if not recent_runs:
                click.echo("No recent pipeline runs found")
                return
            
            click.echo("Recent Pipeline Runs:")
            click.echo("-" * 80)
            
            for run in recent_runs:
                status_icon = "✅" if run.status == "completed" else "❌" if run.status == "failed" else "🔄"
                success_rate = (run.successful_records / run.total_records * 100) if run.total_records > 0 else 0
                
                click.echo(f"{status_icon} {run.name[:30]:<30} {run.status:<10} "
                          f"{run.total_records:>6,} records ({success_rate:>5.1f}% success)")
        
    except Exception as e:
        logger.error(f"Failed to show status: {e}")
        click.echo(f"❌ Status failed: {e}", err=True)
        raise click.Abort()


def _display_metrics_table(report):
    """Display metrics report in table format."""
    # Lazy import only when needed for display
    from pulsepipe.audit import AuditReport
    
    click.echo("\n" + "="*60)
    click.echo(f"INGESTION METRICS REPORT - {report.report_type.upper()}")
    click.echo("="*60)
    
    # Processing Summary
    ps = report.processing_summary
    click.echo("\nProcessing Summary:")
    click.echo("-" * 30)
    click.echo(f"Total Records:      {ps.total_records:>10,}")
    click.echo(f"Successful Records: {ps.successful_records:>10,}")
    click.echo(f"Failed Records:     {ps.failed_records:>10,}")
    click.echo(f"Success Rate:       {ps.success_rate:>10.1f}%")
    click.echo(f"Failure Rate:       {ps.failure_rate:>10.1f}%")
    click.echo(f"Avg Processing Time: {ps.avg_processing_time_ms:>9.1f}ms")
    
    # Error Breakdown
    if report.error_breakdown:
        click.echo("\nError Breakdown:")
        click.echo("-" * 30)
        total_errors = sum(report.error_breakdown.values())
        for error_type, count in sorted(report.error_breakdown.items(), 
                                      key=lambda x: x[1], reverse=True):
            percentage = (count / total_errors * 100) if total_errors > 0 else 0
            click.echo(f"{error_type:<20} {count:>6,} ({percentage:>5.1f}%)")
    
    # Recommendations
    if report.recommendations:
        click.echo("\nRecommendations:")
        click.echo("-" * 30)
        for i, rec in enumerate(report.recommendations, 1):
            click.echo(f"{i}. {rec}")
    
    # Pipeline Runs (if summary report)
    if len(report.pipeline_runs) > 1:
        click.echo(f"\nRecent Pipeline Runs ({len(report.pipeline_runs)}):")
        click.echo("-" * 50)
        for run in report.pipeline_runs[:5]:  # Show top 5
            status_icon = "✅" if run.status == "completed" else "❌" if run.status == "failed" else "🔄"
            success_rate = (run.successful_records / run.total_records * 100) if run.total_records > 0 else 0
            click.echo(f"{status_icon} {run.name[:25]:<25} {success_rate:>5.1f}% success")
    
    click.echo("\n" + "="*60)