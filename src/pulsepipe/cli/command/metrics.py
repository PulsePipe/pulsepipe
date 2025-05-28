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
import asyncio
from datetime import datetime, timedelta
from typing import Optional

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.persistence import get_async_tracking_repository
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig
from pulsepipe.audit import IngestionTracker

logger = LogFactory.get_logger(__name__)


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
    asyncio.run(_export_async(pipeline_run_id, format, output, days, include_details))


async def _export_async(pipeline_run_id: Optional[str], format: str, output: Optional[str], 
                       days: int, include_details: bool):
    """Async implementation of export command."""
    try:
        # Initialize persistence with default SQLite configuration
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": ".pulsepipe/state/ingestion.sqlite3"
                }
            }
        }
        repository = await get_async_tracking_repository(config)
        
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
            
            # Get pipeline run data
            pipeline_run = await repository.get_pipeline_run(pipeline_run_id)
            if not pipeline_run:
                click.echo(f"❌ Pipeline run not found: {pipeline_run_id}")
                return
            
            # Create a mock ingestion tracker to demonstrate export
            config = DataIntelligenceConfig()
            tracker = IngestionTracker(
                pipeline_run_id=pipeline_run_id,
                stage_name="ingestion",
                config=config,
                repository=None  # Legacy tracker doesn't work with async repository
            )
            
            # Export metrics
            tracker.export_metrics(output, format, include_details)
            
        else:
            # Export summary report for recent runs
            click.echo(f"Exporting summary metrics for last {days} days")
            
            reporter = AuditReporter(None)  # Async repository not compatible with legacy reporter
            start_date = datetime.now() - timedelta(days=days)
            
            # For now, just export recent pipeline runs info
            recent_runs = await repository.get_recent_pipeline_runs(limit=50)
            
            # Simple export logic
            if format == 'json':
                import json
                data = {
                    "pipeline_runs": [
                        {
                            "id": run.id,
                            "name": run.name,
                            "status": run.status,
                            "started_at": run.started_at.isoformat() if run.started_at else None,
                            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                            "total_records": run.total_records,
                            "successful_records": run.successful_records,
                            "failed_records": run.failed_records
                        }
                        for run in recent_runs
                    ]
                }
                with open(output, 'w') as f:
                    json.dump(data, f, indent=2)
            else:
                # CSV format
                import csv
                with open(output, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['id', 'name', 'status', 'started_at', 'completed_at', 
                                   'total_records', 'successful_records', 'failed_records'])
                    for run in recent_runs:
                        writer.writerow([
                            run.id, run.name, run.status,
                            run.started_at.isoformat() if run.started_at else '',
                            run.completed_at.isoformat() if run.completed_at else '',
                            run.total_records, run.successful_records, run.failed_records
                        ])
        
        click.echo(f"✅ Metrics exported to: {output}")
        
    except Exception as e:
        logger.error(f"Failed to export metrics: {e}")
        click.echo(f"❌ Export failed: {e}", err=True)
        raise click.Abort()
    finally:
        if 'repository' in locals():
            await repository.close()


@metrics.command()
@click.option('--pipeline-run-id', '-p', help='Pipeline run ID to analyze')
@click.option('--days', '-d', default=7, type=int, help='Number of days to analyze (if no specific run ID)')
@click.option('--format', '-f', default='table', type=click.Choice(['table', 'json']), 
              help='Output format')
def analyze(pipeline_run_id: Optional[str], days: int, format: str):
    """Analyze ingestion metrics and show insights."""
    asyncio.run(_analyze_async(pipeline_run_id, days, format))


async def _analyze_async(pipeline_run_id: Optional[str], days: int, format: str):
    """Async implementation of analyze command."""
    try:
        # Initialize persistence with default SQLite configuration
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": ".pulsepipe/state/ingestion.sqlite3"
                }
            }
        }
        repository = await get_async_tracking_repository(config)
        
        if pipeline_run_id:
            # Analyze specific pipeline run
            click.echo(f"Analyzing metrics for pipeline run: {pipeline_run_id}")
            pipeline_run = await repository.get_pipeline_run(pipeline_run_id)
            if not pipeline_run:
                click.echo(f"❌ Pipeline run not found: {pipeline_run_id}")
                return
            
            # Simple analysis display
            _display_pipeline_analysis(pipeline_run)
            
        else:
            # Analyze recent runs
            click.echo(f"Analyzing metrics for last {days} days")
            recent_runs = await repository.get_recent_pipeline_runs(limit=10)
            
            if format == 'json':
                import json
                data = {
                    "summary": {
                        "total_runs": len(recent_runs),
                        "completed_runs": len([r for r in recent_runs if r.status == 'completed']),
                        "failed_runs": len([r for r in recent_runs if r.status == 'failed'])
                    },
                    "recent_runs": [
                        {
                            "id": run.id,
                            "name": run.name,
                            "status": run.status,
                            "total_records": run.total_records,
                            "successful_records": run.successful_records,
                            "failed_records": run.failed_records
                        }
                        for run in recent_runs
                    ]
                }
                click.echo(json.dumps(data, indent=2))
            else:
                # Display table format
                _display_summary_analysis(recent_runs)
        
    except Exception as e:
        logger.error(f"Failed to analyze metrics: {e}")
        click.echo(f"❌ Analysis failed: {e}", err=True)
        raise click.Abort()
    finally:
        if 'repository' in locals():
            await repository.close()


@metrics.command()
@click.option('--days', '-d', default=30, type=int, help='Number of days of data to keep')
@click.confirmation_option(prompt='Are you sure you want to cleanup old metrics data?')
def cleanup(days: int):
    """Clean up old ingestion metrics data."""
    asyncio.run(_cleanup_async(days))


async def _cleanup_async(days: int):
    """Async implementation of cleanup command."""
    try:
        # Initialize persistence with default SQLite configuration
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": ".pulsepipe/state/ingestion.sqlite3"
                }
            }
        }
        repository = await get_async_tracking_repository(config)
        
        deleted_count = await repository.cleanup_old_data(days)
        
        click.echo(f"✅ Cleaned up {deleted_count} old metric records (older than {days} days)")
        
    except Exception as e:
        logger.error(f"Failed to cleanup metrics: {e}")
        click.echo(f"❌ Cleanup failed: {e}", err=True)
        raise click.Abort()
    finally:
        if 'repository' in locals():
            await repository.close()


@metrics.command()
@click.option('--pipeline-run-id', '-p', help='Pipeline run ID to show status for')
@click.option('--tail', '-t', is_flag=True, help='Show real-time updates (if run is active)')
def status(pipeline_run_id: Optional[str], tail: bool):
    """Show current ingestion metrics status."""
    asyncio.run(_status_async(pipeline_run_id, tail))


async def _status_async(pipeline_run_id: Optional[str], tail: bool):
    """Async implementation of status command."""
    try:
        # Initialize persistence with default SQLite configuration
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": ".pulsepipe/state/ingestion.sqlite3"
                }
            }
        }
        repository = await get_async_tracking_repository(config)
        
        if pipeline_run_id:
            # Show specific pipeline run status
            pipeline_run = await repository.get_pipeline_run(pipeline_run_id)
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
            recent_runs = await repository.get_recent_pipeline_runs(10)
            
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
    finally:
        if 'repository' in locals():
            await repository.close()


def _display_pipeline_analysis(pipeline_run):
    """Display analysis for a specific pipeline run."""
    click.echo("\n" + "="*60)
    click.echo(f"PIPELINE RUN ANALYSIS - {pipeline_run.name.upper()}")
    click.echo("="*60)
    
    click.echo(f"\nRun ID: {pipeline_run.id}")
    click.echo(f"Status: {pipeline_run.status}")
    click.echo(f"Started: {pipeline_run.started_at}")
    if pipeline_run.completed_at:
        click.echo(f"Completed: {pipeline_run.completed_at}")
        duration = pipeline_run.completed_at - pipeline_run.started_at
        click.echo(f"Duration: {duration}")
    
    click.echo("\nRecord Processing:")
    click.echo("-" * 30)
    click.echo(f"Total Records:      {pipeline_run.total_records:>10,}")
    click.echo(f"Successful Records: {pipeline_run.successful_records:>10,}")
    click.echo(f"Failed Records:     {pipeline_run.failed_records:>10,}")
    click.echo(f"Skipped Records:    {pipeline_run.skipped_records:>10,}")
    
    if pipeline_run.total_records > 0:
        success_rate = (pipeline_run.successful_records / pipeline_run.total_records) * 100
        failure_rate = (pipeline_run.failed_records / pipeline_run.total_records) * 100
        click.echo(f"Success Rate:       {success_rate:>10.1f}%")
        click.echo(f"Failure Rate:       {failure_rate:>10.1f}%")
    
    if pipeline_run.error_message:
        click.echo(f"\nError: {pipeline_run.error_message}")
    
    click.echo("\n" + "="*60)


def _display_summary_analysis(recent_runs):
    """Display analysis for recent pipeline runs."""
    click.echo("\n" + "="*60)
    click.echo("RECENT PIPELINE RUNS ANALYSIS")
    click.echo("="*60)
    
    if not recent_runs:
        click.echo("\nNo recent pipeline runs found.")
        return
    
    # Calculate summary statistics
    total_runs = len(recent_runs)
    completed_runs = len([r for r in recent_runs if r.status == 'completed'])
    failed_runs = len([r for r in recent_runs if r.status == 'failed'])
    running_runs = len([r for r in recent_runs if r.status == 'running'])
    
    total_records = sum(r.total_records for r in recent_runs)
    total_successful = sum(r.successful_records for r in recent_runs)
    total_failed = sum(r.failed_records for r in recent_runs)
    
    click.echo("\nSummary Statistics:")
    click.echo("-" * 30)
    click.echo(f"Total Runs:         {total_runs:>10,}")
    click.echo(f"Completed Runs:     {completed_runs:>10,}")
    click.echo(f"Failed Runs:        {failed_runs:>10,}")
    click.echo(f"Running Runs:       {running_runs:>10,}")
    
    if total_runs > 0:
        completion_rate = (completed_runs / total_runs) * 100
        click.echo(f"Completion Rate:    {completion_rate:>10.1f}%")
    
    click.echo(f"\nTotal Records:      {total_records:>10,}")
    click.echo(f"Successful Records: {total_successful:>10,}")
    click.echo(f"Failed Records:     {total_failed:>10,}")
    
    if total_records > 0:
        overall_success_rate = (total_successful / total_records) * 100
        click.echo(f"Overall Success Rate: {overall_success_rate:>8.1f}%")
    
    click.echo(f"\nRecent Runs ({min(len(recent_runs), 10)}):")
    click.echo("-" * 80)
    
    for run in recent_runs[:10]:
        status_icon = "✅" if run.status == "completed" else "❌" if run.status == "failed" else "🔄"
        success_rate = (run.successful_records / run.total_records * 100) if run.total_records > 0 else 0
        
        click.echo(f"{status_icon} {run.name[:30]:<30} {run.status:<10} "
                  f"{run.total_records:>6,} records ({success_rate:>5.1f}% success)")
    
    click.echo("\n" + "="*60)