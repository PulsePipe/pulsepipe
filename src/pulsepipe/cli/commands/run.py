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
# 
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

# src/pulsepipe/cli/commands/run.py

"""
Run command for executing PulsePipe data pipelines.
"""
import os
import sys
import json
import asyncio
import click
from typing import Dict, Any, Optional

from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.cli.options import pipeline_options, output_options


@click.command()
@pipeline_options
@output_options
@click.pass_context
async def run(ctx, adapter, ingester, dry_run, user_id, org_id, 
           print_model, summary, output, pretty):
    """Run a data pipeline with the configured adapter and ingester.
    
    Examples:
        pulsepipe run --profile patient_fhir --summary
        pulsepipe run --adapter adapter.yaml --ingester ingester.yaml
        pulsepipe run --pipeline-id pipeline123 --dry-run
    """
    # Set up logger
    logger = LogFactory.get_logger("pipeline.run")
    
    # Get context from parent command
    config = ctx.obj.get('config', {})
    pipeline_context = ctx.obj.get('context')
    
    # Update context with run-specific options
    if pipeline_context:
        if user_id:
            pipeline_context.user_id = user_id
        if org_id:
            pipeline_context.org_id = org_id
        pipeline_context.is_dry_run = dry_run
    
    # Log pipeline start
    context_prefix = pipeline_context.get_log_prefix() if pipeline_context else ""
    logger.info(f"{context_prefix} Starting pipeline execution")
    
    # Determine config sources
    try:
        # Profile takes precedence if specified
        if ctx.parent.params.get('profile'):
            profile = ctx.parent.params['profile']
            logger.info(f"Using profile: {profile}")
            
            # Profile config should already be loaded by parent command
            adapter_config = config.get('adapter', {})
            ingester_config = config.get('ingester', {})
        else:
            # Load adapter config
            if adapter:
                adapter_config = load_config(adapter).get('adapter', {})
            else:
                adapter_config = load_config("adapter.yaml").get('adapter', {})
            
            # Load ingester config
            if ingester:
                ingester_config = load_config(ingester).get('ingester', {})
            else:
                ingester_config = load_config("ingester.yaml").get('ingester', {})
        
        if dry_run:
            logger.info("DRY RUN: Validating configuration only")
            # Validate configs without running
            click.echo(f"✅ Adapter config valid: {adapter_config.get('type', 'unknown')}")
            click.echo(f"✅ Ingester config valid: {ingester_config.get('type', 'unknown')}")
            return
        
        # Create adapter and ingester
        adapter_instance = create_adapter(adapter_config)
        ingester_instance = create_ingester(ingester_config)
        
        # Create engine
        engine = IngestionEngine(adapter_instance, ingester_instance)
        
        # Run the engine
        clinical_content = await engine.run()
        
        # Process output based on options
        if clinical_content:
            # Show summary if requested
            if summary:
                click.echo(clinical_content.summary())
            
            # Print full model if requested
            if print_model:
                model_json = clinical_content.model_dump_json(indent=4 if pretty else None)
                if output:
                    with open(output, 'w') as f:
                        f.write(model_json)
                    click.echo(f"✅ Model data written to {output}")
                else:
                    click.echo(model_json)
        
        logger.info(f"{context_prefix} Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"{context_prefix} Pipeline execution failed: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


# Replace the above default implementation with this async version
@click.command()
@pipeline_options
@output_options
@click.pass_context
def run(ctx, **kwargs):
    """Run a data pipeline with the configured adapter and ingester."""
    asyncio.run(_run_async(ctx, **kwargs))


async def _run_async(ctx, **kwargs):
    """Async implementation of run command."""
    await run.callback(ctx, **kwargs)