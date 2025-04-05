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
Fixed implementation of the run command.
Replace the content of src/pulsepipe/cli/commands/run.py with this code.
"""

import sys
import asyncio
import click
from typing import Any

from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.cli.options import pipeline_options, output_options


# Async implementation function
async def _run_pipeline(ctx, adapter, ingester, dry_run, user_id, org_id, 
                      print_model, summary, output, pretty):
    """Async implementation of pipeline execution."""
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
        content = await engine.run()
        
        # Process output based on content type
        if content:
            content_type = "unknown"
            if hasattr(content, "__class__") and hasattr(content.__class__, "__name__"):
                if "Clinical" in content.__class__.__name__:
                    content_type = "clinical"
                elif "Operational" in content.__class__.__name__:
                    content_type = "operational"
            
            # Show summary if requested
            if summary and hasattr(content, "summary"):
                summary_text = content.summary()
                click.echo("\n" + summary_text + "\n")
                
                # Print detailed counts by category
                click.echo(f"Details ({content_type} content):")
                for attr_name, attr_value in content.__dict__.items():
                    if isinstance(attr_value, list) and attr_value:
                        count = len(attr_value)
                        if count > 0:
                            # Format attribute name
                            display_name = " ".join(word.capitalize() for word in attr_name.split("_"))
                            click.echo(f"  • {display_name}: {count}")
            
            # Print full model if requested
            if print_model:
                model_json = content.model_dump_json(indent=4 if pretty else None)
                if output:
                    with open(output, 'w') as f:
                        f.write(model_json)
                    click.echo(f"✅ {content_type.capitalize()} model data written to {output}")
                else:
                    click.echo(model_json)
        
        logger.info(f"{context_prefix} Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"{context_prefix} Pipeline execution failed: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@click.command()
@pipeline_options
@output_options
@click.pass_context
def run(ctx, **kwargs):
    """Run a data pipeline with the configured adapter and ingester.
    
    Examples:
        pulsepipe run --profile patient_fhir --summary
        pulsepipe run --adapter adapter.yaml --ingester ingester.yaml
        pulsepipe run --pipeline-id pipeline123 --dry-run
    """
    # Execute the async pipeline function
    asyncio.run(_run_pipeline(ctx, **kwargs))