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

# src/pulsepipe/cli/command/run.py

"""
Enhanced implementation of the run command with multi-pipeline support.
This replaces the original run command with functionality to run either
a single pipeline or multiple pipelines from a configuration file.
"""

import sys
import asyncio
import click
from typing import List, Dict, Any, Optional, Union

from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.cli.options import output_options


async def run_single_pipeline(ctx, adapter_config, ingester_config, profile_name, 
                             summary=False, print_model=False, output=None, pretty=True,
                             timeout=30.0, continuous_override=None):
    """Run a single pipeline with the specified adapter and ingester configs."""
    logger = LogFactory.get_logger("pipeline.run")
    
    # Get context from parent command
    pipeline_context = ctx.obj.get('context')
    if pipeline_context and profile_name:
        pipeline_context.profile = profile_name
    
    # Log pipeline start
    context_prefix = pipeline_context.get_log_prefix() if pipeline_context else ""
    logger.info(f"{context_prefix} Starting pipeline execution")
    
    try:
        # Only override continuous mode if explicitly requested
        if adapter_config.get('type') == 'file_watcher' and continuous_override is not None:
            adapter_config['continuous'] = continuous_override
            if continuous_override:
                logger.info(f"Running in continuous watch mode")
            else:
                logger.info(f"Running in one-time processing mode")
            
        # Create adapter and ingester
        adapter_instance = create_adapter(adapter_config)
        ingester_instance = create_ingester(ingester_config)
        
        # Create engine
        engine = IngestionEngine(adapter_instance, ingester_instance)
        
        # Run the engine with timeout - only use timeout if not in continuous mode
        if adapter_config.get('type') == 'file_watcher' and adapter_config.get('continuous', True):
            # For continuous mode, we don't use a timeout
            click.echo(f"Starting continuous watch mode - Press Ctrl+C to stop")
            content = await engine.run(timeout=None)
        else:
            # For non-continuous mode, use the specified timeout
            content = await engine.run(timeout=timeout)
        
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
        return True
        
    except Exception as e:
        logger.error(f"{context_prefix} Pipeline execution failed: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        return False


async def run_from_pipeline_config(ctx, pipeline_config_path, pipeline_names=None, run_all=False,
                                  summary=False, print_model=False, output=None, pretty=True,
                                  timeout=30.0, continuous_override=None):
    """Run multiple pipelines from a pipeline configuration file."""
    logger = LogFactory.get_logger("pipeline.runner")
    pipeline_context = ctx.obj.get('context')
    
    try:
        # Load the pipeline configuration
        config = load_config(pipeline_config_path)
        pipelines = config.get('pipelines', [])
        
        if not pipelines:
            click.echo(f"❌ No pipelines found in {pipeline_config_path}", err=True)
            return False
        
        # Filter pipelines if needed
        if pipeline_names:
            filtered_pipelines = [p for p in pipelines if p['name'] in pipeline_names]
            if not filtered_pipelines:
                click.echo(f"❌ No pipelines found matching names: {', '.join(pipeline_names)}", err=True)
                return False
            target_pipelines = filtered_pipelines
        elif run_all:
            target_pipelines = pipelines  # Run all defined pipelines
        else:
            # Only use active pipelines by default
            target_pipelines = [p for p in pipelines if p.get('active', True)]
        
        if not target_pipelines:
            click.echo("❌ No active pipelines found", err=True)
            return False
        
        # Log what we're going to run
        logger.info(f"Running {len(target_pipelines)} pipeline(s)")
        for p in target_pipelines:
            logger.info(f"  • {p['name']}: {p.get('description', 'No description')}")
        
        # Check if running in continuous mode
        is_continuous = continuous_override is True or any(p.get('adapter', {}).get('continuous', False) for p in target_pipelines)
        
        if is_continuous and len(target_pipelines) > 1:
            # New approach: Run multiple pipelines concurrently in continuous mode
            logger.info("Running multiple pipelines in continuous mode concurrently")
            click.echo(f"🔄 Starting {len(target_pipelines)} pipelines in continuous mode")
            
            # Create tasks for all pipelines to run concurrently
            pipeline_tasks = []
            
            for pipeline in target_pipelines:
                pipeline_name = pipeline['name']
                click.echo(f"\n[Starting pipeline: {pipeline_name} in continuous mode]")
                
                # Instead of copying the context, just use the original context
                # The pipeline-specific information is passed as parameters to run_single_pipeline
                
                # Schedule the pipeline to run
                task = run_single_pipeline(
                    ctx=ctx,
                    adapter_config=pipeline['adapter'],
                    ingester_config=pipeline['ingester'],
                    profile_name=pipeline_name,
                    summary=summary,
                    print_model=print_model,
                    output=f"{output}_{pipeline_name}" if output else None,
                    pretty=pretty,
                    timeout=None,  # No timeout for continuous mode
                    continuous_override=True
                )
                pipeline_tasks.append(task)
            
            # Run all pipelines concurrently and wait for results
            results = await asyncio.gather(*pipeline_tasks, return_exceptions=True)
            
            # Check for any exceptions
            success = True
            for i, result in enumerate(results):
                pipeline_name = target_pipelines[i]['name']
                if isinstance(result, Exception):
                    logger.error(f"Pipeline {pipeline_name} failed with error: {str(result)}")
                    click.echo(f"❌ Pipeline {pipeline_name} failed: {str(result)}", err=True)
                    success = False
                elif not result:
                    logger.error(f"Pipeline {pipeline_name} returned failure status")
                    success = False
            
            return success
            
        elif is_continuous:
            # Single pipeline in continuous mode
            pipeline = target_pipelines[0]
            pipeline_name = pipeline['name']
            
            # Update context for this pipeline
            if pipeline_context:
                pipeline_context.profile = pipeline_name
            
            click.echo(f"\n[Running pipeline: {pipeline_name} in continuous mode]")
            return await run_single_pipeline(
                ctx=ctx,
                adapter_config=pipeline['adapter'],
                ingester_config=pipeline['ingester'],
                profile_name=pipeline_name,
                summary=summary,
                print_model=print_model,
                output=output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=True  # Force continuous mode
            )
        
        # Run each pipeline in one-time mode
        results = []
        for pipeline in target_pipelines:
            # Update context for this pipeline
            if pipeline_context:
                pipeline_context.profile = pipeline['name']
            
            # Generate output path with pipeline name if multiple pipelines
            pipeline_output = None
            if output and len(target_pipelines) > 1:
                import os
                base, ext = os.path.splitext(output)
                pipeline_output = f"{base}_{pipeline['name']}{ext}"
            elif output:
                pipeline_output = output
            
            click.echo(f"\n[Running pipeline: {pipeline['name']}]")
            success = await run_single_pipeline(
                ctx=ctx,
                adapter_config=pipeline['adapter'],
                ingester_config=pipeline['ingester'],
                profile_name=pipeline['name'],
                summary=summary,
                print_model=print_model,
                output=pipeline_output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=False  # Force non-continuous mode when running multiple pipelines
            )
            results.append((pipeline['name'], success))
        
        # Print summary of results
        if len(results) > 1:
            click.echo("\nPipeline Execution Summary:")
            for name, success in results:
                status = "✅ Success" if success else "❌ Failed"
                click.echo(f"{status}: {name}")
        
        # Return True only if all pipelines succeeded
        return all(success for _, success in results)
    
    except Exception as e:
        logger.error(f"Error running pipelines: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        return False


@click.command()
@click.option('--adapter', '-a', type=click.Path(exists=True, dir_okay=False, resolve_path=True),
              help='Path to adapter configuration file')
@click.option('--ingester', '-i', type=click.Path(exists=True, dir_okay=False, resolve_path=True),
              help='Path to ingester configuration file')
@click.option('--pipeline-config', '-p', type=click.Path(exists=True, dir_okay=False, resolve_path=True),
              help='Path to pipeline configuration file (for running multiple pipelines)')
@click.option('--pipeline', '-n', multiple=True,
              help='Name of specific pipeline(s) to run from pipeline config (can be used multiple times)')
@click.option('--all', 'run_all', is_flag=True, 
              help='Run all pipelines in config, including inactive ones')
@click.option('--timeout', type=float, default=30.0,
              help='Timeout in seconds for processing files (default: 30.0)')
@click.option('--continuous/--one-time', 'continuous_mode', default=None,
              help='Run in continuous watch mode or one-time processing mode')
@click.option('--dry-run', '-d', is_flag=True,
              help='Validate configuration without running the pipeline')
@click.option('--user-id', type=str, help='User identifier for logging/auditing (PulsePilot)')
@click.option('--org-id', type=str, help='Organization identifier for multi-tenant usage (PulsePilot)')
@output_options
@click.pass_context
def run(ctx, adapter, ingester, pipeline_config, pipeline, run_all, timeout, continuous_mode,
       dry_run, user_id, org_id, print_model, summary, output, pretty):
    """Run data pipelines with configured adapters and ingesters.
    
    This command can run either a single pipeline (with adapter and ingester options)
    or multiple pipelines defined in a pipeline configuration file.
    
    Examples:
        # Run a single pipeline with explicit adapter and ingester configs
        pulsepipe run --adapter adapter.yaml --ingester ingester.yaml
        
        # Run using a profile with auto-configuration
        pulsepipe run --profile patient_fhir
        
        # Run with pipeline configuration file (all active pipelines)
        pulsepipe run --pipeline-config pipeline.yaml
        
        # Run specific pipeline(s) from config
        pulsepipe run --pipeline-config pipeline.yaml --pipeline fhir_clinical
        
        # Run multiple specific pipelines
        pulsepipe run -p pipeline.yaml -n fhir_clinical -n x12_billing
        
        # Run in continuous watch mode (waiting for new files)
        pulsepipe run --pipeline-config pipeline.yaml --continuous
        
        # Show summary after pipeline execution
        pulsepipe run --summary
        
        # Print the full model
        pulsepipe run --print-model
    """
    # Update context from parameters
    pipeline_context = ctx.obj.get('context')
    if pipeline_context:
        if user_id:
            pipeline_context.user_id = user_id
        if org_id:
            pipeline_context.org_id = org_id
        pipeline_context.is_dry_run = dry_run
    
    # Check for conflicting options
    if (adapter or ingester) and pipeline_config:
        click.echo("❌ Error: Cannot use both adapter/ingester options and pipeline-config together", err=True)
        ctx.exit(1)
    
    # Early exit for dry run
    if dry_run:
        try:
            if pipeline_config:
                config = load_config(pipeline_config)
                pipelines = config.get('pipelines', [])
                if pipeline:
                    pipelines = [p for p in pipelines if p['name'] in pipeline]
                for p in pipelines:
                    click.echo(f"✅ Validated pipeline: {p['name']}")
            elif adapter and ingester:
                adapter_config = load_config(adapter).get('adapter', {})
                ingester_config = load_config(ingester).get('ingester', {})
                click.echo(f"✅ Adapter config valid: {adapter_config.get('type', 'unknown')}")
                click.echo(f"✅ Ingester config valid: {ingester_config.get('type', 'unknown')}")
            else:
                profile = ctx.parent.params.get('profile')
                if not profile:
                    click.echo("❌ No configuration specified. Use --adapter/--ingester or --pipeline-config", err=True)
                    ctx.exit(1)
                click.echo(f"✅ Profile config valid: {profile}")
            return
        except Exception as e:
            click.echo(f"❌ Configuration validation failed: {str(e)}", err=True)
            ctx.exit(1)
    
    # Choose execution path based on options
    try:
        if pipeline_config:
            # Run from pipeline config file
            success = asyncio.run(run_from_pipeline_config(
                ctx=ctx,
                pipeline_config_path=pipeline_config,
                pipeline_names=list(pipeline) if pipeline else None,
                run_all=run_all,
                summary=summary,
                print_model=print_model,
                output=output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=continuous_mode
            ))
        else:
            # Run a single pipeline
            # Determine config sources
            if adapter and ingester:
                adapter_config = load_config(adapter).get('adapter', {})
                ingester_config = load_config(ingester).get('ingester', {})
            else:
                # Use profile-based config if available
                profile = ctx.parent.params.get('profile')
                if not profile:
                    click.echo("❌ No configuration specified. Use --adapter/--ingester or --pipeline-config or --profile", err=True)
                    ctx.exit(1)
                
                config = ctx.obj.get('config', {})
                adapter_config = config.get('adapter', {})
                ingester_config = config.get('ingester', {})
            
            success = asyncio.run(run_single_pipeline(
                ctx=ctx,
                adapter_config=adapter_config,
                ingester_config=ingester_config,
                profile_name=ctx.parent.params.get('profile'),
                summary=summary, 
                print_model=print_model,
                output=output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=continuous_mode
            ))
        
        if not success:
            ctx.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        ctx.exit(1)
