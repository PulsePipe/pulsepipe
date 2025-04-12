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
import os
import json
import asyncio
import click
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.cli.options import output_options
from pulsepipe.pipelines.chunkers.clinical_chunker import ClinicalSectionChunker
from pulsepipe.pipelines.chunkers.operational_chunker import OperationalEntityChunker
from pulsepipe.utils.errors import (
    PulsePipeError, ConfigurationError, MissingConfigurationError,
    AdapterError, IngesterError, IngestionEngineError, ChunkerError,
    FileSystemError, CLIError
)


def display_error(error: PulsePipeError, verbose: bool = False):
    """Display error information to the user in a structured, helpful format."""
    click.secho(f"❌ Error: {error.message}", fg='red', bold=True)
    
    # Show additional error context if available
    if error.details and verbose:
        click.echo("\nError details:")
        for key, value in error.details.items():
            click.echo(f"  {key}: {value}")
    
    # Show original cause if available and in verbose mode
    if error.cause and verbose:
        click.echo(f"\nCaused by: {type(error.cause).__name__}: {str(error.cause)}")
    
    # Provide hints based on error type
    if isinstance(error, ConfigurationError):
        click.echo("\nSuggestions:")
        click.echo("  • Check your configuration file for errors")
        click.echo("  • Run 'pulsepipe config validate' to validate your configuration")
        click.echo("  • Ensure all required fields are present")
    
    elif isinstance(error, AdapterError):
        click.echo("\nSuggestions:")
        click.echo("  • Verify the adapter configuration is correct")
        click.echo("  • Check that input sources are accessible")
        click.echo("  • Verify file permissions and paths")
    
    elif isinstance(error, IngesterError):
        click.echo("\nSuggestions:")
        click.echo("  • Verify that input data format matches the configured ingester")
        click.echo("  • Check for malformed or invalid input data")
    
    elif isinstance(error, ChunkerError):
        click.echo("\nSuggestions:")
        click.echo("  • Check the chunker configuration")
        click.echo("  • Verify that the data model is compatible with the chunker")


def find_profile_path(profile_name: str) -> Optional[str]:
    """Find the profile configuration file in the appropriate directories."""
    # Define possible locations in priority order
    possible_locations = [
        # 1. Check in ./config/ next to the binary
        os.path.join("config", f"{profile_name}.yaml"),
        # 2. Check in the current directory
        f"{profile_name}.yaml",
        # 3. Check in the location relative to the script directory (for development)
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "config", f"{profile_name}.yaml"),
        # 4. As a fallback, try the src path
        os.path.join("src", "pulsepipe", "config", f"{profile_name}.yaml"),
    ]
    
    # Try each location
    for location in possible_locations:
        if os.path.exists(location):
            return location
    
    # Return None if not found
    return None


async def run_single_pipeline(ctx, adapter_config, ingester_config, profile_name,
                             summary=False, print_model=False, output=None, pretty=True,
                             timeout=30.0, continuous_override=None, chunker_config=None,
                             verbose=False):
    """
    Run a single pipeline with the given configuration.
    
    Args:
        ctx: Click context
        adapter_config: Adapter configuration dictionary
        ingester_config: Ingester configuration dictionary
        profile_name: Name of the profile being used
        summary: Whether to display a summary
        print_model: Whether to print the full model
        output: Output file path
        pretty: Whether to use pretty printing
        timeout: Timeout for processing
        continuous_override: Override for continuous mode
        chunker_config: Chunker configuration
        verbose: Whether to show verbose error information
        
    Returns:
        Boolean indicating success or failure
    """
    logger = LogFactory.get_logger("pipeline.run")
    pipeline_context = ctx.obj.get('context')
    if pipeline_context and profile_name:
        pipeline_context.profile = profile_name

    context_prefix = pipeline_context.get_log_prefix() if pipeline_context else ""
    logger.info(f"{context_prefix} Starting pipeline execution")

    try:
        # Override continuous mode if specified
        if adapter_config.get('type') == 'file_watcher' and continuous_override is not None:
            adapter_config['continuous'] = continuous_override
            mode_desc = "continuous watch" if continuous_override else "one-time processing"
            logger.info(f"Running in {mode_desc} mode")

        # Create adapter and ingester
        try:
            adapter_instance = create_adapter(adapter_config)
        except Exception as e:
            raise AdapterError(
                f"Failed to create adapter: {str(e)}",
                details={"adapter_type": adapter_config.get("type", "unknown")},
                cause=e
            ) from e
            
        try:
            ingester_instance = create_ingester(ingester_config)
        except Exception as e:
            raise IngesterError(
                f"Failed to create ingester: {str(e)}",
                details={"ingester_type": ingester_config.get("type", "unknown")},
                cause=e
            ) from e

        # Create and run the ingestion engine
        engine = IngestionEngine(adapter_instance, ingester_instance)
        
        if adapter_config.get('type') == 'file_watcher' and adapter_config.get('continuous', True):
            click.echo("Starting continuous watch mode - Press Ctrl+C to stop")
            content = await engine.run(timeout=None)
        else:
            content = await engine.run(timeout=timeout)

        # Process the results
        if content:
            # Handle case where content is a list (batch processed)
            if isinstance(content, list):
                logger.info(f"Processed {len(content)} items from batch")
                
                if summary:
                    click.echo(f"\nProcessed {len(content)} items in batch:")
                    for i, item in enumerate(content):
                        click.echo(f"\n--- Item {i+1} ---")
                        if hasattr(item, "summary"):
                            click.echo(item.summary())
                
                if print_model:
                    if output:
                        # Create individual output files for each item
                        for i, item in enumerate(content):
                            item_output = f"{os.path.splitext(output)[0]}_{i+1}{os.path.splitext(output)[1]}"
                            model_json = item.model_dump_json(indent=4 if pretty else None)
                            try:
                                with open(item_output, 'w') as f:
                                    f.write(model_json)
                            except Exception as e:
                                raise FileSystemError(
                                    f"Failed to write output file: {item_output}",
                                    cause=e
                                ) from e
                        click.echo(f"✅ Batch data written to {len(content)} files with prefix {output}")
                    else:
                        # Print to console
                        for i, item in enumerate(content):
                            click.echo(f"\n--- Item {i+1} ---")
                            click.echo(item.model_dump_json(indent=4 if pretty else None))
                
                # Chunking for batch
                if chunker_config:
                    chunker_type = chunker_config.get("type", "auto")
                    chunk_export_format = chunker_config.get("export_chunks_to", None)
                    
                    for i, item in enumerate(content):
                        chunker = None
                        content_type = "unknown"
                        
                        if "Clinical" in item.__class__.__name__:
                            chunker = ClinicalSectionChunker()
                            content_type = "clinical"
                        elif "Operational" in item.__class__.__name__:
                            chunker = OperationalEntityChunker()
                            content_type = "operational"
                            
                        if chunker:
                            try:
                                chunks = chunker.chunk(item)
                                click.echo(f"🧬 Item {i+1}: Chunked into {len(chunks)} sections")
                                
                                if chunk_export_format == "jsonl" and output:
                                    base, ext = os.path.splitext(output)
                                    chunk_output_path = f"{base}_{i+1}.chunks.jsonl"
                                    try:
                                        with open(chunk_output_path, "w") as f:
                                            for c in chunks:
                                                f.write(json.dumps(c) + "\n")
                                        click.echo(f"✅ Chunked output for item {i+1} written to {chunk_output_path}")
                                    except Exception as e:
                                        raise FileSystemError(
                                            f"Failed to write chunk output file: {chunk_output_path}",
                                            cause=e
                                        ) from e
                            except Exception as e:
                                raise ChunkerError(
                                    f"Error chunking {content_type} content: {str(e)}",
                                    details={"content_type": content_type, "chunker_type": chunker_type},
                                    cause=e
                                ) from e
            
            else:
                # Handle single item 
                content_type = "unknown"
                if hasattr(content, "__class__") and hasattr(content.__class__, "__name__"):
                    if "Clinical" in content.__class__.__name__:
                        content_type = "clinical"
                    elif "Operational" in content.__class__.__name__:
                        content_type = "operational"

                if summary and hasattr(content, "summary"):
                    click.echo("\n" + content.summary() + "\n")

                if print_model:
                    model_json = content.model_dump_json(indent=4 if pretty else None)
                    if output:
                        try:
                            with open(output, 'w') as f:
                                f.write(model_json)
                            click.echo(f"✅ {content_type.capitalize()} model data written to {output}")
                        except Exception as e:
                            raise FileSystemError(
                                f"Failed to write output file: {output}",
                                cause=e
                            ) from e
                    else:
                        click.echo(model_json)

                # 🔹 Chunking integration
                chunker_type = (chunker_config or {}).get("type", "auto")
                chunk_export_format = (chunker_config or {}).get("export_chunks_to", None)

                chunker = None
                if chunker_type == "auto":
                    if "Clinical" in content.__class__.__name__:
                        chunker = ClinicalSectionChunker()
                    elif "Operational" in content.__class__.__name__:
                        chunker = OperationalEntityChunker()

                if chunker:
                    try:
                        chunks = chunker.chunk(content)
                        click.echo(f"🧬 Chunked into {len(chunks)} sections")

                        if chunk_export_format == "jsonl" and output:
                            base, ext = os.path.splitext(output)
                            chunk_output_path = f"{base}.chunks.jsonl"
                            try:
                                with open(chunk_output_path, "w") as f:
                                    for c in chunks:
                                        f.write(json.dumps(c) + "\n")
                                click.echo(f"✅ Chunked output written to {chunk_output_path}")
                            except Exception as e:
                                raise FileSystemError(
                                    f"Failed to write chunk output file: {chunk_output_path}",
                                    cause=e
                                ) from e
                    except Exception as e:
                        raise ChunkerError(
                            f"Error chunking {content_type} content: {str(e)}",
                            details={"content_type": content_type, "chunker_type": chunker_type},
                            cause=e
                        ) from e

        logger.info(f"{context_prefix} Pipeline execution completed successfully")
        return True

    except PulsePipeError as e:
        # Display errors in a user-friendly way
        display_error(e, verbose=verbose)
        logger.error(f"{context_prefix} Pipeline execution failed: {e.message}")
        return False
    except Exception as e:
        # Wrap unexpected exceptions
        error = IngestionEngineError(
            f"Unexpected error in pipeline execution: {str(e)}",
            cause=e
        )
        display_error(error, verbose=verbose)
        logger.error(f"{context_prefix} Pipeline execution failed with unexpected error", exc_info=True)
        return False


async def run_from_pipeline_config(ctx, pipeline_config_path, pipeline_names=None, run_all=False,
                                  summary=False, print_model=False, output=None, pretty=True,
                                  timeout=30.0, continuous_override=None, verbose=False):
    """
    Run multiple pipelines from a configuration file.
    
    Args:
        ctx: Click context
        pipeline_config_path: Path to the pipeline configuration file
        pipeline_names: List of specific pipelines to run
        run_all: Whether to run all pipelines including inactive ones
        summary: Whether to display a summary
        print_model: Whether to print the full model
        output: Output file path
        pretty: Whether to use pretty printing
        timeout: Timeout for processing
        continuous_override: Override for continuous mode
        verbose: Whether to show verbose error information
        
    Returns:
        Boolean indicating success or failure
    """
    logger = LogFactory.get_logger("pipeline.runner")
    pipeline_context = ctx.obj.get('context')

    try:
        # Load the pipeline configuration file
        try:
            config = load_config(pipeline_config_path)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load pipeline configuration file: {pipeline_config_path}",
                cause=e
            ) from e
        
        pipelines = config.get('pipelines', [])
        if not pipelines:
            raise ConfigurationError(
                f"No pipelines found in {pipeline_config_path}",
                details={"pipeline_config_path": pipeline_config_path}
            )

        # Determine which pipelines to run
        if pipeline_names:
            target_pipelines = [p for p in pipelines if p.get('name') in pipeline_names]
            if not target_pipelines:
                raise ConfigurationError(
                    f"No matching pipelines found for names: {', '.join(pipeline_names)}",
                    details={"available_pipelines": [p.get('name') for p in pipelines]}
                )
        elif run_all:
            target_pipelines = pipelines
        else:
            target_pipelines = [p for p in pipelines if p.get('active', True)]
            if not target_pipelines:
                raise ConfigurationError(
                    "No active pipelines found in configuration",
                    details={"pipeline_count": len(pipelines), "hint": "Use --all to run inactive pipelines"}
                )

        logger.info(f"Running {len(target_pipelines)} pipeline(s)")
        for p in target_pipelines:
            logger.info(f"  • {p.get('name', 'unnamed')}: {p.get('description', 'No description')}")

        # Determine if we're running in continuous mode
        is_continuous = continuous_override is True or any(
            p.get('adapter', {}).get('continuous', False) for p in target_pipelines)

        # Run multiple pipelines concurrently if in continuous mode
        if is_continuous and len(target_pipelines) > 1:
            click.echo(f"🔄 Starting {len(target_pipelines)} pipelines in continuous mode")
            tasks = [
                run_single_pipeline(
                    ctx=ctx,
                    adapter_config=p.get('adapter', {}),
                    ingester_config=p.get('ingester', {}),
                    profile_name=p.get('name', 'unnamed'),
                    summary=summary,
                    print_model=print_model,
                    output=f"{output}_{p.get('name')}" if output else None,
                    pretty=pretty,
                    timeout=None,  # No timeout for continuous mode
                    continuous_override=True,
                    chunker_config=p.get("chunker", {}),
                    verbose=verbose
                ) for p in target_pipelines
            ]
            
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # Process results
                success_count = sum(1 for r in results if r is True)
                if success_count < len(target_pipelines):
                    click.echo(f"⚠️ {success_count} of {len(target_pipelines)} pipelines completed successfully")
                return all(r is True for r in results)
            except Exception as e:
                raise IngestionEngineError(
                    f"Error running pipelines concurrently: {str(e)}",
                    cause=e
                ) from e

        # Run pipelines sequentially
        results = []
        for p in target_pipelines:
            pipeline_name = p.get('name', 'unnamed')
            click.echo(f"\n🚀 Running pipeline: {pipeline_name}")
            
            # Prepare output path if specified
            pipeline_output = None
            if output:
                base, ext = os.path.splitext(output)
                pipeline_output = f"{base}_{pipeline_name}{ext}"
            
            # Run the pipeline
            result = await run_single_pipeline(
                ctx=ctx,
                adapter_config=p.get('adapter', {}),
                ingester_config=p.get('ingester', {}),
                profile_name=pipeline_name,
                summary=summary,
                print_model=print_model,
                output=pipeline_output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=continuous_override,
                chunker_config=p.get("chunker", {}),
                verbose=verbose
            )
            results.append((pipeline_name, result))

        # Show summary if multiple pipelines were run
        if len(results) > 1:
            click.echo("\nPipeline Execution Summary:")
            for name, success in results:
                status = "✅ Success" if success else "❌ Failed"
                click.echo(f"{status}: {name}")

        return all(success for _, success in results)

    except PulsePipeError as e:
        # Display errors in a user-friendly way
        display_error(e, verbose=verbose)
        logger.error(f"Pipeline runner error: {e.message}")
        return False
    except Exception as e:
        # Wrap unexpected exceptions
        error = IngestionEngineError(
            f"Unexpected error running pipelines: {str(e)}",
            cause=e
        )
        display_error(error, verbose=verbose)
        logger.error(f"Pipeline runner failed with unexpected error", exc_info=True)
        return False


@click.command()
@click.option('--adapter', '-a', type=click.Path(exists=True, dir_okay=False), help="Adapter config YAML")
@click.option('--ingester', '-i', type=click.Path(exists=True, dir_okay=False), help="Ingester config YAML")
@click.option('--profile', '-p', type=str, help="Profile name to use (e.g., new_profile)")
@click.option('--pipeline-config', '-pc', type=click.Path(exists=True, dir_okay=False), help="Pipeline config YAML")
@click.option('--pipeline', '-n', multiple=True, help="Specific pipeline names to run")
@click.option('--all', 'run_all', is_flag=True, help="Run all pipelines in config")
@click.option('--timeout', type=float, default=30.0, help="Timeout for file-based processing")
@click.option('--continuous/--one-time', 'continuous_mode', default=None)
@click.option('--verbose', '-v', is_flag=True, help="Show detailed error information")
@output_options
@click.pass_context
def run(ctx, adapter, ingester, profile, pipeline_config, pipeline, run_all, timeout,
        continuous_mode, print_model, summary, output, pretty, verbose):
    """Run a data processing pipeline.
    
    Process healthcare data through configurable adapters and ingesters.
    """
    pipeline_context = ctx.obj.get('context')
    try:
        # Handle profile-based execution
        if profile:
            # Find the profile path using the helper function
            profile_path = find_profile_path(profile)
            
            if not profile_path:
                raise MissingConfigurationError(
                    f"Profile not found: {profile}",
                    details={
                        "searched_locations": [
                            "./config/", 
                            "./", 
                            "src/pulsepipe/config/"
                        ]
                    }
                )
            
            try:
                profile_config = load_config(profile_path)
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load profile configuration: {profile}",
                    details={"profile_path": profile_path},
                    cause=e
                ) from e
                
            adapter_config = profile_config.get('adapter', {})
            if not adapter_config:
                raise ConfigurationError(
                    f"Profile '{profile}' is missing adapter configuration",
                    details={"profile_path": profile_path}
                )
                
            ingester_config = profile_config.get('ingester', {})
            if not ingester_config:
                raise ConfigurationError(
                    f"Profile '{profile}' is missing ingester configuration",
                    details={"profile_path": profile_path}
                )
                
            chunker_config = profile_config.get('chunker', {})
            
            # Set context profile name
            if pipeline_context:
                pipeline_context.profile = profile
                
            click.echo(f"📋 Using profile: {profile} from {profile_path}")
            success = asyncio.run(run_single_pipeline(
                ctx=ctx,
                adapter_config=adapter_config,
                ingester_config=ingester_config,
                profile_name=profile,
                summary=summary,
                print_model=print_model,
                output=output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=continuous_mode,
                chunker_config=chunker_config,
                verbose=verbose
            ))
        
        # Handle pipeline config execution
        elif pipeline_config:
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
                continuous_override=continuous_mode,
                verbose=verbose
            ))
        
        # Handle explicit adapter/ingester config execution
        elif adapter and ingester:
            try:
                adapter_config = load_config(adapter).get('adapter', {})
                if not adapter_config:
                    raise ConfigurationError(
                        f"File '{adapter}' does not contain adapter configuration",
                        details={"adapter_file": adapter}
                    )
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(
                    f"Failed to load adapter configuration: {adapter}",
                    details={"adapter_file": adapter},
                    cause=e
                ) from e
                
            try:
                ingester_config = load_config(ingester).get('ingester', {})
                if not ingester_config:
                    raise ConfigurationError(
                        f"File '{ingester}' does not contain ingester configuration",
                        details={"ingester_file": ingester}
                    )
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(
                    f"Failed to load ingester configuration: {ingester}",
                    details={"ingester_file": ingester},
                    cause=e
                ) from e
                
            success = asyncio.run(run_single_pipeline(
                ctx=ctx,
                adapter_config=adapter_config,
                ingester_config=ingester_config,
                profile_name="default",
                summary=summary,
                print_model=print_model,
                output=output,
                pretty=pretty,
                timeout=timeout,
                continuous_override=continuous_mode,
                chunker_config=ingester_config.get("chunker", {}),
                verbose=verbose
            ))
        
        # No configuration provided
        else:
            raise CLIError(
                "You must specify either --profile, --pipeline-config, or both --adapter and --ingester",
                details={
                    "profile": profile,
                    "pipeline_config": pipeline_config,
                    "adapter": adapter,
                    "ingester": ingester
                }
            )
            
        if not success:
            ctx.exit(1)
            
    except PulsePipeError as e:
        # Display errors in a user-friendly way
        display_error(e, verbose=verbose)
        ctx.exit(1)
    except Exception as e:
        # Wrap unexpected exceptions
        error = CLIError(
            f"Unexpected error in command execution: {str(e)}",
            cause=e
        )
        display_error(error, verbose=verbose)
        ctx.exit(1)
