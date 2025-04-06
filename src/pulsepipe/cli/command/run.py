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

from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.ingesters.ingestion_engine import IngestionEngine
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.cli.options import output_options
from pulsepipe.pipelines.chunkers.clinical_chunker import ClinicalSectionChunker
from pulsepipe.pipelines.chunkers.operational_chunker import OperationalEntityChunker


async def run_single_pipeline(ctx, adapter_config, ingester_config, profile_name,
                             summary=False, print_model=False, output=None, pretty=True,
                             timeout=30.0, continuous_override=None, chunker_config=None):
    logger = LogFactory.get_logger("pipeline.run")
    pipeline_context = ctx.obj.get('context')
    if pipeline_context and profile_name:
        pipeline_context.profile = profile_name

    context_prefix = pipeline_context.get_log_prefix() if pipeline_context else ""
    logger.info(f"{context_prefix} Starting pipeline execution")

    try:
        if adapter_config.get('type') == 'file_watcher' and continuous_override is not None:
            adapter_config['continuous'] = continuous_override
            logger.info("Running in continuous watch mode" if continuous_override else "Running in one-time processing mode")

        adapter_instance = create_adapter(adapter_config)
        ingester_instance = create_ingester(ingester_config)
        engine = IngestionEngine(adapter_instance, ingester_instance)

        if adapter_config.get('type') == 'file_watcher' and adapter_config.get('continuous', True):
            click.echo("Starting continuous watch mode - Press Ctrl+C to stop")
            content = await engine.run(timeout=None)
        else:
            content = await engine.run(timeout=timeout)

        if content:
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
                    with open(output, 'w') as f:
                        f.write(model_json)
                    click.echo(f"✅ {content_type.capitalize()} model data written to {output}")
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
                chunks = chunker.chunk(content)
                click.echo(f"🧬 Chunked into {len(chunks)} sections")

                if chunk_export_format == "jsonl" and output:
                    base, ext = os.path.splitext(output)
                    chunk_output_path = f"{base}.chunks.jsonl"
                    with open(chunk_output_path, "w") as f:
                        for c in chunks:
                            f.write(json.dumps(c) + "\n")
                    click.echo(f"✅ Chunked output written to {chunk_output_path}")

        logger.info(f"{context_prefix} Pipeline execution completed successfully")
        return True

    except Exception as e:
        logger.error(f"{context_prefix} Pipeline execution failed: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        return False


async def run_from_pipeline_config(ctx, pipeline_config_path, pipeline_names=None, run_all=False,
                                  summary=False, print_model=False, output=None, pretty=True,
                                  timeout=30.0, continuous_override=None):
    logger = LogFactory.get_logger("pipeline.runner")
    pipeline_context = ctx.obj.get('context')

    try:
        config = load_config(pipeline_config_path)
        pipelines = config.get('pipelines', [])

        if not pipelines:
            click.echo(f"❌ No pipelines found in {pipeline_config_path}", err=True)
            return False

        if pipeline_names:
            target_pipelines = [p for p in pipelines if p['name'] in pipeline_names]
        elif run_all:
            target_pipelines = pipelines
        else:
            target_pipelines = [p for p in pipelines if p.get('active', True)]

        if not target_pipelines:
            click.echo("❌ No active pipelines found", err=True)
            return False

        logger.info(f"Running {len(target_pipelines)} pipeline(s)")
        for p in target_pipelines:
            logger.info(f"  • {p['name']}: {p.get('description', 'No description')}")

        is_continuous = continuous_override is True or any(
            p.get('adapter', {}).get('continuous', False) for p in target_pipelines)

        if is_continuous and len(target_pipelines) > 1:
            click.echo(f"🔄 Starting {len(target_pipelines)} pipelines in continuous mode")
            tasks = [
                run_single_pipeline(
                    ctx=ctx,
                    adapter_config=p['adapter'],
                    ingester_config=p['ingester'],
                    profile_name=p['name'],
                    summary=summary,
                    print_model=print_model,
                    output=f"{output}_{p['name']}" if output else None,
                    pretty=pretty,
                    timeout=None,
                    continuous_override=True,
                    chunker_config=p.get("chunker", {})
                ) for p in target_pipelines
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return all(r is True for r in results)

        results = []
        for p in target_pipelines:
            result = await run_single_pipeline(
                ctx=ctx,
                adapter_config=p['adapter'],
                ingester_config=p['ingester'],
                profile_name=p['name'],
                summary=summary,
                print_model=print_model,
                output=f"{os.path.splitext(output)[0]}_{p['name']}.json" if output else None,
                pretty=pretty,
                timeout=timeout,
                continuous_override=False,
                chunker_config=p.get("chunker", {})
            )
            results.append((p['name'], result))

        if len(results) > 1:
            click.echo("\nPipeline Execution Summary:")
            for name, success in results:
                status = "✅ Success" if success else "❌ Failed"
                click.echo(f"{status}: {name}")

        return all(success for _, success in results)

    except Exception as e:
        logger.error(f"Error running pipelines: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        return False


@click.command()
@click.option('--adapter', '-a', type=click.Path(exists=True, dir_okay=False), help="Adapter config YAML")
@click.option('--ingester', '-i', type=click.Path(exists=True, dir_okay=False), help="Ingester config YAML")
@click.option('--pipeline-config', '-p', type=click.Path(exists=True, dir_okay=False), help="Pipeline config YAML")
@click.option('--pipeline', '-n', multiple=True, help="Specific pipeline names to run")
@click.option('--all', 'run_all', is_flag=True, help="Run all pipelines in config")
@click.option('--timeout', type=float, default=30.0, help="Timeout for file-based processing")
@click.option('--continuous/--one-time', 'continuous_mode', default=None)
@output_options
@click.pass_context
def run(ctx, adapter, ingester, pipeline_config, pipeline, run_all, timeout,
        continuous_mode, print_model, summary, output, pretty):

    pipeline_context = ctx.obj.get('context')
    try:
        if pipeline_config:
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
            adapter_config = load_config(adapter).get('adapter', {})
            ingester_config = load_config(ingester).get('ingester', {})
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
                chunker_config=ingester_config.get("chunker", {})
            ))
        if not success:
            ctx.exit(1)
    except Exception as e:
        click.echo(f"❌ CLI Error: {str(e)}", err=True)
        ctx.exit(1)
