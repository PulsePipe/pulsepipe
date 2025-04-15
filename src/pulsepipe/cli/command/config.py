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

# src/pulsepipe/cli/commands/config.py

"""
Configuration management commands for PulsePipe CLI.
"""


import os
import shutil
import yaml
import click
from typing import Dict, Any
from datetime import datetime
from pathlib import Path

from pulsepipe.adapters.file_watcher_bookmarks.sqlite_store import SQLiteBookmarkStore
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config
from pulsepipe.persistence.factory import get_shared_sqlite_connection

@click.group()
@click.pass_context
def config(ctx):
    """Manage PulsePipe configuration.
    
    View, validate, and manage configuration profiles.
    """
    pass

@click.group(invoke_without_command=True)
@click.pass_context
def config(ctx):
    """Manage PulsePipe configuration.

    View, validate, and manage configuration profiles.
    """
    if ctx.invoked_subcommand is None:
        # Display the current configuration
        current_config = ctx.obj.get('config')
        if current_config is None:
            click.echo("No configuration loaded.", err=True)
            ctx.exit(1)
        click.echo(yaml.dump(current_config, default_flow_style=False, sort_keys=False))

@config.command()
@click.option('--profile', '-p', type=str, help='Profile name to validate')
@click.option('--all', 'validate_all', is_flag=True, 
              help='Validate all profiles in config directory')
@click.pass_context
def validate(ctx, profile, validate_all):
    """Validate configuration files.
    
    Examples:
        pulsepipe config
        pulsepipe config validate --profile patient_fhir
        pulsepipe config validate --all
    """
    logger = LogFactory.get_logger("config.validate")
    
    config_dir = Path("config")
    if not config_dir.exists():
        config_dir = Path("src/pulsepipe/config")
    
    if validate_all:
        profiles = [f for f in config_dir.glob("*.yaml") 
                   if f.is_file() and not f.name.startswith("_")]
        
        if not profiles:
            click.echo("No profiles found in config directory")
            return
            
        success_count = 0
        for profile_path in profiles:
            try:
                load_config(str(profile_path))
                click.echo(f"✅ {profile_path.name}: Valid")
                success_count += 1
            except Exception as e:
                click.echo(f"❌ {profile_path.name}: Invalid - {str(e)}")
        
        click.echo(f"\nValidated {len(profiles)} profiles. "
                  f"{success_count} valid, {len(profiles) - success_count} invalid.")
    
    elif profile:
        profile_path = config_dir / f"{profile}.yaml"
        
        if not profile_path.exists():
            click.echo(f"❌ Profile not found: {profile}", err=True)
            return
            
        try:
            config = load_config(str(profile_path))
            click.echo(f"✅ {profile}: Valid")
            
            # Show config components
            components = []
            if 'adapter' in config:
                components.append(f"Adapter: {config['adapter'].get('type', 'unknown')}")
            if 'ingester' in config:
                components.append(f"Ingester: {config['ingester'].get('type', 'unknown')}")
            if 'logging' in config:
                components.append(f"Logging: {config['logging'].get('level', 'INFO')}")
                
            if components:
                click.echo("\nComponents:")
                for comp in components:
                    click.echo(f"  • {comp}")
                    
        except Exception as e:
            click.echo(f"❌ {profile}: Invalid - {str(e)}", err=True)
    
    else:
        click.echo(ctx.get_help())


@config.command()
@click.option('--base', '-b', type=str, default="pulsepipe.yaml",
              help='Base config file (default: pulsepipe.yaml)')
@click.option('--adapter', '-a', type=str, required=True,
              help='Adapter config file')
@click.option('--ingester', '-i', type=str, required=True,
              help='Ingester config file')
@click.option('--chunker', '-c', type=str,
              help='Chunker config file')
@click.option('--embedding', '-e', type=str,
              help='Embedding config file')
@click.option('--vectorstore', '-vs', type=str,
              help='Vectorstore config file')
@click.option('--name', '-n', type=str, required=True,
              help='Profile name to create')
@click.option('--description', '-d', type=str,
              help='Profile description')
@click.option('--force', '-f', is_flag=True,
              help='Overwrite existing profile')
def create_profile(base, adapter, ingester, chunker, embedding, vectorstore, name, description, force):
    """Create a unified profile from separate config files.
    
    Examples:
        pulsepipe config create-profile --adapter fhir.yaml --ingester json.yaml --name patient_fhir
        pulsepipe config create-profile --adapter fhir.yaml --ingester json.yaml --chunker chunker.yaml \\
            --embedding embedding.yaml --vectorstore vectorstore.yaml --name complete_fhir
    """
    logger = LogFactory.get_logger("config.create_profile")
    
    # Ensure config directory exists - prioritize config next to the binary
    config_dir = Path("config")
    if not config_dir.exists():
        config_dir.mkdir(parents=True)
        logger.info(f"📁 Created config directory: {config_dir}")
    
    # Check if profile already exists
    profile_path = config_dir / f"{name}.yaml"
    if profile_path.exists() and not force:
        click.echo(f"❌ Profile already exists: {name}. Use --force to overwrite.", err=True)
        return
    
    try:
        # Load component configs
        base_config = load_config(base)
        adapter_config = load_config(adapter)
        ingester_config = load_config(ingester)
        
        # Load optional components
        chunker_config = {}
        embedding_config = {}
        vectorstore_config = {}
        
        if chunker:
            chunker_config = load_config(chunker)
            if not chunker_config.get("chunker"):
                click.echo(f"⚠️ Warning: '{chunker}' does not contain a chunker configuration")
        
        if embedding:
            embedding_config = load_config(embedding)
            if not embedding_config.get("embedding"):
                click.echo(f"⚠️ Warning: '{embedding}' does not contain an embedding configuration")
        
        if vectorstore:
            vectorstore_config = load_config(vectorstore)
            if not vectorstore_config.get("vectorstore"):
                click.echo(f"⚠️ Warning: '{vectorstore}' does not contain a vectorstore configuration")
        
        # Create unified profile
        from datetime import datetime
        profile_config = {
            "profile": {
                "name": name,
                "description": description or f"{name} profile",
                "created_at": datetime.now().strftime("%Y-%m-%d")
            },
            "adapter": adapter_config.get("adapter", {}),
            "ingester": ingester_config.get("ingester", {})
        }
        
        # Add optional configurations
        if chunker_config.get("chunker"):
            profile_config["chunker"] = chunker_config.get("chunker", {})
        if embedding_config.get("embedding"):
            profile_config["embedding"] = embedding_config.get("embedding", {})
        if vectorstore_config.get("vectorstore"):
            profile_config["vectorstore"] = vectorstore_config.get("vectorstore", {})
        
        # Add logging from base config
        profile_config["logging"] = base_config.get("logging", {})
        
        # Write profile to yaml file
        with open(profile_path, 'w') as f:
            yaml.dump(profile_config, f, default_flow_style=False, sort_keys=False)
        
        click.echo(f"✅ Created profile: {name} at {profile_path}")
        
    except Exception as e:
        logger.error(f"Failed to create profile {name}: {str(e)}", exc_info=True)
        click.echo(f"❌ Error creating profile: {str(e)}", err=True)


@config.command()
@click.option('--config-dir', type=click.Path(exists=True, file_okay=False),
              help='Configuration directory')
@click.pass_context
def list(ctx, config_dir):
    """List available configuration profiles.
    
    Examples:
        pulsepipe config list
    """
    if not config_dir:
        config_dir = "config"
        if not os.path.exists(config_dir):
            config_dir = "src/pulsepipe/config"
    
    try:
        profiles = []
        for filename in os.listdir(config_dir):
            if filename.endswith(".yaml") and not filename.startswith("_"):
                # Check if file contains a profile key at root level
                profile_path = os.path.join(config_dir, filename)
                try:
                    with open(profile_path, 'r') as f:
                        config_data = yaml.safe_load(f)
                        # Only consider files that have a 'profile' key at root level
                        if isinstance(config_data, dict) and "profile" in config_data:
                            profile_name = filename.replace(".yaml", "")
                            # Extract description from profile if available
                            profile_info = config_data.get("profile", {})
                            description = profile_info.get("description", "")
                            
                            # Check for adapter and ingester (required components)
                            has_adapter = "adapter" in config_data
                            has_ingester = "ingester" in config_data
                            
                            # Check for optional components
                            has_chunker = "chunker" in config_data
                            has_embedding = "embedding" in config_data
                            has_vectorstore = "vectorstore" in config_data
                            
                            # Create components indicator
                            components = []
                            if has_adapter:
                                components.append("adapter")
                            if has_ingester:
                                components.append("ingester")
                            if has_chunker:
                                components.append("chunker")
                            if has_embedding:
                                components.append("embedding")
                            if has_vectorstore:
                                components.append("vectorstore")
                            
                            components_str = ", ".join(components)
                            
                            # Only add if it has at least adapter and ingester
                            if has_adapter and has_ingester:
                                profiles.append((profile_name, description, components_str))
                except Exception:
                    # Skip invalid profiles
                    pass
        
        if profiles:
            click.echo("Available profiles:")
            for name, desc, components in sorted(profiles):
                click.echo(f"  • {name}: {desc}")
                click.echo(f"    Components: {components}")
        else:
            click.echo("No profiles found. Use 'pulsepipe config create-profile' to create one.")
            
    except Exception as e:
        click.echo(f"❌ Error listing profiles: {str(e)}", err=True)


@config.group()
def filewatcher():
    """🗂️  File Watcher bookmark and file management.
    
    Manage file watcher features like bookmark cache and file management
    """
    pass

@filewatcher.command("list")
@click.option("--config-path", default="pulsepipe.yaml", help="Path to the configuration file.")
@click.option("--profile", default=None, help="Optional config profile to load.")
def list_processed_files(config_path, profile):
    """📋 List all processed files (successes and errors)."""
    if profile:
        config_dir = os.path.dirname(config_path)
        profile_path = os.path.join(config_dir, f"{profile}.yaml")
        config = load_config(profile_path)
    else:
        config = load_config(config_path)

    sqlite_conn = get_shared_sqlite_connection(config)
    store = SQLiteBookmarkStore(sqlite_conn)
    bookmarks = store.get_all()
    if not bookmarks:
        click.echo("📭 No processed files found.")
    else:
        click.echo("📌 Processed Files:")
        for path in bookmarks:
            click.echo(f" - {path}")

@filewatcher.command("reset")
@click.option("--config-path", default="pulsepipe.yaml", help="Path to the configuration file.")
@click.option("--profile", default=None, help="Optional config profile to load.")
def reset_bookmarks(config_path, profile):
    """🧹 Reset (clear) the bookmark cache."""
    if profile:
        config_dir = os.path.dirname(config_path)
        profile_path = os.path.join(config_dir, f"{profile}.yaml")
        config = load_config(profile_path)
    else:
        config = load_config(config_path)

    sqlite_conn = get_shared_sqlite_connection(config)
    store = SQLiteBookmarkStore(sqlite_conn)
    count = store.clear_all()
    click.echo(f"✅ Cleared {count} bookmarks.")

@filewatcher.command("archive")
@click.option("--config-path", default="pulsepipe.yaml", help="Path to the configuration file.")
@click.option("--profile", default=None, help="Optional config profile to load.")
@click.option("--archive-dir", required=True, help="Destination directory for archived files.")
def archive_files(config_path, profile, archive_dir):
    """📦 Move processed files to an archive directory."""
    if profile:
        config_dir = os.path.dirname(config_path)
        profile_path = os.path.join(config_dir, f"{profile}.yaml")
        config = load_config(profile_path)
    else:
        config = load_config(config_path)

    sqlite_conn = get_shared_sqlite_connection(config)
    store = SQLiteBookmarkStore(sqlite_conn)
    bookmarks = store.get_all()
    os.makedirs(archive_dir, exist_ok=True)
    moved = 0
    for path in bookmarks:
        try:
            dest = Path(archive_dir) / Path(path).name
            shutil.move(path, dest)
            moved += 1
            click.echo(f"📦 Archived: {path} → {dest}")
        except Exception as e:
            click.echo(f"❌ Failed to archive {path}: {e}")
    click.echo(f"✅ Archived {moved} files.")

@filewatcher.command("delete")
@click.option("--config-path", default="pulsepipe.yaml", help="Path to the configuration file.")
@click.option("--profile", default=None, help="Optional config profile to load.")
def delete_files(config_path, profile):
    """🗑️ Delete processed files from disk."""
    if profile:
        config_dir = os.path.dirname(config_path)
        profile_path = os.path.join(config_dir, f"{profile}.yaml")
        config = load_config(profile_path)
    else:
        config = load_config(config_path)

    sqlite_conn = get_shared_sqlite_connection(config)
    store = SQLiteBookmarkStore(sqlite_conn)
    bookmarks = store.get_all()
    deleted = 0
    for path in bookmarks:
        try:
            Path(path).unlink()
            deleted += 1
            click.echo(f"🗑️ Deleted: {path}")
        except Exception as e:
            click.echo(f"❌ Failed to delete {path}: {e}")
    click.echo(f"✅ Deleted {deleted} files.")

@config.command()
@click.option('--name', '-n', type=str, required=True,
              help='Profile name to delete')
@click.option('--force', '-f', is_flag=True,
              help='Delete without confirmation')
def delete_profile(name, force):
    """Delete a configuration profile.
    
    Examples:
        pulsepipe config delete-profile --name old_profile
        pulsepipe config delete-profile --name unused_profile --force
    """
    logger = LogFactory.get_logger("config.delete_profile")
    
    # Search for the profile in known locations
    # Define possible locations in priority order
    possible_locations = [
        # 1. Check in ./config/ next to the binary
        os.path.join("config", f"{name}.yaml"),
        # 2. Check in the current directory
        f"{name}.yaml",
        # 3. Check in the location relative to the script directory (for development)
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "config", f"{name}.yaml"),
        # 4. As a fallback, try the src path
        os.path.join("src", "pulsepipe", "config", f"{name}.yaml"),
    ]
    
    # Find the profile file
    profile_path = None
    for location in possible_locations:
        if os.path.exists(location):
            profile_path = location
            break
    
    if not profile_path:
        click.echo(f"❌ Profile not found: {name}", err=True)
        return
    
    # Confirm deletion unless force flag is used
    if not force:
        confirm = click.confirm(f"Are you sure you want to delete profile '{name}' at {profile_path}?")
        if not confirm:
            click.echo("Operation cancelled.")
            return
    
    try:
        # Delete the profile file
        os.remove(profile_path)
        click.echo(f"✅ Deleted profile: {name} from {profile_path}")
        
    except Exception as e:
        logger.error(f"Failed to delete profile {name}: {str(e)}", exc_info=True)
        click.echo(f"❌ Error deleting profile: {str(e)}", err=True)
