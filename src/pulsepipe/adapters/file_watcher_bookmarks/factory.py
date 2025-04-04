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
# ------------------------------------------------------------------------------
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

from .sqlite_store import SQLiteBookmarkStore

def create_bookmark_store(config: dict):
    store_type = config.get("type", "sqlite")

    if store_type == "sqlite":
        db_path = config.get("db_path", "bookmarks.db")
        return SQLiteBookmarkStore(db_path)

    elif store_type == "postgres":
        raise NotImplementedError(
            "🔒 PostgreSQL bookmark store is only available in PulsePilot Enterprise."
            "\n👉 Learn more at https://pulsepipe.io/pilot"
        )

    elif store_type == "redis":
        raise NotImplementedError(
            "🔒 Redis-based bookmark tracking is available in PulsePilot Pro."
            "\n👉 Upgrade at https://pulsepipe.io/pilot"
        )

    elif store_type == "s3":
        raise NotImplementedError(
            "🔒 S3 + DynamoDB scalable bookmark store is available in PulsePilot Enterprise."
            "\n👉 Get enterprise ingestion at https://pulsepipe.io/pilot"
        )

    else:
        raise ValueError(f"❌ Unsupported bookmark store type: {store_type}")
