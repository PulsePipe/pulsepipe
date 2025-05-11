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

# src/pulsepipe/models/document_reference.py

from typing import Optional, List
from pydantic import BaseModel


class DocumentAuthor(BaseModel):
    """
    Represents an author of a clinical document.
    
    Documents in healthcare often have multiple authors or contributors
    who play different roles in the document's creation. This model captures
    information about each author, including their identity and role.
    """
    author_id: Optional[str]
    author_name: Optional[str]
    author_role: Optional[str]


class DocumentReference(BaseModel):
    """
    Represents a reference to a clinical document in a healthcare system.
    
    Document references point to clinical documents of various types that
    are part of a patient's medical record, such as discharge summaries,
    clinical notes, imaging reports, or scanned documents. This model
    captures metadata about the document while allowing the actual content
    to be stored separately.
    
    Documents may be structured (like CDA) or unstructured (plain text or PDF).
    """
    document_id: Optional[str]
    title: Optional[str]
    document_type: Optional[str]        # E.g., "Discharge Summary", "Consult Note"
    document_class: Optional[str]       # E.g., "Clinical Note", "Legal Document"
    status: Optional[str]               # E.g., "current", "superseded"
    format: Optional[str]               # MIME type or format code
    creation_date: Optional[str]
    last_modified_date: Optional[str]
    security_label: Optional[str]       # E.g., "restricted", "normal"
    content_type: Optional[str]         # MIME type of the content
    content: Optional[str]              # Actual document content if available
    content_url: Optional[str]          # URL reference to content if not inline
    authors: List[DocumentAuthor] = []
    related_encounters: List[str] = []
    patient_id: Optional[str]
    facility: Optional[str]
    department: Optional[str]