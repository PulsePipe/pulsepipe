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

# src/pulsepipe/pipelines/chunkers/narrative_chunker.py

import re
from typing import List, Dict, Any, Union
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.note import Note

class NarrativeChunker:
    """
    Chunker specialized for clinical narratives and notes.
    
    NarrativeChunker splits clinical narratives into semantically meaningful chunks
    based on clinical section headers, paragraph breaks, and sentence boundaries.
    It is designed to produce chunks that preserve clinical context for embedding
    and retrieval.
    
    Features:
    - Recognition of common clinical note sections (HPI, PMH, ROS, etc.)
    - Intelligent splitting at section and subsection boundaries
    - Handling of bullet points and numbered lists
    - Preservation of semantic units (paragraphs as minimal units)
    - Optional overlap between chunks for continuity
    """
    
    def __init__(self, 
                max_chunk_size: int = 512,
                min_chunk_size: int = 50,
                overlap_size: int = 50,
                include_metadata: bool = True,
                section_detection: bool = True):
        """
        Initialize a NarrativeChunker.
        
        Args:
            max_chunk_size: Maximum size of a chunk in characters
            min_chunk_size: Minimum size for a chunk to be valid
            overlap_size: Number of characters to overlap between chunks
            include_metadata: Whether to include metadata in output chunks
            section_detection: Whether to detect and use clinical sections
        """
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing NarrativeChunker")
        
        self.max_chunk_size = max_chunk_size
        self.min_chunk_size = min_chunk_size
        self.overlap_size = overlap_size
        self.include_metadata = include_metadata
        self.section_detection = section_detection
        
        # Common clinical note section headers
        self.section_headers = [
            # Generic headers
            r"^(ASSESSMENT|PLAN|IMPRESSION|DIAGNOSIS|RECOMMENDATION|DISCUSSION)",
            r"^(HISTORY|SUBJECTIVE|OBJECTIVE|PHYSICAL EXAM|LABORATORY|IMAGING)",
            
            # History sections
            r"^(HISTORY OF PRESENT ILLNESS|HPI|CHIEF COMPLAINT|CC)",
            r"^(PAST MEDICAL HISTORY|PMH|PAST SURGICAL HISTORY|PSH)",
            r"^(FAMILY HISTORY|FH|SOCIAL HISTORY|SH)",
            
            # Medication and allergy sections
            r"^(MEDICATIONS|CURRENT MEDICATIONS|MEDS|MEDICATION LIST)",
            r"^(ALLERGIES|DRUG ALLERGIES|MEDICATION ALLERGIES)",
            
            # Physical exam sections
            r"^(VITAL SIGNS|VS|GENERAL APPEARANCE|HEENT)",
            r"^(CARDIOVASCULAR|CV|RESPIRATORY|RESP|PULMONARY|NEURO)",
            r"^(GASTROINTESTINAL|GI|GENITOURINARY|GU|MUSCULOSKELETAL|MSK)",
            
            # Diagnostic sections
            r"^(LABORATORY RESULTS|LABS|RADIOLOGY|DIAGNOSTIC STUDIES)",
            r"^(PATHOLOGY|MICROBIOLOGY|SURGICAL PATHOLOGY)",
            
            # Other common sections
            r"^(REVIEW OF SYSTEMS|ROS|FOLLOW UP|DISPOSITION)",
            r"^(PROCEDURE|TECHNIQUE|FINDINGS|COMPLICATIONS)",
        ]
        
        # Combine section headers into a single regex pattern
        self.section_pattern = re.compile(
            "|".join(self.section_headers),
            re.IGNORECASE | re.MULTILINE
        )
        
        # Pattern for paragraph breaks (blank lines)
        self.paragraph_pattern = re.compile(r"\n\s*\n")
        
        # Pattern for bullet points and numbered lists
        self.list_item_pattern = re.compile(r"^\s*[-*•]|\d+[.)]\s+", re.MULTILINE)
        
        # Pattern for sentence boundaries (period followed by space or newline)
        # Be careful with medical abbreviations like "Dr." or "q.d."
        self.sentence_pattern = re.compile(r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s")
        
        # Common abbreviations to avoid incorrect sentence splits
        self.abbreviations = [
            r"Dr\.", r"Mr\.", r"Mrs\.", r"Ms\.", r"Ph\.D\.", r"M\.D\.", r"R\.N\.",
            r"q\.d\.", r"b\.i\.d\.", r"t\.i\.d\.", r"q\.i\.d\.", r"p\.r\.n\.",
            r"a\.m\.", r"p\.m\.", r"e\.g\.", r"i\.e\.", r"etc\.", r"vs\."
        ]
    
    def chunk(self, content: Union[PulseClinicalContent, str]) -> List[Dict[str, Any]]:
        """
        Split clinical content into narrative chunks.
        
        Args:
            content: Either a PulseClinicalContent object or a string
            
        Returns:
            List of chunks as dictionaries
        """
        if isinstance(content, PulseClinicalContent):
            return self._chunk_clinical_content(content)
        elif isinstance(content, str):
            return self._chunk_text(content)
        else:
            self.logger.warning(f"Unsupported content type for chunking: {type(content)}")
            return []
    
    def _chunk_clinical_content(self, content: PulseClinicalContent) -> List[Dict[str, Any]]:
        """
        Process a PulseClinicalContent object and chunk all narrative fields.
        
        Args:
            content: Clinical content with notes and narratives
            
        Returns:
            List of chunks as dictionaries
        """
        chunks = []
        
        # Process notes
        if content.notes:
            for note in content.notes:
                note_chunks = self._chunk_note(note, content)
                chunks.extend(note_chunks)
        
        # Process imaging reports (they often have narrative text)
        if content.imaging:
            for report in content.imaging:
                if report.narrative:
                    report_chunks = self._chunk_text(
                        report.narrative, 
                        content_type="imaging_report",
                        report_id=report.report_id,
                        patient_id=report.patient_id,
                        encounter_id=report.encounter_id
                    )
                    chunks.extend(report_chunks)
        
        # Process pathology reports
        if content.pathology:
            for report in content.pathology:
                if report.narrative:
                    report_chunks = self._chunk_text(
                        report.narrative,
                        content_type="pathology_report",
                        report_id=report.report_id,
                        patient_id=report.patient_id,
                        encounter_id=report.encounter_id
                    )
                    chunks.extend(report_chunks)
        
        self.logger.info(f"🧩 NarrativeChunker produced {len(chunks)} chunks from clinical content")
        return chunks
    
    def _chunk_note(self, note: Note, content: PulseClinicalContent) -> List[Dict[str, Any]]:
        """
        Chunk a clinical note into semantically meaningful pieces.
        
        Args:
            note: Clinical note to chunk
            content: Parent clinical content for metadata
            
        Returns:
            List of note chunks
        """
        if not note.text:
            return []
        
        # Get metadata from the note and parent content
        metadata = {
            "note_type": note.note_type_code,
            "timestamp": note.timestamp,
            "patient_id": note.patient_id or getattr(content.patient, "id", None) if content.patient else None,
            "encounter_id": note.encounter_id or getattr(content.encounter, "id", None) if content.encounter else None,
            "author_id": note.author_id,
            "author_name": note.author_name
        }
        
        # Chunk the note text
        return self._chunk_text(note.text, content_type="clinical_note", **metadata)
    
    def _chunk_text(self, text: str, content_type: str = "text", **metadata) -> List[Dict[str, Any]]:
        """
        Split text into semantically meaningful chunks.
        
        Args:
            text: Text to chunk
            content_type: Type of content being chunked
            **metadata: Additional metadata to include with chunks
            
        Returns:
            List of chunks as dictionaries
        """
        if not text:
            return []
        
        chunks = []
        
        # First try to split by sections if enabled
        if self.section_detection:
            sections = self._split_by_sections(text)
        else:
            sections = [text]
        
        # For each section, create chunks
        for i, section in enumerate(sections):
            # Skip empty sections
            if not section.strip():
                continue
                
            # Get section title if present (first line ending with colon)
            section_title = None
            section_lines = section.split('\n', 1)
            if len(section_lines) > 1 and ':' in section_lines[0]:
                section_title = section_lines[0].strip()
                section_content = section_lines[1]
            else:
                section_content = section
            
            # If section is small enough to be a single chunk
            if len(section_content) <= self.max_chunk_size:
                chunk = {
                    "type": content_type,
                    "content": section_content,
                    "section": section_title
                }
                
                if self.include_metadata:
                    chunk["metadata"] = metadata
                    
                chunks.append(chunk)
                continue
            
            # Otherwise, split further by paragraphs
            paragraphs = self._split_by_paragraphs(section_content)
            current_chunk = ""
            current_section = section_title
            
            for paragraph in paragraphs:
                # If adding this paragraph exceeds max size, finalize chunk
                if len(current_chunk) + len(paragraph) > self.max_chunk_size and len(current_chunk) > self.min_chunk_size:
                    chunk = {
                        "type": content_type,
                        "content": current_chunk,
                        "section": current_section
                    }
                    
                    if self.include_metadata:
                        chunk["metadata"] = metadata
                        
                    chunks.append(chunk)
                    
                    # Start a new chunk with overlap
                    if self.overlap_size > 0 and len(current_chunk) > self.overlap_size:
                        # Try to find a sentence boundary for the overlap
                        overlap_text = current_chunk[-self.overlap_size:]
                        sentence_boundaries = list(self.sentence_pattern.finditer(overlap_text))
                        
                        if sentence_boundaries:
                            # Use the last sentence boundary in the overlap
                            last_boundary = sentence_boundaries[-1]
                            overlap_index = last_boundary.end() + len(overlap_text) - self.overlap_size
                            current_chunk = current_chunk[-(self.overlap_size - overlap_index):]
                        else:
                            # No sentence boundary found, use the raw overlap
                            current_chunk = current_chunk[-self.overlap_size:]
                    else:
                        current_chunk = ""
                
                # Add paragraph to current chunk
                if current_chunk and not current_chunk.endswith('\n'):
                    current_chunk += '\n\n'
                current_chunk += paragraph
            
            # Add the last chunk if it's not empty
            if current_chunk and len(current_chunk) >= self.min_chunk_size:
                chunk = {
                    "type": content_type,
                    "content": current_chunk,
                    "section": current_section
                }
                
                if self.include_metadata:
                    chunk["metadata"] = metadata
                    
                chunks.append(chunk)
        
        self.logger.info(f"🧩 Created {len(chunks)} chunks from text of length {len(text)}")
        return chunks
    
    def _split_by_sections(self, text: str) -> List[str]:
        """
        Split text by clinical section headers.
        
        Args:
            text: Text to split
            
        Returns:
            List of sections
        """
        # Find all section headers
        section_matches = list(self.section_pattern.finditer(text))
        
        if not section_matches:
            return [text]
        
        sections = []
        last_pos = 0
        
        for match in section_matches:
            # Add text before this section as a section
            if match.start() > last_pos:
                section_text = text[last_pos:match.start()].strip()
                if section_text:
                    sections.append(section_text)
            
            # Find the end of this section (start of next section or end of text)
            next_match_index = section_matches.index(match) + 1
            if next_match_index < len(section_matches):
                next_match = section_matches[next_match_index]
                section_end = next_match.start()
            else:
                section_end = len(text)
            
            # Add this section with its header
            section_text = text[match.start():section_end].strip()
            if section_text:
                sections.append(section_text)
            
            last_pos = section_end
        
        # Add any remaining text after the last section
        if last_pos < len(text):
            section_text = text[last_pos:].strip()
            if section_text:
                sections.append(section_text)
        
        return sections
    
    def _split_by_paragraphs(self, text: str) -> List[str]:
        """
        Split text into paragraphs.
        
        Args:
            text: Text to split
            
        Returns:
            List of paragraphs
        """
        # Split by blank lines
        paragraphs = self.paragraph_pattern.split(text)
        
        # Process list items (keep bullet points with their content)
        result = []
        current_list = None
        
        for para in paragraphs:
            if not para.strip():
                continue
                
            # Check if this is a list item
            if self.list_item_pattern.search(para):
                if current_list is None:
                    current_list = para
                else:
                    current_list += "\n" + para
            else:
                # Not a list item
                if current_list is not None:
                    result.append(current_list)
                    current_list = None
                result.append(para)
        
        # Add the last list if there is one
        if current_list is not None:
            result.append(current_list)
        
        return result
