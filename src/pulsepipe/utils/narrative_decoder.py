"""
Utility functions for decoding narrative content that might be encoded in
base64, hex, or contain HTML that needs to be sanitized.
"""

import base64
import binascii
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def is_base64(text: str) -> bool:
    """
    Check if a string is likely base64 encoded.
    
    Args:
        text: String to check
        
    Returns:
        bool: True if the string appears to be base64 encoded
    """
    if not text:
        return False
        
    # Base64 strings are typically multiples of 4 characters
    # and only contain a specific character set
    if len(text) % 4 != 0:
        return False
    
    # Base64 pattern: A-Z, a-z, 0-9, +, /, and = for padding
    pattern = r'^[A-Za-z0-9+/]+={0,2}$'
    return bool(re.match(pattern, text))

def is_hex(text: str) -> bool:
    """
    Check if a string is likely hex encoded.

    Args:
        text: String to check

    Returns:
        bool: True if the string appears to be hex encoded
    """
    if not text:
        return False

    # Hex strings typically have an even number of characters
    # and only contain hexadecimal characters
    pattern = r'^[A-Fa-f0-9]+$'
    is_hex_pattern = bool(re.match(pattern, text)) and len(text) % 2 == 0

    # Additional check - try to decode it and see if it succeeds
    if is_hex_pattern:
        try:
            # Try to decode and get ASCII text (printable characters)
            decoded = binascii.unhexlify(text).decode('utf-8')
            # If we get mostly printable ASCII, it's likely real hex-encoded content
            printable_chars = sum(c.isprintable() for c in decoded)
            return printable_chars / len(decoded) > 0.8
        except (binascii.Error, UnicodeDecodeError):
            return False

    return False

def clean_html(html_content: str) -> str:
    """
    Remove HTML tags from content, leaving only the text.

    Args:
        html_content: HTML string to clean

    Returns:
        str: Plain text with HTML tags removed
    """
    if not html_content:
        return ""

    # Simple HTML tag removal - for complex HTML parsing,
    # consider using a library like BeautifulSoup
    clean_text = re.sub(r'<[^>]+>', '', html_content)
    # Normalize whitespace
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text

def decode_narrative(content: Optional[str], sanitize_html: bool = True) -> Optional[str]:
    """
    Attempt to detect and decode encoded narrative content.

    Args:
        content: Potentially encoded content
        sanitize_html: Whether to remove HTML tags from the result

    Returns:
        str: Decoded content or original content if no encoding detected
    """
    if not content:
        return None

    result = content
    decoded = None

    # Try to detect and decode base64
    if is_base64(content):
        try:
            decoded = base64.b64decode(content).decode('utf-8')
            logger.debug("Successfully decoded base64 content")
            result = decoded
        except (binascii.Error, UnicodeDecodeError) as e:
            logger.debug(f"Failed to decode potential base64 content: {e}")

    # Try to detect and decode hex
    elif is_hex(content):
        try:
            decoded = binascii.unhexlify(content).decode('utf-8')
            logger.debug("Successfully decoded hex content")
            result = decoded
        except (binascii.Error, UnicodeDecodeError) as e:
            logger.debug(f"Failed to decode potential hex content: {e}")

    # If detection failed, try a more direct approach for hex
    # This helps in cases where our detection heuristic might not catch valid hex
    if not decoded and len(content) % 2 == 0 and all(c in '0123456789ABCDEFabcdef' for c in content):
        try:
            decoded = binascii.unhexlify(content).decode('utf-8')
            # If we get mostly printable characters, it's probably valid hex
            if sum(c.isprintable() for c in decoded) / len(decoded) > 0.8:
                logger.debug("Successfully decoded hex content (fallback method)")
                result = decoded
        except (binascii.Error, UnicodeDecodeError):
            pass

    # Clean HTML if requested
    if sanitize_html and result and ('<' in result and '>' in result):
        result = clean_html(result)

    return result