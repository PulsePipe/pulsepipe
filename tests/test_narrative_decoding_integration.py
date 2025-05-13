import unittest
import base64
import binascii
from unittest.mock import MagicMock, patch

from src.pulsepipe.utils.narrative_decoder import decode_narrative
from src.pulsepipe.ingesters.fhir_utils.document_reference_mapper import DocumentReferenceMapper
from src.pulsepipe.models.document_reference import DocumentReference

class TestNarrativeDecodingIntegration(unittest.TestCase):
    """
    Test narrative decoding directly in the mappers without depending on
    full content models which have strict validation requirements.
    """
        
    def test_document_reference_mapper_decodes_base64(self):
        """Test base64 decoding in DocumentReference mapper directly"""
        # Plain text content to encode
        plain_content = "This is test clinical content for the patient."
        encoded_content = base64.b64encode(plain_content.encode('utf-8')).decode('utf-8')

        # Create a FHIR attachment with encoded data
        attachment = {
            "contentType": "text/plain",
            "data": encoded_content
        }

        # Use the decode_narrative function directly within the mapper
        mapper = DocumentReferenceMapper()

        # Create a patched content_text to hold the result
        content_text = mapper._decode_attachment_content(attachment)

        # Verify the content was decoded
        self.assertEqual(content_text, plain_content)

    def test_document_reference_mapper_decodes_hex(self):
        """Test hex decoding in DocumentReference mapper directly"""
        # Plain text content to encode
        plain_content = "This is test clinical content for the patient."
        encoded_content = plain_content.encode('utf-8').hex()

        # Create a FHIR attachment with encoded data
        attachment = {
            "contentType": "text/plain",
            "data": encoded_content
        }

        # Use the decode_narrative function directly within the mapper
        mapper = DocumentReferenceMapper()

        # Create a patched content_text to hold the result
        content_text = mapper._decode_attachment_content(attachment)

        # Verify the content was decoded
        self.assertEqual(content_text, plain_content)
        
    def test_html_narrative_decoding(self):
        """Test direct decoding of HTML narrative"""
        # Create an HTML narrative
        html = "<div xmlns='http://www.w3.org/1999/xhtml'><p>Patient has <b>mild infiltrates</b> in the <i>right lower lobe</i>.</p></div>"

        # Decode directly
        decoded = decode_narrative(html)

        # Verify HTML tags were cleaned
        self.assertNotIn("<b>", decoded, "HTML tags were not cleaned")
        self.assertNotIn("<i>", decoded, "HTML tags were not cleaned")
        self.assertIn("mild infiltrates", decoded, "Content was lost in cleaning")
        self.assertIn("right lower lobe", decoded, "Content was lost in cleaning")

if __name__ == '__main__':
    unittest.main()