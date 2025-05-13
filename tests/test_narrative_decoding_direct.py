import unittest
import base64
import binascii

from src.pulsepipe.utils.narrative_decoder import decode_narrative

class TestNarrativeDecodingDirect(unittest.TestCase):
    """Test direct decoding of narratives to verify the functionality works"""
    
    def test_decode_base64_narrative(self):
        """Test decoding base64 encoded narrative text"""
        # Sample text
        original_text = "This is clinical information that needs to be decoded"
        
        # Base64 encode it
        encoded_text = base64.b64encode(original_text.encode('utf-8')).decode('utf-8')
        
        # Decode using our utility
        decoded_text = decode_narrative(encoded_text)
        
        # Verify
        self.assertEqual(decoded_text, original_text)
    
    def test_decode_hex_narrative(self):
        """Test decoding hex encoded narrative text"""
        # Sample text
        original_text = "This is clinical information in hex format"
        
        # Hex encode it
        encoded_text = original_text.encode('utf-8').hex()
        
        # Decode using our utility
        decoded_text = decode_narrative(encoded_text)
        
        # Verify
        self.assertEqual(decoded_text, original_text)
    
    def test_decode_html_narrative(self):
        """Test HTML cleaning in narratives"""
        html_text = "<div><p>Patient has <b>acute sinusitis</b> with <i>fever</i>.</p></div>"
        expected_text = "Patient has acute sinusitis with fever."
        
        # Process with our decoder
        processed_text = decode_narrative(html_text)
        
        # Verify HTML is cleaned
        self.assertEqual(processed_text, expected_text)
    
    def test_decode_base64_html_narrative(self):
        """Test decoding base64 encoded HTML narrative"""
        # Original HTML
        original_html = "<div><p>Patient has <b>pneumonia</b> in the <i>right lung</i>.</p></div>"
        expected_text = "Patient has pneumonia in the right lung."
        
        # Base64 encode it
        encoded_html = base64.b64encode(original_html.encode('utf-8')).decode('utf-8')
        
        # Decode using our utility
        decoded_text = decode_narrative(encoded_html)
        
        # Verify
        self.assertEqual(decoded_text, expected_text)

if __name__ == '__main__':
    unittest.main()