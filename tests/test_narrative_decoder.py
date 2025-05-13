import unittest
import base64
import binascii
from src.pulsepipe.utils.narrative_decoder import (
    is_base64, is_hex, clean_html, decode_narrative
)

class TestNarrativeDecoder(unittest.TestCase):
    def test_is_base64(self):
        # Valid base64 strings
        self.assertTrue(is_base64("SGVsbG8gV29ybGQ="))  # "Hello World"
        self.assertTrue(is_base64("VGhpcyBpcyBhIHRlc3Q="))  # "This is a test"
        
        # Invalid base64 strings
        self.assertFalse(is_base64("Not base64!"))
        self.assertFalse(is_base64("123"))
        self.assertFalse(is_base64(""))
        self.assertFalse(is_base64(None))
    
    def test_is_hex(self):
        # Valid hex strings
        self.assertTrue(is_hex("48656c6c6f20576f726c64"))  # "Hello World"
        self.assertTrue(is_hex("54657374"))  # "Test"
        
        # Invalid hex strings
        self.assertFalse(is_hex("Not hex!"))
        self.assertFalse(is_hex("123G"))  # 'G' isn't valid hex
        self.assertFalse(is_hex(""))
        self.assertFalse(is_hex(None))
        self.assertFalse(is_hex("123"))  # Odd length
    
    def test_clean_html(self):
        html = "<div>This is <b>formatted</b> text with <i>styles</i>.</div>"
        expected = "This is formatted text with styles."
        self.assertEqual(clean_html(html), expected)
        
        # Test with empty inputs
        self.assertEqual(clean_html(""), "")
        self.assertEqual(clean_html(None), "")
    
    def test_decode_narrative_base64(self):
        # Base64 encoded "This is clinical information"
        base64_text = "VGhpcyBpcyBjbGluaWNhbCBpbmZvcm1hdGlvbg=="
        expected = "This is clinical information"
        result = decode_narrative(base64_text)
        self.assertEqual(result, expected)
    
    def test_decode_narrative_hex(self):
        # Create a test string
        test_string = "Patient note"
        # Convert to hex
        hex_text = test_string.encode('utf-8').hex()

        # Test our utility's ability to handle manually provided hex
        result = decode_narrative(hex_text, sanitize_html=False)

        # For this test, we'll directly call the binascii function
        # since our automatic detection might need tuning
        manual_result = binascii.unhexlify(hex_text).decode('utf-8')

        # Verify manual decoding works
        self.assertEqual(manual_result, test_string)
        # Verify our function matches manual result
        self.assertEqual(result, manual_result)
    
    def test_decode_narrative_html(self):
        # HTML content
        html = "<div>Patient has <b>fever</b> and <i>cough</i>.</div>"
        expected = "Patient has fever and cough."
        result = decode_narrative(html)
        self.assertEqual(result, expected)
    
    def test_decode_narrative_with_html_in_base64(self):
        # Base64 encoded HTML
        html = "<div>Patient has <b>fever</b> and <i>cough</i>.</div>"
        base64_html = base64.b64encode(html.encode('utf-8')).decode('utf-8')
        expected = "Patient has fever and cough."
        result = decode_narrative(base64_html)
        self.assertEqual(result, expected)
    
    def test_decode_narrative_plaintext(self):
        # Plain text that shouldn't be modified
        plaintext = "Regular patient notes without encoding"
        result = decode_narrative(plaintext)
        self.assertEqual(result, plaintext)
    
    def test_decode_narrative_empty(self):
        # Empty or None values
        self.assertIsNone(decode_narrative(""))
        self.assertIsNone(decode_narrative(None))
    
    def test_decode_narrative_disable_html_sanitization(self):
        html = "<div>HTML <b>content</b></div>"
        result = decode_narrative(html, sanitize_html=False)
        self.assertEqual(result, html)

if __name__ == '__main__':
    unittest.main()