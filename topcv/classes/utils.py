import hashlib
import base64

def url_to_id_short(url, length=8):
    """Create a short ID from URL (customizable length)"""
    hash_bytes = hashlib.sha256(url.encode('utf-8')).digest()
    # Use base64 for shorter representation
    b64 = base64.urlsafe_b64encode(hash_bytes).decode('utf-8')
    return b64[:length].rstrip('=')