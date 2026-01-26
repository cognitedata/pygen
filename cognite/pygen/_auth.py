"""Interactive OAuth 2.0 authentication for Cognite Data Fusion."""
from __future__ import annotations

import base64
import hashlib
import http.server
import secrets
import ssl
import sys
import urllib.parse
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

import requests


def _log(message: str) -> None:
    """Log to stderr to avoid breaking MCP protocol on stdout."""
    print(message, file=sys.stderr)

# Cognite's public CLI client ID (no secret required)
CLIENT_ID = "c6f97d29-79a5-48ac-85de-1de8229226cb"
AUTHORITY = "https://auth.cognite.com"
REDIRECT_URI = "https://localhost:3000"
PORT = 3000
TIMEOUT = 300  # 5 minutes
CERT_DIR = Path.home() / ".cdf-login"


class Tokens(NamedTuple):
    """OAuth tokens returned from authentication."""

    access_token: str
    refresh_token: str | None
    expires_in: int


def _base64url(data: bytes) -> str:
    """Base64url encode without padding."""
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _generate_pkce() -> tuple[str, str]:
    """Generate PKCE code verifier and challenge."""
    verifier = secrets.token_urlsafe(64)[:64]
    challenge = _base64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge


def _get_or_create_certs() -> tuple[str, str]:
    """Generate or load self-signed certificate for HTTPS callback."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    CERT_DIR.mkdir(exist_ok=True)
    key_path = CERT_DIR / "localhost-key.pem"
    cert_path = CERT_DIR / "localhost-cert.pem"

    if key_path.exists() and cert_path.exists():
        return str(key_path), str(cert_path)

    # Generate private key
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Generate certificate
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .sign(key, hashes.SHA256())
    )

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    return str(key_path), str(cert_path)


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """Handle OAuth callback."""

    code: str | None = None
    error: str | None = None

    def do_GET(self) -> None:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

        if "error" in params:
            _CallbackHandler.error = params["error"][0]
            self._respond("Authentication failed. You can close this window.")
        elif "code" in params:
            if params.get("state", [None])[0] != self.server.expected_state:  # type: ignore[attr-defined]
                _CallbackHandler.error = "State mismatch - possible CSRF attack"
                self._respond("Security error. Please try again.")
            else:
                _CallbackHandler.code = params["code"][0]
                self._respond("Login successful! You can close this window.")

    def _respond(self, message: str, code: int = 200) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        html = f"<html><body style='font-family:system-ui;text-align:center;padding:50px'><h1>{message}</h1></body></html>"
        self.wfile.write(html.encode())

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # Suppress HTTP logs


def interactive_login(organization: str | None = None) -> Tokens:
    """
    Perform interactive OAuth login via browser.

    Args:
        organization: Optional organization hint for login

    Returns:
        Tokens containing access_token, refresh_token, and expires_in
    """
    _log("Starting login flow...")

    # Reset state
    _CallbackHandler.code = None
    _CallbackHandler.error = None

    # Get certs and PKCE
    key_path, cert_path = _get_or_create_certs()
    code_verifier, code_challenge = _generate_pkce()
    state = secrets.token_urlsafe(16)

    # Discover endpoints
    config = requests.get(f"{AUTHORITY}/.well-known/openid-configuration", timeout=30).json()

    # Build auth URL
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "openid profile email",
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    if organization:
        params["organization_hint"] = organization

    auth_url = f"{config['authorization_endpoint']}?{urllib.parse.urlencode(params)}"

    # Create HTTPS server
    server = http.server.HTTPServer(("localhost", PORT), _CallbackHandler)
    server.expected_state = state  # type: ignore[attr-defined]

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    server.socket = context.wrap_socket(server.socket, server_side=True)

    # Open browser
    _log("Opening browser for authentication...")
    webbrowser.open(auth_url)

    # Wait for callback
    _log("Waiting for login (timeout: 5 minutes)...")
    while _CallbackHandler.code is None and _CallbackHandler.error is None:
        server.handle_request()

    server.server_close()

    if _CallbackHandler.error:
        raise RuntimeError(f"Login failed: {_CallbackHandler.error}")

    # Exchange code for tokens
    _log("Exchanging code for token...")
    resp = requests.post(
        config["token_endpoint"],
        data={
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "code": _CallbackHandler.code,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": code_verifier,
        },
        timeout=30,
    )

    if not resp.ok:
        raise RuntimeError(f"Token exchange failed: {resp.text}")

    data = resp.json()
    _log("Login successful!")

    return Tokens(
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token"),
        expires_in=data.get("expires_in", 3600),
    )
