"""
Example: OAuth2 Authentication with Pygen v2

This example demonstrates how to use OAuth2 client credentials
authentication with the Pygen v2 client.
"""

from cognite.pygen._client.auth import OAuth2ClientCredentials, OAuth2Error

# Example 1: Basic usage with Azure AD
print("Example 1: Basic OAuth2 Authentication")
print("=" * 50)

credentials = OAuth2ClientCredentials(
    token_url="https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/v2.0/token",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    scopes=["https://api.cognitedata.com/.default"],
)

try:
    # Get authentication headers
    headers = credentials.get_headers()
    print("✓ Successfully authenticated!")
    print(f"  Authorization header: {headers['Authorization'][:50]}...")

    # The token is now cached and will be reused
    headers2 = credentials.get_headers()
    print("✓ Token was cached and reused")

finally:
    # Clean up resources
    credentials.close()

print()

# Example 2: Using context manager (recommended)
print("Example 2: Using Context Manager")
print("=" * 50)

with OAuth2ClientCredentials(
    token_url="https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/v2.0/token",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    scopes=["https://api.cognitedata.com/.default"],
    refresh_margin=600,  # Refresh 10 minutes before expiry
) as credentials:
    try:
        headers = credentials.get_headers()
        print("✓ Successfully authenticated with context manager!")
        print("  Token will auto-refresh 10 minutes before expiry")
    except OAuth2Error as e:
        print(f"✗ Authentication failed: {e}")

print()

# Example 3: Multiple scopes
print("Example 3: Multiple Scopes")
print("=" * 50)

credentials = OAuth2ClientCredentials(
    token_url="https://auth.example.com/oauth2/token",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    scopes=[
        "https://api.cognitedata.com/.default",
        "offline_access",
    ],
)

print(f"✓ Credentials configured with {len(credentials.scopes)} scopes")
credentials.close()

print()

# Example 4: Error handling
print("Example 4: Error Handling")
print("=" * 50)

credentials = OAuth2ClientCredentials(
    token_url="https://invalid.example.com/oauth2/token",
    client_id="invalid",
    client_secret="invalid",
)

try:
    headers = credentials.get_headers()
except OAuth2Error as e:
    print("✓ OAuth2Error caught successfully:")
    print(f"  {type(e).__name__}: {e}")
finally:
    credentials.close()

print()

# Example 5: Custom configuration
print("Example 5: Custom Configuration")
print("=" * 50)

credentials = OAuth2ClientCredentials(
    token_url="https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/v2.0/token",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    scopes=["https://api.cognitedata.com/.default"],
    audience="https://api.cognitedata.com",  # Optional audience parameter
    refresh_margin=300,  # Refresh 5 minutes before expiry (default)
)

print(f"✓ Token URL: {credentials.token_url}")
print(f"✓ Client ID: {credentials.client_id}")
print(f"✓ Scopes: {credentials.scopes}")
print(f"✓ Audience: {credentials.audience}")
print(f"✓ Refresh margin: {credentials.refresh_margin}s")

credentials.close()

print()
print("All examples completed!")
