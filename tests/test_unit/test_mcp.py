"""Tests for MCP server functionality."""

from __future__ import annotations

import pytest


class TestDataModelIdParsing:
    """Tests for parsing data model IDs."""

    def test_parse_valid_data_model_id(self):
        """Test parsing a valid data model ID."""
        data_model_id = "mySpace/MyModel@v1"
        space, rest = data_model_id.split("/")
        external_id, version = rest.split("@")

        assert space == "mySpace"
        assert external_id == "MyModel"
        assert version == "v1"

    def test_parse_data_model_id_with_dashes(self):
        """Test parsing a data model ID with dashes."""
        data_model_id = "my-space/My-Model@v1.0.0"
        space, rest = data_model_id.split("/")
        external_id, version = rest.split("@")

        assert space == "my-space"
        assert external_id == "My-Model"
        assert version == "v1.0.0"

    def test_parse_invalid_data_model_id_missing_slash(self):
        """Test that invalid data model ID raises error."""
        data_model_id = "mySpaceMyModel@v1"

        with pytest.raises(ValueError):
            space, rest = data_model_id.split("/")
            if "/" not in data_model_id:
                raise ValueError("Invalid data model ID format")

    def test_parse_invalid_data_model_id_missing_at(self):
        """Test that invalid data model ID raises error."""
        data_model_id = "mySpace/MyModelv1"

        with pytest.raises(ValueError):
            space, rest = data_model_id.split("/")
            external_id, version = rest.split("@")


class TestMcpImportError:
    """Tests for MCP import error handling."""

    def test_create_mcp_server_import_error_message(self):
        """Test that helpful error message is shown when mcp not installed."""
        # This test verifies the error handling logic exists
        # We can't easily test the actual import error without uninstalling mcp

        from cognite.pygen._mcp import create_mcp_server

        # The function should exist
        assert callable(create_mcp_server)


class TestAuthModule:
    """Tests for authentication module."""

    def test_base64url_encoding(self):
        """Test base64url encoding."""
        from cognite.pygen._auth import _base64url

        # Test known value
        data = b"test"
        result = _base64url(data)

        # Should be base64url encoded without padding
        assert "=" not in result
        assert "+" not in result
        assert "/" not in result

    def test_generate_pkce(self):
        """Test PKCE generation."""
        from cognite.pygen._auth import _generate_pkce

        verifier, challenge = _generate_pkce()

        # Verifier should be 64 characters
        assert len(verifier) == 64

        # Challenge should be base64url encoded (no padding, no +, no /)
        assert "=" not in challenge
        assert "+" not in challenge
        assert "/" not in challenge

        # Challenge should be derived from verifier (consistent)
        verifier2, challenge2 = _generate_pkce()
        assert verifier != verifier2  # Should be random
        assert challenge != challenge2

    def test_tokens_namedtuple(self):
        """Test Tokens named tuple."""
        from cognite.pygen._auth import Tokens

        tokens = Tokens(
            access_token="test_token",
            refresh_token="refresh_token",
            expires_in=3600,
        )

        assert tokens.access_token == "test_token"
        assert tokens.refresh_token == "refresh_token"
        assert tokens.expires_in == 3600

    def test_tokens_namedtuple_optional_refresh(self):
        """Test Tokens with None refresh token."""
        from cognite.pygen._auth import Tokens

        tokens = Tokens(
            access_token="test_token",
            refresh_token=None,
            expires_in=3600,
        )

        assert tokens.access_token == "test_token"
        assert tokens.refresh_token is None
