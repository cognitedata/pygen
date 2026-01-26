"""MCP server support for pygen-generated SDKs."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


def create_mcp_server(
    data_model_id: str,
    cluster: str,
    project: str,
    access_token: str,
    *,
    include_graphql: bool = False,
    include_write: bool = False,
) -> Any:
    """Create an MCP server for a CDF data model.

    Args:
        data_model_id: Data model identifier in format "space/externalId@version"
        cluster: CDF cluster (e.g., "api", "westeurope-1")
        project: CDF project name
        access_token: OAuth access token
        include_graphql: Whether to include the graphql_query tool (default: False)
        include_write: Whether to include upsert/delete tools (default: False)

    Returns:
        FastMCP server instance ready to run
    """
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        raise ImportError(
            "MCP support requires the 'mcp' package. Install with: pip install cognite-pygen[mcp]"
        ) from None

    # Parse data model ID
    space, rest = data_model_id.split("/")
    external_id, version = rest.split("@")

    # Create CogniteClient with token
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.credentials import Token

    config = ClientConfig(
        client_name="pygen-mcp",
        project=project,
        base_url=f"https://{cluster}.cognitedata.com",
        credentials=Token(access_token),
    )
    cognite_client = CogniteClient(config)

    # Generate SDK in memory
    from cognite.pygen import generate_sdk_notebook

    sdk_client = generate_sdk_notebook(
        (space, external_id, version),
        client=cognite_client,
    )

    # Create MCP server
    mcp = FastMCP(f"{external_id}_mcp")

    # Register tools for each API
    for attr_name in dir(sdk_client):
        if attr_name.startswith("_"):
            continue
        api = getattr(sdk_client, attr_name)
        if not hasattr(api, "list") or not hasattr(api, "_view_id"):
            continue

        # Create tools with captured api reference
        _make_list_tool(mcp, api, attr_name)
        _make_retrieve_tool(mcp, api, attr_name)

        # Write operations (optional)
        if include_write:
            _make_delete_tool(mcp, api, attr_name)

    # GraphQL tool (optional)
    if include_graphql:

        @mcp.tool(name="graphql_query")
        def graphql_query(query: str) -> str:
            """Execute a GraphQL query against the data model."""
            result = sdk_client.graphql_query(query)
            return json.dumps([r.model_dump(mode="json") for r in result], default=str)

    return mcp


def _make_list_tool(mcp: Any, api_ref: Any, name: str) -> None:
    """Create a list tool for an API."""

    @mcp.tool(name=f"{name}_list")
    def list_items(limit: int = 25) -> str:
        """List items from this view."""
        results = api_ref.list(limit=limit)
        return json.dumps([r.model_dump(mode="json") for r in results], default=str)


def _make_retrieve_tool(mcp: Any, api_ref: Any, name: str) -> None:
    """Create a retrieve tool for an API."""

    @mcp.tool(name=f"{name}_retrieve")
    def retrieve_item(external_id: str) -> str:
        """Retrieve an item by external_id."""
        result = api_ref.retrieve(external_id)
        if result is None:
            return json.dumps({"error": "Not found"})
        return json.dumps(result.model_dump(mode="json"), default=str)


def _make_delete_tool(mcp: Any, api_ref: Any, name: str) -> None:
    """Create a delete tool for an API."""

    @mcp.tool(name=f"{name}_delete")
    def delete_item(external_id: str) -> str:
        """Delete an item by external_id."""
        api_ref.delete(external_id)
        return json.dumps({"deleted": external_id})
