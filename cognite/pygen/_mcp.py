"""MCP server support for pygen-generated SDKs."""

from __future__ import annotations

import datetime
import inspect
import json
import types
from typing import Any, Union, get_args, get_origin

# MCP-compatible simple types that can be used as tool parameters
_MCP_SIMPLE_TYPES = (str, int, float, bool, type(None))

# Type marker for MCP tool parameters that need conversion
_DATETIME_MARKER = "datetime"  # ISO 8601 string -> datetime
_NODE_REF_MARKER = "node_ref"  # {"space": str, "externalId": str} or [...] -> tuple or list[tuple]


def _is_datetime_type(annotation: Any) -> bool:
    """Check if annotation is a datetime type."""
    if annotation is datetime.datetime:
        return True
    if isinstance(annotation, str) and "datetime" in annotation:
        return True
    origin = get_origin(annotation)
    if origin is types.UnionType or origin is Union:
        args = get_args(annotation)
        return any(_is_datetime_type(arg) for arg in args if arg is not type(None))
    return False


def _is_node_ref_type(annotation: Any) -> bool:
    """Check if annotation is a node reference type (NodeId, DirectRelationReference, tuple[str,str])."""
    ann_str = str(annotation)
    # Check for common node reference patterns
    if any(pattern in ann_str for pattern in ["NodeId", "DirectRelationReference", "tuple[str, str]"]):
        return True
    return False


def _is_mcp_compatible_type(annotation: Any) -> bool:
    """Check if a type annotation can be simplified to MCP-compatible types."""
    if annotation is inspect.Parameter.empty:
        return False

    # Handle string annotations (forward references)
    if isinstance(annotation, str):
        simple_names = ("str", "int", "float", "bool", "None", "datetime")
        parts = annotation.replace(" ", "").split("|")
        if all(
            p in simple_names
            or p.startswith("list[")
            or "datetime" in p
            or "NodeId" in p
            or "DirectRelationReference" in p
            or "tuple[str" in p
            for p in parts
        ):
            return True
        return False

    # Direct simple types
    if annotation in _MCP_SIMPLE_TYPES:
        return True

    # Datetime types
    if _is_datetime_type(annotation):
        return True

    # Node reference types
    if _is_node_ref_type(annotation):
        return True

    origin = get_origin(annotation)

    # Handle list[str] -> treat as str
    if origin is list:
        args = get_args(annotation)
        if args and args[0] in _MCP_SIMPLE_TYPES:
            return True
        return False

    # Handle Union types (PEP 604: X | Y returns types.UnionType, typing.Union returns Union)
    if origin is types.UnionType or origin is Union:
        args = get_args(annotation)
        if args:
            for arg in args:
                if arg in _MCP_SIMPLE_TYPES:
                    return True
                if _is_mcp_compatible_type(arg):
                    return True
        return False

    return False


def _get_mcp_type_info(annotation: Any) -> tuple[Any, str | None]:
    """Get MCP type and optional conversion marker.

    Returns:
        (python_type, marker) where marker is None, 'datetime', or 'node_ref'
    """
    # Check for datetime first
    if _is_datetime_type(annotation):
        return (str, _DATETIME_MARKER)

    # Check for node reference - use dict | list to accept both single refs and arrays
    if _is_node_ref_type(annotation):
        return (dict | list, _NODE_REF_MARKER)

    # Simple types
    if annotation in (str, int, float, bool):
        return (annotation, None)

    # Handle string annotations
    if isinstance(annotation, str):
        if "datetime" in annotation:
            return (str, _DATETIME_MARKER)
        if "NodeId" in annotation or "DirectRelationReference" in annotation or "tuple[str" in annotation:
            return (dict | list, _NODE_REF_MARKER)
        if "int" in annotation:
            return (int, None)
        if "float" in annotation:
            return (float, None)
        if "bool" in annotation:
            return (bool, None)
        return (str, None)

    origin = get_origin(annotation)

    # Handle list[X] -> X
    if origin is list:
        args = get_args(annotation)
        if args:
            return _get_mcp_type_info(args[0])
        return (str, None)

    # Handle Union types (PEP 604 and typing.Union)
    if origin is types.UnionType or origin is Union:
        args = get_args(annotation)
        for arg in args:
            if arg is not type(None):
                return _get_mcp_type_info(arg)

    return (str, None)  # Default to string


def _convert_node_ref(value: dict[str, Any]) -> tuple[str, str] | None:
    """Convert a single node ref dict to tuple."""
    space = value.get("space")
    external_id = value.get("externalId") or value.get("external_id")
    if space and external_id:
        return (space, external_id)
    return None


def _convert_mcp_value(value: Any, marker: str | None) -> Any:
    """Convert MCP input value to SDK-compatible value."""
    if value is None:
        return None

    if marker == _DATETIME_MARKER:
        # Parse ISO 8601 string to datetime
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value

    if marker == _NODE_REF_MARKER:
        # Handle array of node refs: [{"space": x, "externalId": y}, ...]
        if isinstance(value, list):
            converted = []
            for item in value:
                if isinstance(item, dict):
                    ref = _convert_node_ref(item)
                    if ref:
                        converted.append(ref)
            return converted if converted else None

        # Handle single node ref: {"space": x, "externalId": y}
        if isinstance(value, dict):
            return _convert_node_ref(value)
        return value

    return value


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
    """Create a list tool for an API with dynamically introspected filter parameters."""
    # Introspect the list method signature
    sig = inspect.signature(api_ref.list)

    # Collect MCP-compatible parameters: (name, type, default, marker)
    param_info: list[tuple[str, type, Any, str | None]] = []
    param_descriptions: list[str] = []

    for param_name, param in sig.parameters.items():
        if param_name == "self":
            continue

        # Skip complex types that MCP can't handle
        annotation = param.annotation
        if not _is_mcp_compatible_type(annotation) and param_name not in ("limit", "space"):
            continue

        # Get MCP type and conversion marker
        if param_name == "limit":
            simple_type, marker = int, None
        elif param_name == "space":
            simple_type, marker = str, None
        elif annotation is inspect.Parameter.empty:
            simple_type, marker = str, None
        else:
            simple_type, marker = _get_mcp_type_info(annotation)

        # Get default value
        default = param.default if param.default is not inspect.Parameter.empty else None

        # Build description with type hint
        if marker == _DATETIME_MARKER:
            type_hint = "str (ISO 8601 datetime)"
        elif marker == _NODE_REF_MARKER:
            type_hint = 'object or array ({"space": str, "externalId": str} or [...])'
        elif hasattr(simple_type, "__name__"):
            type_hint = simple_type.__name__
        else:
            type_hint = str(simple_type)

        param_info.append((param_name, simple_type, default, marker))
        param_descriptions.append(f"  - {param_name}: {type_hint}")

    # Build docstring with available parameters
    docstring = f"List {name} items with optional filters.\n\nAvailable parameters:\n" + "\n".join(param_descriptions)

    # Create the tool function that passes through kwargs with conversion
    def make_list_fn(api, params):
        # Build marker lookup
        markers = {pname: marker for pname, _, _, marker in params}

        def list_items(**kwargs: Any) -> str:
            # Convert and filter out None values
            converted_kwargs = {}
            for k, v in kwargs.items():
                if v is not None:
                    converted_kwargs[k] = _convert_mcp_value(v, markers.get(k))
            results = api.list(**converted_kwargs)
            return json.dumps([r.model_dump(mode="json") for r in results], default=str)

        list_items.__doc__ = docstring

        # Dynamically set the signature using the collected parameters
        new_params = [
            inspect.Parameter(
                pname, inspect.Parameter.KEYWORD_ONLY, default=pdefault, annotation=ptype | None  # type: ignore
            )
            for pname, ptype, pdefault, _ in params
        ]
        list_items.__signature__ = inspect.Signature(new_params)  # type: ignore
        return list_items

    tool_fn = make_list_fn(api_ref, param_info)
    mcp.tool(name=f"{name}_list")(tool_fn)


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
