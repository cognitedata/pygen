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

        # Search tool (if available)
        if hasattr(api, "search"):
            _make_search_tool(mcp, api, attr_name)

        # Aggregate tool (if available)
        if hasattr(api, "aggregate"):
            _make_aggregate_tool(mcp, api, attr_name)

        # Histogram tool (if available)
        if hasattr(api, "histogram"):
            _make_histogram_tool(mcp, api, attr_name)

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

    def handle_result(result: Any) -> str:
        return json.dumps([r.model_dump(mode="json") for r in result], default=str)

    _make_dynamic_tool(
        mcp,
        api_ref,
        "list",
        f"{name}_list",
        f"List/filter {name} items.",
        handle_result,
    )


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


def _parse_docstring_args(docstring: str | None) -> dict[str, str]:
    """Parse docstring Args section to extract parameter descriptions."""
    if not docstring:
        return {}

    descriptions: dict[str, str] = {}
    in_args = False
    current_param = None
    current_desc_lines: list[str] = []

    for line in docstring.split("\n"):
        stripped = line.strip()

        # Detect Args section
        if stripped == "Args:":
            in_args = True
            continue

        # Detect end of Args section
        if in_args and stripped and not stripped.startswith(" ") and ":" in stripped and stripped.endswith(":"):
            # New section like "Returns:", "Raises:", etc.
            if current_param:
                descriptions[current_param] = " ".join(current_desc_lines).strip()
            break

        if in_args:
            # Check for new parameter (starts with param_name:)
            if stripped and ":" in stripped and not line.startswith("        "):
                # Save previous parameter
                if current_param:
                    descriptions[current_param] = " ".join(current_desc_lines).strip()

                # Parse new parameter
                parts = stripped.split(":", 1)
                current_param = parts[0].strip()
                current_desc_lines = [parts[1].strip()] if len(parts) > 1 else []
            elif current_param and stripped:
                # Continuation of current parameter description
                current_desc_lines.append(stripped)

    # Don't forget the last parameter
    if current_param:
        descriptions[current_param] = " ".join(current_desc_lines).strip()

    return descriptions


# Parameters that should always be exposed as strings (method-specific params)
_FORCE_STRING_PARAMS = {
    "aggregate",  # Aggregation type: count, sum, avg, min, max
    "group_by",  # Field name to group by
    "property",  # Field name for aggregation/histogram
    "query",  # Search query
    "search_property",  # Field to search in
    "properties",  # Fields to search (for search method)
    "interval",  # Histogram interval (float)
}


def _introspect_method_params(
    method: Any,
) -> tuple[list[tuple[str, Any, Any, str | None]], dict[str, str]]:
    """Introspect a method and return MCP-compatible parameters.

    Returns:
        (param_info, param_descriptions) where param_info is list of (name, type, default, marker)
        and param_descriptions is dict of parameter name to description from docstring.
    """
    sig = inspect.signature(method)
    param_info: list[tuple[str, Any, Any, str | None]] = []

    # Parse docstring for descriptions
    docstring_descs = _parse_docstring_args(method.__doc__)

    for param_name, param in sig.parameters.items():
        if param_name == "self":
            continue

        annotation = param.annotation

        # Force include certain method-specific parameters
        if param_name in _FORCE_STRING_PARAMS:
            if param_name == "interval":
                simple_type = float
            else:
                simple_type = str
            default = param.default if param.default is not inspect.Parameter.empty else None
            param_info.append((param_name, simple_type, default, None))
            continue

        # Skip complex types that MCP can't handle
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

        default = param.default if param.default is not inspect.Parameter.empty else None
        param_info.append((param_name, simple_type, default, marker))

    return param_info, docstring_descs


def _build_param_description(
    param_info: list[tuple[str, Any, Any, str | None]],
    docstring_descs: dict[str, str],
) -> list[str]:
    """Build parameter descriptions for docstring."""
    descriptions = []
    for pname, ptype, _, marker in param_info:
        if marker == _DATETIME_MARKER:
            type_hint = "str (ISO 8601 datetime)"
        elif marker == _NODE_REF_MARKER:
            type_hint = 'object or array ({"space": str, "externalId": str})'
        elif hasattr(ptype, "__name__"):
            type_hint = ptype.__name__
        else:
            type_hint = str(ptype)

        # Get description from docstring if available
        desc = docstring_descs.get(pname, "")
        if desc:
            descriptions.append(f"  - {pname} ({type_hint}): {desc}")
        else:
            descriptions.append(f"  - {pname}: {type_hint}")
    return descriptions


def _make_dynamic_tool(
    mcp: Any,
    api_ref: Any,
    method_name: str,
    tool_name: str,
    base_description: str,
    result_handler: Any,
) -> None:
    """Create a tool with dynamically introspected parameters."""
    method = getattr(api_ref, method_name)
    param_info, docstring_descs = _introspect_method_params(method)
    param_descriptions = _build_param_description(param_info, docstring_descs)

    docstring = f"{base_description}\n\nParameters:\n" + "\n".join(param_descriptions)

    # Build marker lookup for conversions
    markers = {pname: marker for pname, _, _, marker in param_info}

    def make_fn(api, method_nm, param_markers, handler):
        def tool_fn(**kwargs: Any) -> str:
            converted_kwargs = {}
            for k, v in kwargs.items():
                if v is not None:
                    converted_kwargs[k] = _convert_mcp_value(v, param_markers.get(k))
            result = getattr(api, method_nm)(**converted_kwargs)
            return handler(result)

        return tool_fn

    tool_fn = make_fn(api_ref, method_name, markers, result_handler)
    tool_fn.__doc__ = docstring

    # Build signature
    new_params = [
        inspect.Parameter(
            pname, inspect.Parameter.KEYWORD_ONLY, default=pdefault, annotation=ptype | None  # type: ignore
        )
        for pname, ptype, pdefault, _ in param_info
    ]
    tool_fn.__signature__ = inspect.Signature(new_params)  # type: ignore

    mcp.tool(name=tool_name)(tool_fn)


def _make_search_tool(mcp: Any, api_ref: Any, name: str) -> None:
    """Create a search tool for an API with dynamically introspected parameters."""

    def handle_result(result: Any) -> str:
        return json.dumps([r.model_dump(mode="json") for r in result], default=str)

    _make_dynamic_tool(
        mcp,
        api_ref,
        "search",
        f"{name}_search",
        f"Search {name} items by text query.",
        handle_result,
    )


def _make_aggregate_tool(mcp: Any, api_ref: Any, name: str) -> None:
    """Create an aggregate tool for an API with dynamically introspected parameters."""

    def handle_result(result: Any) -> str:
        if isinstance(result, list):
            return json.dumps([_serialize_aggregate(r) for r in result], default=str)
        return json.dumps(_serialize_aggregate(result), default=str)

    _make_dynamic_tool(
        mcp,
        api_ref,
        "aggregate",
        f"{name}_aggregate",
        f"Aggregate {name} items (count, sum, avg, min, max).",
        handle_result,
    )


def _serialize_aggregate(result: Any) -> dict[str, Any]:
    """Serialize an aggregate result to a dict."""
    if hasattr(result, "model_dump"):
        return result.model_dump(mode="json")
    if hasattr(result, "value"):
        data: dict[str, Any] = {"value": result.value}
        if hasattr(result, "aggregate"):
            data["aggregate"] = result.aggregate
        return data
    return {"value": result}


def _make_histogram_tool(mcp: Any, api_ref: Any, name: str) -> None:
    """Create a histogram tool for an API with dynamically introspected parameters."""

    def handle_result(result: Any) -> str:
        # HistogramValue has .buckets attribute containing the histogram bins
        buckets = getattr(result, "buckets", result)
        return json.dumps([{"start": b.start, "count": b.count} for b in buckets], default=str)

    _make_dynamic_tool(
        mcp,
        api_ref,
        "histogram",
        f"{name}_histogram",
        f"Generate histogram for {name} numeric properties.",
        handle_result,
    )
