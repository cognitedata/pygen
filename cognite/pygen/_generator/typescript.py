from dataclasses import dataclass
from pathlib import Path

from cognite.pygen._pygen_model import APIClassFile, DataClass, DataClassFile, Field, PygenSDKModel
from cognite.pygen._typescript import instance_api

from .generator import Generator


class TypeScriptGenerator(Generator):
    format = "typescript"

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._package_generator = TypeScriptPackageGenerator(self.model, self.config.client_name)

    def generate(self) -> dict[Path, str]:
        model = self.model
        sdk: dict[Path, str] = {}

        # Generate data class files
        for data_class in model.data_classes:
            file_path = Path(f"data_classes/{data_class.filename}")
            sdk[file_path] = self.create_data_class_code(data_class)

        sdk[Path("data_classes/index.ts")] = self.create_data_class_index()

        # Generate API class files
        for api_class in model.api_classes:
            file_path = Path(f"_api/{api_class.filename}")
            sdk[file_path] = self.create_api_class_code(api_class)

        sdk[Path("_api/index.ts")] = self.create_api_index_code()

        # Generate client file
        sdk[Path("_client.ts")] = self.create_client_code()

        sdk[Path("index.ts")] = self.create_package_index_code()

        sdk.update(self.add_instance_api())
        return sdk

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        generator = TypeScriptDataClassGenerator(data_class)
        parts: list[str] = [
            generator.create_import_statements(),
            generator.create_view_reference_constant(),
        ]
        if data_class.write:
            parts.append(generator.generate_write_interface())
        parts.append(generator.generate_read_interface())
        if data_class.write:
            parts.append(generator.generate_as_write_function())
        parts.extend(
            [
                generator.generate_list_class(),
                generator.generate_filter_class(),
            ]
        )
        return "\n\n".join(parts)

    def create_data_class_index(self) -> str:
        """Generate the data_classes/index.ts file that exports all data classes.

        Exports all data classes including read, write, list, filter classes and
        view constants from each view module.
        """
        lines: list[str] = [
            "/**",
            " * Data classes for the generated SDK.",
            " *",
            " * This module exports all data classes including read, write, list, and filter classes.",
            " *",
            " * @packageDocumentation",
            " */",
            "",
        ]

        # Collect all exports
        all_exports: list[str] = []

        for data_class_file in self.model.data_classes:
            # Module name is filename without .ts extension
            module_name = data_class_file.filename.replace(".ts", "")

            # Build list of exports from this module
            exports: list[str] = []

            # Add view constant (UPPER_SNAKE_CASE)
            view_const = self._to_view_const_name(data_class_file.read.name)
            exports.append(view_const)

            # Add data classes
            if data_class_file.write:
                exports.append(data_class_file.write.name)
            exports.append(data_class_file.read.name)

            # Add asWrite function
            as_write_func = data_class_file.read.name[0].lower() + data_class_file.read.name[1:] + "AsWrite"
            exports.append(as_write_func)

            # Add list and filter classes
            exports.append(data_class_file.read_list.name)
            exports.append(data_class_file.filter.name)

            all_exports.extend(exports)

            # Generate export statement
            exports_str = ",\n  ".join(exports)
            lines.append(f'export {{\n  {exports_str},\n}} from "./{module_name}.ts";')

        return "\n".join(lines)

    @staticmethod
    def _to_view_const_name(class_name: str) -> str:
        """Convert PascalCase class name to UPPER_SNAKE_CASE view constant name."""
        result: list[str] = []
        for i, char in enumerate(class_name):
            if char.isupper() and i > 0:
                result.append("_")
            result.append(char.upper())
        return "".join(result) + "_VIEW"

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        """Generate API class code for a single view."""
        generator = TypeScriptAPIGenerator(api_class)
        parts = [
            generator.create_import_statements(),
            generator.create_helper_function(),
            generator.create_api_class(),
        ]
        return "\n".join(parts)

    def create_api_index_code(self) -> str:
        """Generate the _api/index.ts file."""
        return self._package_generator.create_api_index()

    def create_client_code(self) -> str:
        """Generate the _client.ts file."""
        return self._package_generator.create_client()

    def create_package_index_code(self) -> str:
        """Generate the root index.ts file."""
        return self._package_generator.create_package_index()

    def add_instance_api(self) -> dict[Path, str]:
        instance_api_files: dict[Path, str] = {}
        location = Path(instance_api.__path__[0])
        for file in location.rglob("**/*.ts"):
            relative_path = file.relative_to(location)
            instance_api_files[location.name / relative_path] = file.read_text(encoding="utf-8")
        return instance_api_files


class TypeScriptDataClassGenerator:
    """Generator for TypeScript data class files.

    Generates TypeScript interfaces and classes for a single view including:
    - View reference constant
    - Write interface (extends NodeInstanceWrite or EdgeInstanceWrite)
    - Read interface (extends NodeInstance or EdgeInstance)
    - asWrite conversion function
    - List class extending InstanceList<T>
    - Filter class extending FilterContainer
    """

    def __init__(self, data_class: DataClassFile) -> None:
        self.data_class = data_class
        self._view_const_name = self._create_view_const_name()

    def _create_view_const_name(self) -> str:
        """Create the view constant name in UPPER_SNAKE_CASE."""
        name = self.data_class.read.name
        # Convert PascalCase to UPPER_SNAKE_CASE
        result: list[str] = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append("_")
            result.append(char.upper())
        return "".join(result) + "_VIEW"

    def create_import_statements(self) -> str:
        """Generate import statements for the data class file."""
        instance_type = self.data_class.instance_type
        is_node = instance_type == "node"

        # Determine which base types to import based on instance type
        if is_node:
            instance_type_imports = "NodeInstance, NodeInstanceWrite"
        else:
            instance_type_imports = "EdgeInstance, EdgeInstanceWrite"

        # Collect filter type imports
        filter_imports: set[str] = {"FilterContainer"}
        for field in self.data_class.read.fields:
            if field.filter_name:
                filter_imports.add(field.filter_name)

        # Build reference imports
        reference_imports: list[str] = ["ViewReference"]
        has_direct_relation = any(self.data_class.list_fields(dtype="InstanceId"))
        if has_direct_relation:
            reference_imports.append("InstanceId")
        if not is_node:
            reference_imports.append("NodeReference")
        filter_import_str = ",\n  ".join(sorted(filter_imports))
        return f"""/**
 * Data classes for {self.data_class.read.display_name}.
 *
 * @packageDocumentation
 */

import type {{
  {instance_type_imports},
}} from "../instance_api/types/instance.ts";
import {{ InstanceList }} from "../instance_api/types/instance.ts";
import type {{ {", ".join(sorted(reference_imports))} }} from "../instance_api/types/references.ts";
import {{
  {filter_import_str},
}} from "../instance_api/types/dtypeFilters.ts";"""

    def create_view_reference_constant(self) -> str:
        """Generate the view reference constant."""
        view_id = self.data_class.view_id
        return f"""/** View reference for {self.data_class.read.name} */
export const {self._view_const_name}: ViewReference = {{
  space: "{view_id.space}",
  externalId: "{view_id.external_id}",
  version: "{view_id.version}",
}};"""

    def generate_write_interface(self) -> str:
        """Generate the write interface for the data class."""
        write = self.data_class.write
        if not write:
            raise ValueError("No write class defined for this data class file.")

        instance_type = self.data_class.instance_type
        is_node = instance_type == "node"
        base_interface = "NodeInstanceWrite" if is_node else "EdgeInstanceWrite"

        # For edges, add startNode and endNode
        edge_properties = ""
        if not is_node:
            edge_properties = """
  startNode: NodeReference;
  endNode: NodeReference;"""

        fields_str = self._create_interface_fields(write, readonly=False)

        return f"""/**
 * Write class for {write.display_name} instances.
 *
 * {write.description}
 */
export interface {write.name} extends {base_interface} {{
  readonly instanceType: "{instance_type}";{edge_properties}
{fields_str}
}}"""

    def generate_read_interface(self) -> str:
        """Generate the read interface for the data class."""
        read = self.data_class.read
        instance_type = self.data_class.instance_type
        is_node = instance_type == "node"
        base_interface = "NodeInstance" if is_node else "EdgeInstance"

        fields_str = self._create_interface_fields(read, readonly=True)

        return f"""/**
 * Read class for {read.display_name} instances.
 *
 * Contains all properties including system metadata.
 */
export interface {read.name} extends {base_interface} {{
  readonly instanceType: "{instance_type}";
{fields_str}
}}"""

    def _create_interface_fields(self, data_class: DataClass, readonly: bool) -> str:
        """Create interface field definitions."""
        lines: list[str] = []
        for field in data_class.fields:
            # Build the field line
            readonly_prefix = "readonly " if readonly else ""

            # Convert type hint for write context (direct relations can also accept tuple)
            type_hint = field.type_hint
            if not readonly and "InstanceId" in type_hint:
                # For write interfaces, also accept tuple format
                if "| undefined" in type_hint:
                    type_hint = type_hint.replace(
                        "InstanceId | undefined", "InstanceId | readonly [string, string] | undefined"
                    )
                else:
                    type_hint = type_hint.replace("InstanceId", "InstanceId | readonly [string, string]")

            # Handle optional fields (with | undefined)
            is_optional = "| undefined" in type_hint
            if is_optional:
                # Remove | undefined and use optional syntax
                type_hint = type_hint.replace(" | undefined", "")
                field_name = f"{field.name}?"
            else:
                field_name = field.name

            # Add JSDoc comment if there's a description
            if field.description:
                lines.append(f"  /** {field.description} */")

            lines.append(f"  {readonly_prefix}{field_name}: {type_hint};")

        return "\n".join(lines)

    def generate_as_write_function(self) -> str:
        """Generate the asWrite conversion function."""
        read = self.data_class.read
        write = self.data_class.write

        if not write:
            # If no write class, generate a no-op warning
            return f"""/**
 * Note: {read.name} does not have a writable representation.
 */"""

        # Create function name in camelCase from PascalCase class name
        func_name = read.name[0].lower() + read.name[1:] + "AsWrite"

        return f"""/**
 * Converts a {read.name} read instance to a write instance.
 *
 * @param instance - The {read.name} to convert
 * @returns A {write.name} instance
 */
export function {func_name}(instance: {read.name}): {write.name} {{
  const {{ dataRecord, ...rest }} = instance;
  const write: {write.name} = {{
    ...rest,
  }};
  if (dataRecord) {{
    (write as {{ dataRecord?: {{ existingVersion: number }} }}).dataRecord = {{
      existingVersion: dataRecord.version,
    }};
  }}
  return write;
}}"""

    def generate_list_class(self) -> str:
        """Generate the list class for the data class."""
        read = self.data_class.read
        list_cls = self.data_class.read_list
        write = self.data_class.write

        # Create the asWrite function name
        func_name = read.name[0].lower() + read.name[1:] + "AsWrite"

        # Determine write type name
        write_type = write.name if write else read.name

        return f"""/**
 * List of {read.display_name} instances.
 */
export class {list_cls.name} extends InstanceList<{read.name}> {{
  /**
   * Creates a new {list_cls.name}.
   *
   * @param items - Initial items to populate the list
   */
  constructor(items?: readonly {read.name}[]) {{
    super(items, {self._view_const_name});
  }}

  /**
   * Converts all instances in the list to write instances.
   *
   * @returns Array of {write_type} instances
   */
  asWrite(): {write_type}[] {{
    return this.map({func_name});
  }}

  /**
   * Creates a new {list_cls.name} from an array.
   *
   * @param items - Array of {read.name} instances
   * @returns A new {list_cls.name}
   */
  static fromArray(items: readonly {read.name}[]): {list_cls.name} {{
    return new {list_cls.name}(items);
  }}
}}"""

    def generate_filter_class(self) -> str:
        """Generate the filter class for the data class."""
        filter_cls = self.data_class.filter
        instance_type = self.data_class.instance_type

        # Collect filter attributes and constructor assignments
        filter_fields: list[str] = []
        constructor_assignments: list[str] = []
        filter_names: list[str] = []

        for field in self.data_class.read.fields:
            if not field.filter_name:
                continue

            # Add property declaration
            filter_fields.append(f"  /** Filter for the {field.name} property */")
            filter_fields.append(f"  readonly {field.name}: {field.filter_name};")

            # Add constructor assignment
            view_const = self._view_const_name
            filter_class = field.filter_name
            prop_id = field.cdf_prop_id
            constructor_assignments.append(
                f'    const {field.name} = new {filter_class}({view_const}, "{prop_id}", operator);'
            )
            filter_names.append(field.name)

        # Build the class
        fields_str = "\n".join(filter_fields)
        assignments_str = "\n".join(constructor_assignments)
        filter_array = ", ".join(filter_names)
        this_assignments = "\n".join(f"    this.{name} = {name};" for name in filter_names)

        return f"""/**
 * Filter container for {self.data_class.read.display_name} instances.
 *
 * Provides type-safe filters for all {self.data_class.read.name} properties.
 */
export class {filter_cls.name} extends FilterContainer {{
{fields_str}

  /**
   * Creates a new {filter_cls.name}.
   *
   * @param operator - How to combine filters ("and" or "or"). Defaults to "and"
   */
  constructor(operator: "and" | "or" = "and") {{
{assignments_str}

    super([{filter_array}], operator, "{instance_type}");

{this_assignments}
  }}
}}"""


# ============================================================================
# Filter Parameter Templates for TypeScript
# ============================================================================


@dataclass
class TSFilterParam:
    """Represents a filter parameter for a TypeScript API method."""

    name: str
    type_hint: str
    filter_call: str  # e.g., "filter.name.equalsOrIn(options.name ?? null)"


# Mapping from filter name to TypeScript parameter generation
_TS_FILTER_PARAM_TEMPLATES: dict[str, list[tuple[str, str, str]]] = {
    "TextFilter": [
        ("", "string | readonly string[]", ".equalsOrIn(options.{param} ?? null)"),
        ("Prefix", "string", ".prefix(options.{param} ?? null)"),
    ],
    "IntegerFilter": [
        ("", "number", ".greaterThanOrEquals(options.{param} ?? null)"),
        ("", "number", ".lessThanOrEquals(options.{param} ?? null)"),
    ],
    "FloatFilter": [
        ("", "number", ".greaterThanOrEquals(options.{param} ?? null)"),
        ("", "number", ".lessThanOrEquals(options.{param} ?? null)"),
    ],
    "BooleanFilter": [
        ("", "boolean", ".equals(options.{param} ?? null)"),
    ],
    "DateFilter": [
        ("", "Date | string", ".greaterThanOrEquals(options.{param} ?? null)"),
        ("", "Date | string", ".lessThanOrEquals(options.{param} ?? null)"),
    ],
    "DateTimeFilter": [
        ("", "Date | string", ".greaterThanOrEquals(options.{param} ?? null)"),
        ("", "Date | string", ".lessThanOrEquals(options.{param} ?? null)"),
    ],
    "DirectRelationFilter": [
        (
            "",
            (
                "string | InstanceId | readonly [string, string] | "
                "readonly (string | InstanceId | readonly [string, string])[]"
            ),
            ".equalsOrIn(options.{param} ?? null)",
        ),
    ],
}


def _create_ts_filter_params(field: Field) -> list[TSFilterParam]:
    """Create TypeScript filter parameters for a field based on its filter type."""
    if not field.filter_name:
        return []

    templates = _TS_FILTER_PARAM_TEMPLATES.get(field.filter_name, [])
    params: list[TSFilterParam] = []

    # Determine if this is a range filter type (uses min/max prefixes)
    range_filter_types = {"IntegerFilter", "FloatFilter", "DateFilter", "DateTimeFilter"}
    is_range_filter = field.filter_name in range_filter_types

    for i, (suffix, type_hint, filter_call_template) in enumerate(templates):
        # For range filters (min/max), use min/max prefix (camelCase)
        if is_range_filter and len(templates) == 2 and suffix == "":
            prefix = "min" if i == 0 else "max"
            # Capitalize first letter of field name
            param_name = f"{prefix}{field.name[0].upper()}{field.name[1:]}"
        else:
            param_name = f"{field.name}{suffix}"

        params.append(
            TSFilterParam(
                name=param_name,
                type_hint=type_hint,
                filter_call=f"filter.{field.name}{filter_call_template.format(param=param_name)}",
            )
        )

    return params


# ============================================================================
# TypeScriptAPIGenerator
# ============================================================================


class TypeScriptAPIGenerator:
    """Generator for TypeScript API class files."""

    def __init__(self, api_class: APIClassFile) -> None:
        self.api_class = api_class
        self.data_class = api_class.data_class
        self._filter_params: list[TSFilterParam] | None = None
        self._view_const_name = self._create_view_const_name()

    def _create_view_const_name(self) -> str:
        """Create the view constant name in UPPER_SNAKE_CASE."""
        name = self.data_class.read.name
        result: list[str] = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append("_")
            result.append(char.upper())
        return "".join(result) + "_VIEW"

    @property
    def filter_params(self) -> list[TSFilterParam]:
        """Get all filter parameters for the API methods."""
        if self._filter_params is None:
            self._filter_params = []
            for field in self.data_class.read.fields:
                self._filter_params.extend(_create_ts_filter_params(field))
        return self._filter_params

    def create_import_statements(self) -> str:
        """Generate import statements for the API class file."""
        read_name = self.data_class.read.name
        filter_name = self.data_class.filter.name
        list_name = self.data_class.read_list.name
        view_const = self._view_const_name

        # Check if we need InstanceId import (for direct relation filters)
        has_direct_relation = any(field.filter_name == "DirectRelationFilter" for field in self.data_class.read.fields)
        instance_id_import = ", InstanceId" if has_direct_relation else ""

        return f"""/**
 * API class for {read_name} instances.
 *
 * @packageDocumentation
 */

import {{ InstanceAPI }} from "../instance_api/api.ts";
import type {{ PygenClientConfig }} from "../instance_api/auth/index.ts";
import type {{ ViewReference{instance_id_import} }} from "../instance_api/types/references.ts";
import type {{ Aggregation, PropertySort, SortDirection }} from "../instance_api/types/query.ts";
import type {{ AggregateResponse, Page }} from "../instance_api/types/responses.ts";
import {{ InstanceList }} from "../instance_api/types/instance.ts";

import {{
  {view_const},
  {read_name},
  {filter_name},
  {list_name},
}} from "../data_classes/index.ts";"""

    def create_helper_function(self) -> str:
        """Generate the createPropertyRef helper function."""
        return """
/**
 * Creates a property reference for sorting.
 *
 * @param viewRef - The view reference
 * @param propertyName - The property name
 * @returns A property path array
 */
function createPropertyRef(
  viewRef: ViewReference,
  propertyName: string,
): [string, string, string] {
  return [viewRef.space, `${viewRef.externalId}/${viewRef.version}`, propertyName];
}"""

    def _create_filter_options_interface(self, include_pagination: bool = False, include_sort: bool = False) -> str:
        """Create the filter options interface fields."""
        lines: list[str] = []
        for param in self.filter_params:
            lines.append(f"    {param.name}?: {param.type_hint};")

        # Add common filter params
        lines.extend(
            [
                "    externalIdPrefix?: string;",
                "    space?: string | readonly string[];",
            ]
        )

        if include_pagination:
            lines.extend(
                [
                    "    cursor?: string;",
                    "    limit?: number;",
                ]
            )
        elif include_sort:
            lines.extend(
                [
                    "    sortBy?: string;",
                    "    sortDirection?: SortDirection;",
                    "    limit?: number;",
                ]
            )

        return "\n".join(lines)

    def _create_filter_calls(self) -> str:
        """Create filter method calls for building the filter."""
        filter_name = self.data_class.filter.name
        lines: list[str] = [f'    const filter = new {filter_name}("and");']

        for param in self.filter_params:
            lines.append(f"    {param.filter_call};")

        lines.extend(
            [
                "    filter.externalId.prefix(options.externalIdPrefix ?? null);",
                "    filter.space.equalsOrIn(options.space ?? null);",
            ]
        )

        return "\n".join(lines)

    def create_api_class(self) -> str:
        """Generate the complete API class."""
        api_name = self.api_class.name
        read_name = self.data_class.read.name
        instance_type = self.data_class.instance_type
        view_const = self._view_const_name

        # Generate all method parts
        build_filter_method = self._create_build_filter_method()
        retrieve_method = self._create_retrieve_method()
        iterate_method = self._create_iterate_method()
        search_method = self._create_search_method()
        aggregate_method = self._create_aggregate_method()
        list_method = self._create_list_method()

        return f"""
/**
 * API for {read_name} instances with type-safe filter methods.
 */
export class {api_name} extends InstanceAPI<{read_name}> {{
  /**
   * Creates a new {api_name}.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {{
    super(config, {view_const}, "{instance_type}");
  }}

{build_filter_method}

{retrieve_method}

{iterate_method}

{search_method}

{aggregate_method}

{list_method}
}}"""

    def _create_build_filter_method(self) -> str:
        """Generate the private _buildFilter method."""
        filter_name = self.data_class.filter.name
        filter_options = self._create_filter_options_interface()
        filter_calls = self._create_filter_calls()

        return f"""  /**
   * Builds a {filter_name} from the given options.
   *
   * @param options - Filter options
   * @returns A configured {filter_name}
   */
  private _buildFilter(options: {{
{filter_options}
  }}): {filter_name} {{
{filter_calls}

    return filter;
  }}"""

    def _create_retrieve_method(self) -> str:
        """Generate the retrieve method with overloads."""
        read_name = self.data_class.read.name
        list_name = self.data_class.read_list.name

        return f"""  /**
   * Retrieve {read_name} instances by ID.
   *
   * @param id - Instance identifier. Can be a string, InstanceId, tuple, or array
   * @param options - Additional options
   * @returns For single id: The {read_name} if found, undefined otherwise.
   *          For array of ids: A {list_name} of found instances.
   */
  async retrieve(
    id: string | InstanceId | readonly [string, string],
    options?: {{ space?: string }},
  ): Promise<{read_name} | undefined>;
  async retrieve(
    id: readonly (string | InstanceId | readonly [string, string])[],
    options?: {{ space?: string }},
  ): Promise<{list_name}>;
  async retrieve(
    id:
      | string
      | InstanceId
      | readonly [string, string]
      | readonly (string | InstanceId | readonly [string, string])[],
    options: {{ space?: string }} = {{}},
  ): Promise<{read_name} | {list_name} | undefined> {{
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {{
      return await this._retrieve(
        id as string | InstanceId | readonly [string, string],
        options,
      ) as {read_name} | undefined;
    }}
    const result = await this._retrieve(
      id as readonly (string | InstanceId | readonly [string, string])[],
      options,
    );
    return new {list_name}([...(result as InstanceList<{read_name}>)]);
  }}"""

    def _create_iterate_method(self) -> str:
        """Generate the iterate method."""
        list_name = self.data_class.read_list.name
        filter_options = self._create_filter_options_interface(include_pagination=True)

        return f"""  /**
   * Iterate over instances with pagination.
   *
   * @param options - Filter and pagination options
   * @returns A Page containing items and optional next cursor.
   */
  async iterate(options: {{
{filter_options}
  }} = {{}}): Promise<Page<{list_name}>> {{
    const filter = this._buildFilter(options);
    const page = await this._iterate({{
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    }});

    return {{ ...page, items: new {list_name}([...page.items]) }};
  }}"""

    def _create_search_method(self) -> str:
        """Generate the search method."""
        list_name = self.data_class.read_list.name
        filter_options = self._create_filter_options_interface()

        return f"""  /**
   * Search instances using full-text search.
   *
   * @param options - Search and filter options
   * @returns A {list_name} of matching instances.
   */
  async search(options: {{
    query?: string;
    properties?: string | readonly string[];
{filter_options}
    limit?: number;
  }} = {{}}): Promise<{list_name}> {{
    const filter = this._buildFilter(options);

    const result = await this._search({{
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    }});

    return new {list_name}([...result.items]);
  }}"""

    def _create_aggregate_method(self) -> str:
        """Generate the aggregate method."""
        filter_options = self._create_filter_options_interface()

        return f"""  /**
   * Aggregate instances.
   *
   * @param aggregate - Aggregation(s) to perform.
   * @param options - Filter and grouping options.
   * @returns AggregateResponse with aggregated values.
   */
  async aggregate(
    aggregate: Aggregation | readonly Aggregation[],
    options: {{
      groupBy?: string | readonly string[];
{filter_options}
    }} = {{}},
  ): Promise<AggregateResponse> {{
    const filter = this._buildFilter(options);

    return this._aggregate(aggregate, {{
      groupBy: options.groupBy,
      filter: filter.asFilter(),
    }});
  }}"""

    def _create_list_method(self) -> str:
        """Generate the list method."""
        list_name = self.data_class.read_list.name
        view_const = self._view_const_name
        filter_options = self._create_filter_options_interface(include_sort=True)

        return f"""  /**
   * List instances with type-safe filtering.
   *
   * @param options - Filter, sort, and pagination options.
   * @returns A {list_name} of matching instances.
   */
  async list(options: {{
{filter_options}
  }} = {{}}): Promise<{list_name}> {{
    const filter = this._buildFilter(options);
    const sort: PropertySort | undefined = options.sortBy !== undefined
      ? {{
        property: createPropertyRef({view_const}, options.sortBy),
        direction: options.sortDirection,
      }}
      : undefined;

    const result = await this._list({{
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    }});

    return new {list_name}([...result]);
  }}"""


# ============================================================================
# TypeScriptPackageGenerator
# ============================================================================


class TypeScriptPackageGenerator:
    """Generator for TypeScript package structure files (index.ts, _client.ts)."""

    def __init__(self, model: PygenSDKModel, client_name: str) -> None:
        self.model = model
        self.client_name = client_name

    def create_api_index(self) -> str:
        """Generate the _api/index.ts file that exports all API classes."""
        lines: list[str] = [
            "/**",
            " * API classes for the generated SDK.",
            " *",
            " * This module exports all view-specific API classes.",
            " *",
            " * @packageDocumentation",
            " */",
            "",
        ]

        # Collect and sort API classes by name
        api_classes_sorted = sorted(self.model.api_classes, key=lambda x: x.name)

        for api_class in api_classes_sorted:
            # Module name is filename without .ts extension
            module_name = api_class.filename.replace(".ts", "")
            lines.append(f'export {{ {api_class.name} }} from "./{module_name}.ts";')

        return "\n".join(lines)

    def create_client(self) -> str:
        """Generate the _client.ts file with the client class."""
        # Collect and sort API classes
        api_classes_sorted = sorted(self.model.api_classes, key=lambda x: x.name)

        # Build imports
        api_imports = ", ".join(api.name for api in api_classes_sorted)

        # Build property declarations
        property_declarations = "\n".join(
            f"  /** API for {api.data_class.read.name} instances */\n"
            f"  readonly {api.client_attribute_name}: {api.name};"
            for api in api_classes_sorted
        )

        # Build constructor initializations
        api_inits = "\n".join(
            f"    this.{api.client_attribute_name} = new {api.name}(config);" for api in api_classes_sorted
        )

        # Build view list for docstring
        view_list = "\n".join(f" * - {api.client_attribute_name}: {api.name}" for api in api_classes_sorted)

        return f"""/**
 * Client for the generated SDK.
 *
 * This module contains the {self.client_name} that composes view-specific APIs.
 *
 * @packageDocumentation
 */

import type {{ PygenClientConfig }} from "./instance_api/auth/index.ts";
import {{ InstanceClient }} from "./instance_api/client.ts";

import {{ {api_imports} }} from "./_api/index.ts";

/**
 * Generated client for interacting with the data model.
 *
 * This client provides access to the following views:
{view_list}
 */
export class {self.client_name} extends InstanceClient {{
{property_declarations}

  /**
   * Creates a new {self.client_name}.
   *
   * @param config - Configuration for the client including URL, project, and credentials.
   * @param writeWorkers - Number of concurrent workers for write operations. Default is 5.
   * @param deleteWorkers - Number of concurrent workers for delete operations. Default is 3.
   * @param retrieveWorkers - Number of concurrent workers for retrieve operations. Default is 10.
   */
  constructor(
    config: PygenClientConfig,
    writeWorkers = 5,
    deleteWorkers = 3,
    retrieveWorkers = 10,
  ) {{
    super(config, writeWorkers, deleteWorkers, retrieveWorkers);

    // Initialize view-specific APIs
{api_inits}
  }}
}}"""

    def create_package_index(self) -> str:
        """Generate the root index.ts file that exports the client and re-exports data classes."""
        return f"""/**
 * Generated SDK package.
 *
 * This package provides the {self.client_name} for interacting with the data model.
 *
 * @packageDocumentation
 */

export {{ {self.client_name} }} from "./_client.ts";
export * from "./data_classes/index.ts";
"""
