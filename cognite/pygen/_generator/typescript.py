from pathlib import Path

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator.config import PygenSDKConfig
from cognite.pygen._pygen_model import APIClassFile, DataClass, DataClassFile, PygenSDKModel

from .generator import Generator


class TypeScriptGenerator(Generator):
    format = "typescript"

    def __init__(self, data_model: DataModelResponseWithViews, config: PygenSDKConfig | None = None) -> None:
        super().__init__(data_model, config)
        self._data_class_generator_cache: dict[str, TypeScriptDataClassGenerator] = {}

    def _get_data_class_generator(self, data_class: DataClassFile) -> "TypeScriptDataClassGenerator":
        """Get or create a TypeScriptDataClassGenerator for the given data class file."""
        if data_class.filename not in self._data_class_generator_cache:
            self._data_class_generator_cache[data_class.filename] = TypeScriptDataClassGenerator(data_class)
        return self._data_class_generator_cache[data_class.filename]

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        generator = self._get_data_class_generator(data_class)
        parts: list[str] = [
            generator.create_import_statements(),
            generator.create_view_reference_constant(),
        ]
        if data_class.write:
            parts.append(generator.generate_write_interface())
        parts.extend(
            [
                generator.generate_read_interface(),
                generator.generate_as_write_function(),
                generator.generate_list_class(),
                generator.generate_filter_class(),
            ]
        )
        return "\n\n".join(parts)

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        raise NotImplementedError()

    def create_data_class_init_code(self, model: PygenSDKModel) -> str:
        raise NotImplementedError()

    def create_api_init_code(self, model: PygenSDKModel) -> str:
        raise NotImplementedError()

    def create_client_code(self, model: PygenSDKModel) -> str:
        raise NotImplementedError()

    def create_package_init_code(self, model: PygenSDKModel) -> str:
        raise NotImplementedError()

    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()


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
  {",\n  ".join(sorted(filter_imports))},
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
