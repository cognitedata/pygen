import textwrap
from pathlib import Path

from cognite.pygen._pygen_model import APIClassFile, DataClass, DataClassFile, Field

from .generator import Generator


class PythonGenerator(Generator):
    format = "python"

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        generator = PythonDataClassGenerator(data_class)
        parts: list[str] = [
            generator.create_import_statements(data_class),
        ]
        if data_class.write:
            parts.append(generator.generate_write_class(data_class))
        parts.extend(
            [
                generator.generate_read_class(data_class),
                generator.generate_read_list_class(data_class),
                generator.generate_filter_class(data_class),
            ]
        )
        return "\n\n\n".join(parts)

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        raise NotImplementedError()

    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()


# Types that require imports from instance_api.models._types
_TYPE_IMPORTS: dict[str, str] = {
    "Date": "cognite.pygen._python.instance_api.models._types",
    "DateTime": "cognite.pygen._python.instance_api.models._types",
    "JsonValue": "cognite.pygen._python.instance_api.models._types",
}

# Filter types that require imports from dtype_filters
_FILTER_IMPORTS: set[str] = {
    "TextFilter",
    "FloatFilter",
    "IntegerFilter",
    "BooleanFilter",
    "DateFilter",
    "DateTimeFilter",
    "DirectRelationFilter",
}


class PythonDataClassGenerator:
    def __init__(self, data_class: DataClassFile) -> None:
        self.data_class = data_class

    def create_import_statements(self, data_class: DataClassFile) -> str:
        """Generate import statements for the data class file."""
        lines: list[str] = []

        # Collect all type hints to determine imports
        all_fields: list[Field] = list(data_class.read.fields)
        if data_class.write:
            all_fields.extend(data_class.write.fields)

        # Check which special types are used
        type_hints = [f.type_hint for f in all_fields]
        used_types: set[str] = set()
        for type_hint in type_hints:
            for type_name in _TYPE_IMPORTS:
                if type_name in type_hint:
                    used_types.add(type_name)

        # Check if we need InstanceId (for direct relations)
        needs_instance_id = any("InstanceId" in th for th in type_hints)

        # Check which filters are used
        filter_names = [f.filter_name for f in data_class.read.fields if f.filter_name]
        used_filters = [fn for fn in filter_names if fn in _FILTER_IMPORTS]

        # Check if alias is needed (when field name differs from cdf_prop_id)
        needs_alias = any(f.name != f.cdf_prop_id for f in all_fields)

        # Standard library imports
        lines.append("from typing import ClassVar, Literal")
        lines.append("")

        # Third-party imports
        if needs_alias:
            lines.append("from pydantic import Field")
            lines.append("")

        # Local imports from instance_api
        instance_imports: list[str] = []
        if "NodeReference" in "".join(type_hints) or data_class.read.instance_type == "edge":
            instance_imports.append("NodeReference")
        instance_imports.append("ViewReference")

        lines.append(
            f"from cognite.pygen._python.instance_api.models._references import {', '.join(sorted(instance_imports))}"
        )

        # Import custom types if needed
        if used_types:
            types_import = ", ".join(sorted(used_types))
            lines.append(f"from cognite.pygen._python.instance_api.models._types import {types_import}")

        # Import dtype_filters
        if used_filters:
            filters_import = ", ".join(sorted(set(used_filters)))
            lines.append("from cognite.pygen._python.instance_api.models.dtype_filters import (")
            lines.append("    FilterContainer,")
            for filter_name in sorted(set(used_filters)):
                lines.append(f"    {filter_name},")
            lines.append(")")
        else:
            lines.append("from cognite.pygen._python.instance_api.models.dtype_filters import FilterContainer")

        # Import instance base classes
        instance_classes: list[str] = ["Instance", "InstanceList"]
        if needs_instance_id:
            instance_classes.append("InstanceId")
        if data_class.write:
            instance_classes.append("InstanceWrite")
        lines.append("from cognite.pygen._python.instance_api.models.instance import (")
        for cls in sorted(instance_classes):
            lines.append(f"    {cls},")
        lines.append(")")

        return "\n".join(lines)

    def generate_read_class(self, data_class: DataClassFile) -> str:
        """Generate the read class for the data class."""
        read = data_class.read
        return textwrap.dedent(f'''class {read.name}(Instance):
            """Read class for {read.display_name} instances."""
        
            _view_id: ClassVar[ViewReference] = ViewReference(
                space="{read.view_id.space}", external_id="{read.view_id.external_id}", version="{read.view_id.version}"
            )
            instance_type: Literal["{read.instance_type}"] = Field("{read.instance_type}", alias="instanceType")
            {self.create_fields(read)}
        
            def as_write(self) -> {read.write_class_name}:
                """Convert to write representation."""
                return {read.write_class_name}.model_validate(self.model_dump(by_alias=True))
        ''')

    @staticmethod
    def create_fields(data_class: DataClass) -> str:
        field_lines = []
        for field in data_class.fields:
            field_line = f"{field.name}: {field.type_hint}"
            field_lines.append(field_line)
        return "\n    ".join(field_lines)

    def generate_read_list_class(self, data_class: DataClassFile) -> str:
        """Generate the list class for the data class."""
        read_list = data_class.read_list
        lines: list[str] = []

        # Class definition
        lines.append(f"class {read_list.name}(InstanceList[{read_list.read_class_name}]):")

        # Docstring
        lines.append(f'    """List of {read_list.read_class_name} instances."""')
        lines.append("")

        # Class variable
        lines.append(f"    _INSTANCE: ClassVar[type[{read_list.read_class_name}]] = {read_list.read_class_name}")

        return "\n".join(lines)

    def generate_filter_class(self, data_class: DataClassFile) -> str:
        """Generate the filter class for the data class."""
        filter_class = data_class.filter
        read = data_class.read

        # Get fields that have filters
        filter_fields = [f for f in read.fields if f.filter_name]

        lines: list[str] = []

        # Class definition
        lines.append(f"class {filter_class.name}(FilterContainer):")

        # __init__ method
        lines.append('    def __init__(self, operator: Literal["and", "or"] = "and") -> None:')
        lines.append(f"        view_id = {read.name}._view_id")

        # Create filter attributes
        for field in filter_fields:
            lines.append(f'        self.{field.name} = {field.filter_name}(view_id, "{field.cdf_prop_id}", operator)')

        # Call super().__init__
        lines.append("        super().__init__(")
        lines.append("            data_type_filters=[")
        for field in filter_fields:
            lines.append(f"                self.{field.name},")
        lines.append("            ],")
        lines.append("            operator=operator,")
        lines.append(f'            instance_type="{filter_class.instance_type}",')
        lines.append("        )")

        return "\n".join(lines)

    def _generate_field_line(self, field: Field, is_write: bool = False) -> str:
        """Generate a single field definition line."""
        name = field.name
        type_hint = field.type_hint
        needs_alias = name != field.cdf_prop_id

        # For write classes, allow tuple input for InstanceId
        if is_write and "InstanceId" in type_hint:
            # Replace InstanceId with InstanceId | tuple[str, str] for write
            if "InstanceId | None" in type_hint:
                type_hint = "InstanceId | tuple[str, str] | None"
            elif "InstanceId" in type_hint:
                type_hint = "InstanceId | tuple[str, str]"

        # Check if nullable (has | None)
        is_nullable = "| None" in type_hint

        if needs_alias:
            if is_nullable:
                return f'    {name}: {type_hint} = Field(None, alias="{field.cdf_prop_id}")'
            else:
                return f'    {name}: {type_hint} = Field(alias="{field.cdf_prop_id}")'
        else:
            if is_nullable:
                return f"    {name}: {type_hint} = None"
            else:
                return f"    {name}: {type_hint}"
