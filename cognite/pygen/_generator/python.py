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
            parts.append(generator.generate_write_class())
        parts.extend(
            [
                generator.generate_read_class(),
                generator.generate_read_list_class(),
                generator.generate_filter_class(),
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
        raise NotImplementedError()

    def generate_read_class(self) -> str:
        """Generate the read class for the data class."""
        read = self.data_class.read
        return f'''class {read.name}(Instance):
            """Read class for {read.display_name} instances."""

            _view_id: ClassVar[ViewReference] = ViewReference(
                space="{read.view_id.space}", external_id="{read.view_id.external_id}", version="{read.view_id.version}"
            )
            instance_type: Literal["{read.instance_type}"] = Field("{read.instance_type}", alias="instanceType")
            {self.create_fields(read)}

            def as_write(self) -> {read.write_class_name}:
                """Convert to write representation."""
                return {read.write_class_name}.model_validate(self.model_dump(by_alias=True))
        '''

    def generate_write_class(self) -> str:
        raise NotImplementedError()

    @staticmethod
    def create_fields(data_class: DataClass) -> str:
        field_lines = []
        for field in data_class.fields:
            field_line = f"{field.name}: {field.type_hint}"
            field_lines.append(field_line)
        return "\n    ".join(field_lines)

    def generate_read_list_class(self) -> str:
        """Generate the list class for the data class."""
        raise NotImplementedError()

    def generate_filter_class(self) -> str:
        """Generate the filter class for the data class."""
        filter_class = self.data_class.filter
        read = self.data_class.read
        return f"""class {filter_class.name}(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = {read.name}._view_id
        self.property_one = TextFilter(view_id, "prop1", operator)
        self.property_two = IntegerFilter(view_id, "prop2", operator)
        super().__init__(
            data_type_filters=[
                self.property_one,
                self.property_two,
            ],
            operator=operator,
            instance_type="{read.instance_type}",
        )
"""

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
