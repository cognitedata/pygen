from pathlib import Path

from cognite.pygen._pygen_model import APIClassFile, DataClass, DataClassFile

from .generator import Generator


class PythonGenerator(Generator):
    format = "python"

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        generator = PythonDataClassGenerator(data_class)
        parts: list[str] = [
            generator.create_import_statements(),
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
        return "\n\n".join(parts)

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        raise NotImplementedError()

    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()


class PythonDataClassGenerator:
    def __init__(self, data_class: DataClassFile, top_level: str = "cognite.pygen._python") -> None:
        self.data_class = data_class
        self.top_level = top_level

    def create_import_statements(self) -> str:
        """Generate import statements for the data class file."""
        import_statements: list[str] = [
            "from typing import ClassVar, Literal",
            "",
        ]
        if any(
            field.default_value is not None or field.cdf_prop_id != field.name
            for field in self.data_class.list_fields()
        ):
            # Any field has a default value or alias, need to import Field from pydantic
            import_statements.append("from pydantic import Field")
            import_statements.append("")
        has_direct_relation = any(self.data_class.list_fields(dtype="InstanceId"))
        if has_direct_relation:
            import_statements.append(
                f"from {self.top_level}.instance_api.models._references import InstanceId, ViewReference"
            )
        else:
            import_statements.append(f"from {self.top_level}.instance_api.models._references import ViewReference")
        if time_fields := set(field.dtype for field in self.data_class.list_fields(dtype={"DateTime", "Date"})):
            import_statements.append(f"from {self.top_level}.instance_api.models._types import {sorted(time_fields)}")

        filter_imports: set[str] = {"    FilterContainer,"}
        for field in self.data_class.read.fields:
            if field.filter_name:
                filter_imports.add(f"    {field.filter_name},")
        import_statements.extend(
            [
                f"from {self.top_level}.instance_api.models.dtype_filters import (",
                *sorted(filter_imports),
                ")",
            ]
        )
        import_statements.append(f"from {self.top_level}.instance_api.models.instance import (")
        import_statements.append("    Instance,")
        if has_direct_relation:
            import_statements.append("    InstanceId,")
        import_statements.append("    InstanceList,")
        if self.data_class.write:
            import_statements.append("    InstanceWrite,")
        import_statements.append(")")
        return "\n".join(import_statements)

    def generate_read_class(self) -> str:
        """Generate the read class for the data class."""
        read = self.data_class.read
        view_id = self.data_class.view_id
        instance_type = self.data_class.instance_type
        write_method = ""
        if self.data_class.write:
            write = self.data_class.write
            write_method = f'''
    def as_write(self) -> {write.name}:
        """Convert to write representation."""
        return {write.name}.model_validate(self.model_dump(by_alias=True))'''

        return f'''class {read.name}(Instance):
    """Read class for {read.display_name} instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="{view_id.space}", external_id="{view_id.external_id}", version="{view_id.version}"
    )
    instance_type: Literal["{instance_type}"] = Field("{instance_type}", alias="instanceType")
    {self.create_fields(read)}
{write_method}
'''

    def generate_write_class(self) -> str:
        write = self.data_class.write
        if not write:
            raise ValueError("No write class defined for this data class file.")
        view_id = self.data_class.view_id
        instance_type = self.data_class.instance_type
        return f'''class {write.name}(InstanceWrite):
    """Write class for {write.display_name} instances."""
    _view_id: ClassVar[ViewReference] = ViewReference(
        space="{view_id.space}", external_id="{view_id.external_id}", version="{view_id.version}"
    )
    instance_type: Literal["{instance_type}"] = Field("{instance_type}", alias="instanceType")
    {self.create_fields(write)}
'''

    @staticmethod
    def create_fields(data_class: DataClass) -> str:
        field_lines = []
        for field in data_class.fields:
            field_line = f"{field.name}: {field.type_hint}"
            args: list[str] = []
            if field.default_value is not None:
                args.append(f"default={field.default_value}")
            if field.cdf_prop_id != field.name:
                args.append(f'alias="{field.cdf_prop_id}"')
            if args:
                field_line += f' = Field({", ".join(args)})'
            field_lines.append(field_line)
        return "\n    ".join(field_lines)

    def generate_read_list_class(self) -> str:
        """Generate the list class for the data class."""
        read = self.data_class.read
        list_cls = self.data_class.read_list
        return f'''class {list_cls.name}(InstanceList[{read.name}]):
    """List of {read.display_name} instances."""
    _INSTANCE: ClassVar[type[{read.name}]] = {read.name}
'''

    def generate_filter_class(self) -> str:
        """Generate the filter class for the data class."""
        filter_class = self.data_class.filter
        read = self.data_class.read
        instance_type = self.data_class.instance_type

        attributes, names = self._create_filter_attributes()
        attribute_str = f"\n{' '*8}".join(attributes)
        attribute_list = f"\n{' '*16}".join(names)
        return f"""class {filter_class.name}(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = {read.name}._view_id
        {attribute_str}
        super().__init__(
            data_type_filters=[
                {attribute_list}
            ],
            operator=operator,
            instance_type="{instance_type}",
        )
"""

    def _create_filter_attributes(self) -> tuple[list[str], list[str]]:
        """Create filter attributes for the filter class."""
        attributes: list[str] = []
        names: list[str] = []
        read = self.data_class.read
        for field in read.fields:
            if not field.filter_name:
                continue
            attributes.append(f'self.{field.name} = {field.filter_name}(view_id, "{field.cdf_prop_id}", operator)')
            names.append(f"self.{field.name},")
        return attributes, names
