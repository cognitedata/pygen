from cognite.pygen._pygen_model import DataClass, ReadDataClass


class PythonGenerator:
    def create_write_class(self, write: DataClass) -> str:
        return f'''class {write.name}(InstanceWrite):
    """Write class for {write.display_name} instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="{write.view_id.space}", external_id="{write.view_id.external_id}", version="{write.view_id.version}"
    )
    instance_type: Literal["{write.instance_type}"] = Field("{write.instance_type}", alias="instanceType")
    {self.create_fields(write)}
'''

    def create_class(self, read: ReadDataClass) -> str:
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

    def create_fields(self, data_class: DataClass) -> str:
        field_lines = []
        for field in data_class.fields:
            field_line = f"{field.name}: {field.type_hint}"
            field_lines.append(field_line)
        return "\n    ".join(field_lines)
