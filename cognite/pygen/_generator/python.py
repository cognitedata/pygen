from pathlib import Path

from cognite.pygen._pygen_model import APIClassFile, DataClassFile

from .generator import Generator


class PythonGenerator(Generator):
    format = "python"

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        generator = PythonDataClassGenerator(data_class)
        parts: list[str] = [
            generator.create_import_statements(data_class),
            generator.generate_read_class(data_class),
            generator.generate_read_list_class(data_class),
        ]
        if data_class.write:
            parts.append(generator.generate_write_class(data_class))
        parts.append(generator.generate_filter_class(data_class))
        return "\n\n".join(parts)

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        raise NotImplementedError()

    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()


class PythonDataClassGenerator:
    def __init__(self, data_class: DataClassFile) -> None:
        self.data_class = data_class

    def create_import_statements(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()

    def generate_read_class(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()

    def generate_read_list_class(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()

    def generate_write_class(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()

    def generate_filter_class(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()
