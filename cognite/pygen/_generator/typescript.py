from pathlib import Path

from cognite.pygen._pygen_model import APIClassFile, DataClassFile

from .generator import Generator


class TypeScriptGenerator(Generator):
    format = "typescript"

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        raise NotImplementedError()

    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()
