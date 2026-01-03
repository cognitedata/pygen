from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator._types import OutputFormat
from cognite.pygen._generator.config import PygenSDKConfig
from cognite.pygen._pygen_model import APIClassFile, DataClassFile, PygenSDKModel

from .transformer import to_pygen_model


class Generator(ABC):
    format: ClassVar[OutputFormat]

    def __init__(self, data_model: DataModelResponseWithViews, config: PygenSDKConfig | None = None) -> None:
        self.data_model = data_model
        self.config = config or PygenSDKConfig()
        self.model = to_pygen_model(self.data_model, self.format, self.config)

    def generate(self) -> dict[Path, str]:
        model = self.model
        sdk: dict[Path, str] = {}

        # Generate data class files
        for data_class in model.data_classes:
            file_path = Path(f"data_classes/{data_class.filename}")
            sdk[file_path] = self.create_data_class_code(data_class)

        # Generate data_classes/__init__.py
        sdk[Path("data_classes/__init__.py")] = self.create_data_class_init_code(model)

        # Generate API class files
        for api_class in model.api_classes:
            file_path = Path(f"_api/{api_class.filename}")
            sdk[file_path] = self.create_api_class_code(api_class)

        # Generate _api/__init__.py
        sdk[Path("_api/__init__.py")] = self.create_api_init_code(model)

        # Generate client file
        sdk[Path("_client.py")] = self.create_client_code(model)

        # Generate top-level __init__.py
        sdk[Path("__init__.py")] = self.create_package_init_code(model)

        # Add instance_api module if not using pygen as dependency
        if self.format != "python" or not self.config.pygen_as_dependency:
            sdk.update(self.add_instance_api())

        return sdk

    @abstractmethod
    def create_data_class_code(self, data_class: DataClassFile) -> str:
        raise NotImplementedError()

    @abstractmethod
    def create_api_class_code(self, api_class: APIClassFile) -> str:
        raise NotImplementedError()

    @abstractmethod
    def create_data_class_init_code(self, model: PygenSDKModel) -> str:
        """Generate the data_classes/__init__.py file."""
        raise NotImplementedError()

    @abstractmethod
    def create_api_init_code(self, model: PygenSDKModel) -> str:
        """Generate the _api/__init__.py file."""
        raise NotImplementedError()

    @abstractmethod
    def create_client_code(self, model: PygenSDKModel) -> str:
        """Generate the client file."""
        raise NotImplementedError()

    @abstractmethod
    def create_package_init_code(self, model: PygenSDKModel) -> str:
        """Generate the top-level __init__.py file."""
        raise NotImplementedError()

    @abstractmethod
    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()
