from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator._types import OutputFormat
from cognite.pygen._generator.config import PygenSDKConfig, create_internal_config

from .transformer import to_pygen_model


class Generator(ABC):
    format: ClassVar[OutputFormat]

    def __init__(self, data_model: DataModelResponseWithViews, config: PygenSDKConfig | None = None) -> None:
        self.data_model = data_model
        self.config = create_internal_config(config or PygenSDKConfig(), self.format)
        self.model = to_pygen_model(self.data_model, self.format, self.config)

    @abstractmethod
    def generate(self) -> dict[Path, str]:
        raise NotImplementedError()
