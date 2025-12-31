from abc import ABC
from pathlib import Path
from typing import ClassVar


class Generator(ABC):
    format: ClassVar[str]

    def generate(self) -> dict[str, Path]:
        raise NotImplementedError()
