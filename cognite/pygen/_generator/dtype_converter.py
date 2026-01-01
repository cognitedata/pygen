from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import ClassVar, Literal

from cognite.pygen._client.models import ViewResponseProperty

from ._types import OutputFormat


class DataTypeConverter(ABC):
    """Abstract base class for data type converters."""

    output_format: ClassVar[OutputFormat]

    def __init__(self, context: Literal["read", "write"]):
        self.context = context

    @abstractmethod
    def create_type_hint(self, prop: ViewResponseProperty) -> str:
        """Creates a type hint for the given property.

        Args:
            prop (ViewResponseProperty): The property to create a type hint for.

        Returns:
            str: The type hint as a string.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_filter_name(self, prop: ViewResponseProperty) -> str | None:
        """Gets the filter name for the given property.

        Args:
            prop (ViewResponseProperty): The property to get the filter name for.
        Returns:
            str | None: The filter name, or None if not applicable.
        raise NotImplementedError()
        """
        raise NotImplementedError()


class PythonDataTypeConverter(DataTypeConverter):
    output_format = "python"

    def create_type_hint(self, prop: ViewResponseProperty) -> str:
        raise NotImplementedError()

    def get_filter_name(self, prop: ViewResponseProperty) -> str | None:
        raise NotImplementedError()


class TypeScriptDataTypeConverter(DataTypeConverter):
    output_format = "typescript"

    def create_type_hint(self, prop: ViewResponseProperty) -> str:
        raise NotImplementedError()

    def get_filter_name(self, prop: ViewResponseProperty) -> str | None:
        raise NotImplementedError()


def get_converter_by_format(format: OutputFormat, context: Literal["read", "write"]) -> DataTypeConverter:
    """Get the data type converter class for the given output format.

    Args:
        format (OutputFormat): The output format.
    Returns:
        type[DataTypeConverter]: The data type converter class.
    """
    converter_cls = _CONVERTER_BY_FORMAT.get(format)
    if not converter_cls:
        raise ValueError(f"Unsupported output format: {format}")
    return converter_cls(context)


_CONVERTER_BY_FORMAT: Mapping[str, type[DataTypeConverter]] = {
    _converter.output_format: _converter  # type: ignore[type-abstract]
    for _converter in DataTypeConverter.__subclasses__()
}
