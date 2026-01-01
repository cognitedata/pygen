from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import ClassVar, Literal

from cognite.pygen._client.models import (
    BooleanProperty,
    DateProperty,
    DirectNodeRelation,
    EnumProperty,
    FileCDFExternalIdReference,
    Float32Property,
    Float64Property,
    Int32Property,
    Int64Property,
    JSONProperty,
    ListablePropertyTypeDefinition,
    SequenceCDFExternalIdReference,
    TextProperty,
    TimeseriesCDFExternalIdReference,
    TimestampProperty,
    ViewCorePropertyResponse,
    ViewResponseProperty,
)

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


# Python type mappings for core property types
_PYTHON_PRIMITIVE_TYPES: dict[type, str] = {
    TextProperty: "str",
    BooleanProperty: "bool",
    Int32Property: "int",
    Int64Property: "int",
    Float32Property: "float",
    Float64Property: "float",
    DateProperty: "Date",
    TimestampProperty: "int",
    JSONProperty: "dict",
    TimeseriesCDFExternalIdReference: "str",
    FileCDFExternalIdReference: "str",
    SequenceCDFExternalIdReference: "str",
    EnumProperty: "str",
    DirectNodeRelation: "InstanceId",
}

# Python filter name mappings
_PYTHON_FILTER_NAMES: dict[type, str] = {
    TextProperty: "TextFilter",
    BooleanProperty: "BooleanFilter",
    Int32Property: "IntegerFilter",
    Int64Property: "IntegerFilter",
    Float32Property: "FloatFilter",
    Float64Property: "FloatFilter",
    DateProperty: "DateFilter",
    TimestampProperty: "DateTimeFilter",
    DirectNodeRelation: "DirectRelationFilter",
}


class PythonDataTypeConverter(DataTypeConverter):
    output_format = "python"

    def create_type_hint(self, prop: ViewResponseProperty) -> str:
        if not isinstance(prop, ViewCorePropertyResponse):
            return "Any"

        data_type = prop.type
        type_class = type(data_type)
        base_type = _PYTHON_PRIMITIVE_TYPES.get(type_class, "Any")

        if isinstance(data_type, ListablePropertyTypeDefinition) and data_type.list:
            type_hint = f"list[{base_type}]"
        else:
            type_hint = base_type

        nullable = prop.nullable if prop.nullable is not None else False
        if nullable:
            type_hint = f"{type_hint} | None"

        return type_hint

    def get_filter_name(self, prop: ViewResponseProperty) -> str | None:
        if not isinstance(prop, ViewCorePropertyResponse):
            return None

        data_type = prop.type
        if isinstance(data_type, ListablePropertyTypeDefinition) and data_type.list:
            return None

        type_class = type(data_type)
        return _PYTHON_FILTER_NAMES.get(type_class)


# TypeScript type mappings for core property types
_TYPESCRIPT_PRIMITIVE_TYPES: dict[type, str] = {
    TextProperty: "string",
    BooleanProperty: "boolean",
    Int32Property: "number",
    Int64Property: "number",
    Float32Property: "number",
    Float64Property: "number",
    DateProperty: "Date",
    TimestampProperty: "Date",
    JSONProperty: "object",
    TimeseriesCDFExternalIdReference: "string",
    FileCDFExternalIdReference: "string",
    SequenceCDFExternalIdReference: "string",
    EnumProperty: "string",
    DirectNodeRelation: "InstanceId",
}

# TypeScript filter name mappings
_TYPESCRIPT_FILTER_NAMES: dict[type, str] = {
    TextProperty: "TextFilter",
    BooleanProperty: "BooleanFilter",
    Int32Property: "IntegerFilter",
    Int64Property: "IntegerFilter",
    Float32Property: "FloatFilter",
    Float64Property: "FloatFilter",
    DateProperty: "DateFilter",
    TimestampProperty: "DateTimeFilter",
    DirectNodeRelation: "DirectRelationFilter",
}


class TypeScriptDataTypeConverter(DataTypeConverter):
    output_format = "typescript"

    def create_type_hint(self, prop: ViewResponseProperty) -> str:
        if not isinstance(prop, ViewCorePropertyResponse):
            return "unknown"

        data_type = prop.type
        type_class = type(data_type)
        base_type = _TYPESCRIPT_PRIMITIVE_TYPES.get(type_class, "unknown")

        if isinstance(data_type, ListablePropertyTypeDefinition) and data_type.list:
            type_hint = f"readonly {base_type}[]"
        else:
            type_hint = base_type

        nullable = prop.nullable if prop.nullable is not None else False
        if nullable:
            type_hint = f"{type_hint} | undefined"

        return type_hint

    def get_filter_name(self, prop: ViewResponseProperty) -> str | None:
        if not isinstance(prop, ViewCorePropertyResponse):
            return None

        # List properties do not have filters
        if isinstance(prop.type, ListablePropertyTypeDefinition) and prop.type.list:
            return None

        return _TYPESCRIPT_FILTER_NAMES.get(type(prop.type))


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
