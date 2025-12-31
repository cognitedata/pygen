"""Field representation for code generation.

This module defines the Field class which represents a property from a CDF view
for code generation purposes. It includes type mappings for Python and TypeScript.
"""

from enum import Enum
from typing import Literal

from pydantic import Field as PydanticField
from pydantic import computed_field

from ._model import CodeModel


class CDFPropertyType(str, Enum):
    """Enumeration of all CDF property types.

    These correspond to the types in the CDF Data Modeling API.
    """

    # Primitive types
    TEXT = "text"
    BOOLEAN = "boolean"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    TIMESTAMP = "timestamp"
    DATE = "date"
    JSON = "json"

    # CDF reference types
    TIMESERIES = "timeseries"
    FILE = "file"
    SEQUENCE = "sequence"

    # Relation types
    DIRECT = "direct"

    # Object type (legacy)
    OBJECT = "object"

    # Enum type
    ENUM = "enum"


class FilterType(str, Enum):
    """Filter types for filtering operations on fields.

    These map to the filter classes in the instance_api.
    """

    TEXT = "TextFilter"
    INTEGER = "IntegerFilter"
    FLOAT = "FloatFilter"
    BOOLEAN = "BooleanFilter"
    DATETIME = "DateTimeFilter"
    DATE = "DateFilter"
    DIRECT_RELATION = "DirectRelationFilter"
    NONE = "None"  # For types that don't support filtering


# Type mappings from CDF property type to Python type hints
_PYTHON_TYPE_MAP: dict[CDFPropertyType, str] = {
    CDFPropertyType.TEXT: "str",
    CDFPropertyType.BOOLEAN: "bool",
    CDFPropertyType.INT32: "int",
    CDFPropertyType.INT64: "int",
    CDFPropertyType.FLOAT32: "float",
    CDFPropertyType.FLOAT64: "float",
    CDFPropertyType.TIMESTAMP: "DateTime",
    CDFPropertyType.DATE: "Date",
    CDFPropertyType.JSON: "dict[str, Any]",
    CDFPropertyType.TIMESERIES: "str",
    CDFPropertyType.FILE: "str",
    CDFPropertyType.SEQUENCE: "str",
    CDFPropertyType.DIRECT: "InstanceId",
    CDFPropertyType.OBJECT: "dict[str, Any]",
    CDFPropertyType.ENUM: "str",  # Enum values are strings
}

# Type mappings from CDF property type to TypeScript type hints
_TYPESCRIPT_TYPE_MAP: dict[CDFPropertyType, str] = {
    CDFPropertyType.TEXT: "string",
    CDFPropertyType.BOOLEAN: "boolean",
    CDFPropertyType.INT32: "number",
    CDFPropertyType.INT64: "number",
    CDFPropertyType.FLOAT32: "number",
    CDFPropertyType.FLOAT64: "number",
    CDFPropertyType.TIMESTAMP: "Date",
    CDFPropertyType.DATE: "Date",
    CDFPropertyType.JSON: "Record<string, unknown>",
    CDFPropertyType.TIMESERIES: "string",
    CDFPropertyType.FILE: "string",
    CDFPropertyType.SEQUENCE: "string",
    CDFPropertyType.DIRECT: "InstanceId",
    CDFPropertyType.OBJECT: "Record<string, unknown>",
    CDFPropertyType.ENUM: "string",
}

# Filter type mappings from CDF property type to filter class
_FILTER_TYPE_MAP: dict[CDFPropertyType, FilterType] = {
    CDFPropertyType.TEXT: FilterType.TEXT,
    CDFPropertyType.BOOLEAN: FilterType.BOOLEAN,
    CDFPropertyType.INT32: FilterType.INTEGER,
    CDFPropertyType.INT64: FilterType.INTEGER,
    CDFPropertyType.FLOAT32: FilterType.FLOAT,
    CDFPropertyType.FLOAT64: FilterType.FLOAT,
    CDFPropertyType.TIMESTAMP: FilterType.DATETIME,
    CDFPropertyType.DATE: FilterType.DATE,
    CDFPropertyType.JSON: FilterType.NONE,
    CDFPropertyType.TIMESERIES: FilterType.TEXT,
    CDFPropertyType.FILE: FilterType.TEXT,
    CDFPropertyType.SEQUENCE: FilterType.TEXT,
    CDFPropertyType.DIRECT: FilterType.DIRECT_RELATION,
    CDFPropertyType.OBJECT: FilterType.NONE,
    CDFPropertyType.ENUM: FilterType.TEXT,
}


def to_snake_case(name: str) -> str:
    """Convert a camelCase or PascalCase name to snake_case.

    Args:
        name: The name to convert.

    Returns:
        The snake_case version of the name.

    Examples:
        >>> to_snake_case("createdDate")
        'created_date'
        >>> to_snake_case("MyClassName")
        'my_class_name'
        >>> to_snake_case("already_snake")
        'already_snake'
    """
    result: list[str] = []
    for i, char in enumerate(name):
        if char.isupper():
            if i > 0 and not name[i - 1].isupper():
                result.append("_")
            elif i > 0 and i + 1 < len(name) and name[i - 1].isupper() and not name[i + 1].isupper():
                result.append("_")
            result.append(char.lower())
        else:
            result.append(char)
    return "".join(result)


def to_camel_case(name: str) -> str:
    """Convert a snake_case name to camelCase.

    Args:
        name: The name to convert.

    Returns:
        The camelCase version of the name.

    Examples:
        >>> to_camel_case("created_date")
        'createdDate'
        >>> to_camel_case("my_class_name")
        'myClassName'
        >>> to_camel_case("alreadyCamel")
        'alreadyCamel'
    """
    if "_" not in name:
        return name
    parts = name.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class Field(CodeModel):
    """Represents a property from a CDF view for code generation.

    This class contains all the information needed to generate code for a property
    in both Python and TypeScript SDKs.

    Attributes:
        cdf_prop_id: The CDF property identifier (original name from the API).
        cdf_type: The CDF property type.
        is_list: Whether this field is a list type.
        is_nullable: Whether this field can be None/null.
        description: Optional description of the property.
        default_value: Optional default value for the property.
        direct_relation_source: For direct relations, the source view reference.
    """

    cdf_prop_id: str
    cdf_type: CDFPropertyType
    is_list: bool = False
    is_nullable: bool = True
    description: str | None = None
    default_value: str | None = None
    direct_relation_source: str | None = PydanticField(
        None,
        description="For direct relations, the target view external_id/version",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def python_name(self) -> str:
        """Get the Python-appropriate property name (snake_case)."""
        return to_snake_case(self.cdf_prop_id)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def typescript_name(self) -> str:
        """Get the TypeScript-appropriate property name (camelCase)."""
        return to_camel_case(self.cdf_prop_id)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def python_type_hint(self) -> str:
        """Get the Python type hint for this field.

        Returns:
            The Python type hint string, including list and nullable modifiers.
        """
        base_type = _PYTHON_TYPE_MAP.get(self.cdf_type, "Any")
        return self._build_type_hint(base_type, python=True)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def typescript_type_hint(self) -> str:
        """Get the TypeScript type hint for this field.

        Returns:
            The TypeScript type hint string, including array and nullable modifiers.
        """
        base_type = _TYPESCRIPT_TYPE_MAP.get(self.cdf_type, "unknown")
        return self._build_type_hint(base_type, python=False)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def filter_type(self) -> FilterType:
        """Get the filter type for this field."""
        return _FILTER_TYPE_MAP.get(self.cdf_type, FilterType.NONE)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_filterable(self) -> bool:
        """Check if this field supports filtering."""
        return self.filter_type != FilterType.NONE and not self.is_list

    @computed_field  # type: ignore[prop-decorator]
    @property
    def needs_alias(self) -> bool:
        """Check if this field needs an alias (Python name != CDF property ID)."""
        return self.python_name != self.cdf_prop_id

    def _build_type_hint(self, base_type: str, python: bool) -> str:
        """Build the complete type hint with list and nullable modifiers.

        Args:
            base_type: The base type string.
            python: True for Python syntax, False for TypeScript.

        Returns:
            The complete type hint string.
        """
        if self.is_list:
            if python:
                type_hint = f"list[{base_type}]"
            else:
                type_hint = f"{base_type}[]"
        else:
            type_hint = base_type

        if self.is_nullable:
            if python:
                type_hint = f"{type_hint} | None"
            else:
                type_hint = f"{type_hint} | undefined"

        return type_hint

    @property
    def python_field_definition(self) -> str:
        """Generate the Python field definition for this property.

        Returns:
            A string like 'name: str' or 'created_date: Date = Field(alias="createdDate")'
        """
        parts: list[str] = []
        parts.append(f"{self.python_name}: {self.python_type_hint}")

        field_args: list[str] = []
        if self.needs_alias:
            field_args.append(f'alias="{self.cdf_prop_id}"')
        if self.default_value is not None:
            field_args.append(f"default={self.default_value}")
        elif self.is_nullable:
            field_args.insert(0, "None")

        if field_args:
            parts.append(f" = Field({', '.join(field_args)})")

        return "".join(parts)

    @property
    def typescript_field_definition(self) -> str:
        """Generate the TypeScript field definition for this property.

        Returns:
            A string like 'name: string;' or 'createdDate?: Date;'
        """
        optional = "?" if self.is_nullable else ""
        return f"{self.typescript_name}{optional}: {self.typescript_type_hint};"


# Write-specific field variants for direct relations
class WriteField(Field):
    """Field variant for write classes.

    In write classes, direct relations can accept either InstanceId or tuple[str, str].
    """

    @computed_field  # type: ignore[prop-decorator]
    @property
    def python_type_hint(self) -> str:
        """Get the Python type hint for write classes.

        Direct relations in write classes can accept tuples for convenience.
        """
        if self.cdf_type == CDFPropertyType.DIRECT:
            base = "InstanceId | tuple[str, str]"
            if self.is_list:
                base = f"list[{base}]"
            if self.is_nullable:
                base = f"{base} | None"
            return base
        return super().python_type_hint

    @computed_field  # type: ignore[prop-decorator]
    @property
    def typescript_type_hint(self) -> str:
        """Get the TypeScript type hint for write classes.

        Direct relations in write classes can accept tuples for convenience.
        """
        if self.cdf_type == CDFPropertyType.DIRECT:
            base = "InstanceId | [string, string]"
            if self.is_list:
                base = f"({base})[]"
            if self.is_nullable:
                base = f"{base} | undefined"
            return base
        return super().typescript_type_hint


Language = Literal["python", "typescript"]


def get_type_hint(cdf_type: CDFPropertyType, language: Language) -> str:
    """Get the type hint for a CDF property type in the specified language.

    Args:
        cdf_type: The CDF property type.
        language: The target language ('python' or 'typescript').

    Returns:
        The type hint string for the specified language.
    """
    if language == "python":
        return _PYTHON_TYPE_MAP.get(cdf_type, "Any")
    else:
        return _TYPESCRIPT_TYPE_MAP.get(cdf_type, "unknown")


def get_filter_type(cdf_type: CDFPropertyType) -> FilterType:
    """Get the filter type for a CDF property type.

    Args:
        cdf_type: The CDF property type.

    Returns:
        The FilterType enum value for the specified CDF type.
    """
    return _FILTER_TYPE_MAP.get(cdf_type, FilterType.NONE)
