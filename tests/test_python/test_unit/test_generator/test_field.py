"""Tests for the Field class and related utilities in _pygen_model._field."""

import pytest

from cognite.pygen._pygen_model import (
    CDFPropertyType,
    Field,
    FilterType,
    WriteField,
    get_filter_type,
    get_type_hint,
    to_camel_case,
    to_snake_case,
)


class TestToSnakeCase:
    """Tests for the to_snake_case function."""

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("createdDate", "created_date"),
            ("MyClassName", "my_class_name"),
            ("already_snake", "already_snake"),
            ("URL", "url"),
            ("getHTTPResponse", "get_http_response"),
            ("simpleValue", "simple_value"),
            ("XMLParser", "xml_parser"),
            ("parseXML", "parse_xml"),
            ("name", "name"),
            ("ID", "id"),
        ],
    )
    def test_to_snake_case(self, input_name: str, expected: str) -> None:
        assert to_snake_case(input_name) == expected


class TestToCamelCase:
    """Tests for the to_camel_case function."""

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("created_date", "createdDate"),
            ("my_class_name", "myClassName"),
            ("alreadyCamel", "alreadyCamel"),
            ("simple", "simple"),
            ("get_http_response", "getHttpResponse"),
            ("name", "name"),
        ],
    )
    def test_to_camel_case(self, input_name: str, expected: str) -> None:
        assert to_camel_case(input_name) == expected


class TestGetTypeHint:
    """Tests for the get_type_hint function."""

    @pytest.mark.parametrize(
        "cdf_type,language,expected",
        [
            (CDFPropertyType.TEXT, "python", "str"),
            (CDFPropertyType.TEXT, "typescript", "string"),
            (CDFPropertyType.BOOLEAN, "python", "bool"),
            (CDFPropertyType.BOOLEAN, "typescript", "boolean"),
            (CDFPropertyType.INT32, "python", "int"),
            (CDFPropertyType.INT32, "typescript", "number"),
            (CDFPropertyType.INT64, "python", "int"),
            (CDFPropertyType.INT64, "typescript", "number"),
            (CDFPropertyType.FLOAT32, "python", "float"),
            (CDFPropertyType.FLOAT32, "typescript", "number"),
            (CDFPropertyType.FLOAT64, "python", "float"),
            (CDFPropertyType.FLOAT64, "typescript", "number"),
            (CDFPropertyType.TIMESTAMP, "python", "DateTime"),
            (CDFPropertyType.TIMESTAMP, "typescript", "Date"),
            (CDFPropertyType.DATE, "python", "Date"),
            (CDFPropertyType.DATE, "typescript", "Date"),
            (CDFPropertyType.JSON, "python", "dict[str, Any]"),
            (CDFPropertyType.JSON, "typescript", "Record<string, unknown>"),
            (CDFPropertyType.TIMESERIES, "python", "str"),
            (CDFPropertyType.TIMESERIES, "typescript", "string"),
            (CDFPropertyType.FILE, "python", "str"),
            (CDFPropertyType.FILE, "typescript", "string"),
            (CDFPropertyType.SEQUENCE, "python", "str"),
            (CDFPropertyType.SEQUENCE, "typescript", "string"),
            (CDFPropertyType.DIRECT, "python", "InstanceId"),
            (CDFPropertyType.DIRECT, "typescript", "InstanceId"),
        ],
    )
    def test_get_type_hint(self, cdf_type: CDFPropertyType, language: str, expected: str) -> None:
        assert get_type_hint(cdf_type, language) == expected  # type: ignore[arg-type]


class TestGetFilterType:
    """Tests for the get_filter_type function."""

    @pytest.mark.parametrize(
        "cdf_type,expected",
        [
            (CDFPropertyType.TEXT, FilterType.TEXT),
            (CDFPropertyType.BOOLEAN, FilterType.BOOLEAN),
            (CDFPropertyType.INT32, FilterType.INTEGER),
            (CDFPropertyType.INT64, FilterType.INTEGER),
            (CDFPropertyType.FLOAT32, FilterType.FLOAT),
            (CDFPropertyType.FLOAT64, FilterType.FLOAT),
            (CDFPropertyType.TIMESTAMP, FilterType.DATETIME),
            (CDFPropertyType.DATE, FilterType.DATE),
            (CDFPropertyType.JSON, FilterType.NONE),
            (CDFPropertyType.TIMESERIES, FilterType.TEXT),
            (CDFPropertyType.FILE, FilterType.TEXT),
            (CDFPropertyType.SEQUENCE, FilterType.TEXT),
            (CDFPropertyType.DIRECT, FilterType.DIRECT_RELATION),
            (CDFPropertyType.OBJECT, FilterType.NONE),
        ],
    )
    def test_get_filter_type(self, cdf_type: CDFPropertyType, expected: FilterType) -> None:
        assert get_filter_type(cdf_type) == expected


class TestField:
    """Tests for the Field class."""

    def test_simple_string_field(self) -> None:
        """Test a simple string field."""
        field = Field(
            cdf_prop_id="name",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=False,
        )
        assert field.python_name == "name"
        assert field.typescript_name == "name"
        assert field.python_type_hint == "str"
        assert field.typescript_type_hint == "string"
        assert field.filter_type == FilterType.TEXT
        assert field.is_filterable is True
        assert field.needs_alias is False

    def test_nullable_string_field(self) -> None:
        """Test a nullable string field."""
        field = Field(
            cdf_prop_id="description",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=True,
        )
        assert field.python_type_hint == "str | None"
        assert field.typescript_type_hint == "string | undefined"

    def test_camel_case_property(self) -> None:
        """Test a field with camelCase property ID."""
        field = Field(
            cdf_prop_id="createdDate",
            cdf_type=CDFPropertyType.DATE,
            is_nullable=False,
        )
        assert field.python_name == "created_date"
        assert field.typescript_name == "createdDate"
        assert field.needs_alias is True
        assert field.python_type_hint == "Date"
        assert field.typescript_type_hint == "Date"

    def test_list_field_python(self) -> None:
        """Test a list field generates correct Python type."""
        field = Field(
            cdf_prop_id="tags",
            cdf_type=CDFPropertyType.TEXT,
            is_list=True,
            is_nullable=True,
        )
        assert field.python_type_hint == "list[str] | None"
        assert field.is_filterable is False  # Lists are not filterable

    def test_list_field_typescript(self) -> None:
        """Test a list field generates correct TypeScript type."""
        field = Field(
            cdf_prop_id="tags",
            cdf_type=CDFPropertyType.TEXT,
            is_list=True,
            is_nullable=True,
        )
        assert field.typescript_type_hint == "string[] | undefined"

    def test_required_list_field(self) -> None:
        """Test a required list field."""
        field = Field(
            cdf_prop_id="values",
            cdf_type=CDFPropertyType.FLOAT64,
            is_list=True,
            is_nullable=False,
        )
        assert field.python_type_hint == "list[float]"
        assert field.typescript_type_hint == "number[]"

    def test_direct_relation_field(self) -> None:
        """Test a direct relation field."""
        field = Field(
            cdf_prop_id="category",
            cdf_type=CDFPropertyType.DIRECT,
            is_nullable=True,
            direct_relation_source="CategoryNode/v1",
        )
        assert field.python_type_hint == "InstanceId | None"
        assert field.typescript_type_hint == "InstanceId | undefined"
        assert field.filter_type == FilterType.DIRECT_RELATION
        assert field.direct_relation_source == "CategoryNode/v1"

    def test_timestamp_field(self) -> None:
        """Test a timestamp field."""
        field = Field(
            cdf_prop_id="updatedTimestamp",
            cdf_type=CDFPropertyType.TIMESTAMP,
            is_nullable=True,
        )
        assert field.python_name == "updated_timestamp"
        assert field.python_type_hint == "DateTime | None"
        assert field.typescript_type_hint == "Date | undefined"
        assert field.filter_type == FilterType.DATETIME

    def test_json_field(self) -> None:
        """Test a JSON field."""
        field = Field(
            cdf_prop_id="metadata",
            cdf_type=CDFPropertyType.JSON,
            is_nullable=True,
        )
        assert field.python_type_hint == "dict[str, Any] | None"
        assert field.typescript_type_hint == "Record<string, unknown> | undefined"
        assert field.is_filterable is False

    def test_field_with_description(self) -> None:
        """Test a field with description."""
        field = Field(
            cdf_prop_id="name",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=False,
            description="The name of the product",
        )
        assert field.description == "The name of the product"

    def test_python_field_definition_simple(self) -> None:
        """Test Python field definition for a simple field."""
        field = Field(
            cdf_prop_id="name",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=False,
        )
        assert field.python_field_definition == "name: str"

    def test_python_field_definition_nullable(self) -> None:
        """Test Python field definition for a nullable field."""
        field = Field(
            cdf_prop_id="description",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=True,
        )
        assert field.python_field_definition == "description: str | None = Field(None)"

    def test_python_field_definition_with_alias(self) -> None:
        """Test Python field definition with alias."""
        field = Field(
            cdf_prop_id="createdDate",
            cdf_type=CDFPropertyType.DATE,
            is_nullable=False,
        )
        assert field.python_field_definition == 'created_date: Date = Field(alias="createdDate")'

    def test_python_field_definition_nullable_with_alias(self) -> None:
        """Test Python field definition for nullable field with alias."""
        field = Field(
            cdf_prop_id="updatedTimestamp",
            cdf_type=CDFPropertyType.TIMESTAMP,
            is_nullable=True,
        )
        assert (
            field.python_field_definition
            == 'updated_timestamp: DateTime | None = Field(None, alias="updatedTimestamp")'
        )

    def test_typescript_field_definition_simple(self) -> None:
        """Test TypeScript field definition for a simple field."""
        field = Field(
            cdf_prop_id="name",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=False,
        )
        assert field.typescript_field_definition == "name: string;"

    def test_typescript_field_definition_optional(self) -> None:
        """Test TypeScript field definition for an optional field."""
        field = Field(
            cdf_prop_id="description",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=True,
        )
        assert field.typescript_field_definition == "description?: string | undefined;"


class TestWriteField:
    """Tests for the WriteField class."""

    def test_write_field_direct_relation(self) -> None:
        """Test WriteField for direct relation accepts tuples."""
        field = WriteField(
            cdf_prop_id="category",
            cdf_type=CDFPropertyType.DIRECT,
            is_nullable=True,
        )
        assert field.python_type_hint == "InstanceId | tuple[str, str] | None"
        assert field.typescript_type_hint == "InstanceId | [string, string] | undefined"

    def test_write_field_required_direct_relation(self) -> None:
        """Test WriteField for required direct relation."""
        field = WriteField(
            cdf_prop_id="parent",
            cdf_type=CDFPropertyType.DIRECT,
            is_nullable=False,
        )
        assert field.python_type_hint == "InstanceId | tuple[str, str]"
        assert field.typescript_type_hint == "InstanceId | [string, string]"

    def test_write_field_list_direct_relation(self) -> None:
        """Test WriteField for list of direct relations."""
        field = WriteField(
            cdf_prop_id="children",
            cdf_type=CDFPropertyType.DIRECT,
            is_list=True,
            is_nullable=True,
        )
        assert field.python_type_hint == "list[InstanceId | tuple[str, str]] | None"
        assert field.typescript_type_hint == "(InstanceId | [string, string])[] | undefined"

    def test_write_field_non_direct_uses_base(self) -> None:
        """Test WriteField for non-direct types uses base type hints."""
        field = WriteField(
            cdf_prop_id="name",
            cdf_type=CDFPropertyType.TEXT,
            is_nullable=False,
        )
        assert field.python_type_hint == "str"
        assert field.typescript_type_hint == "string"


class TestFieldSerialization:
    """Tests for Field model serialization/deserialization."""

    def test_field_model_dump(self) -> None:
        """Test that Field can be serialized to dict."""
        field = Field(
            cdf_prop_id="createdDate",
            cdf_type=CDFPropertyType.DATE,
            is_nullable=False,
            description="The creation date",
        )
        dumped = field.model_dump()
        assert dumped["cdf_prop_id"] == "createdDate"
        assert dumped["cdf_type"] == "date"
        assert dumped["is_nullable"] is False
        assert dumped["description"] == "The creation date"
        # Computed fields should be included
        assert dumped["python_name"] == "created_date"
        assert dumped["typescript_name"] == "createdDate"
        assert dumped["python_type_hint"] == "Date"
        assert dumped["typescript_type_hint"] == "Date"

    def test_field_model_validate(self) -> None:
        """Test that Field can be deserialized from dict."""
        data = {
            "cdf_prop_id": "name",
            "cdf_type": "text",
            "is_nullable": False,
        }
        field = Field.model_validate(data)
        assert field.cdf_prop_id == "name"
        assert field.cdf_type == CDFPropertyType.TEXT
        assert field.is_nullable is False
