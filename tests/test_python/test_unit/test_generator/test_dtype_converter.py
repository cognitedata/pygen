from collections.abc import Mapping
from typing import Any

import pytest

from cognite.pygen._client.models import (
    BooleanProperty,
    ContainerReference,
    Float32Property,
    Int64Property,
    MultiEdgeProperty,
    NodeReference,
    TextProperty,
    TimestampProperty,
    ViewCorePropertyResponse,
    ViewReference,
    ViewResponseProperty,
)
from cognite.pygen._generator._types import OutputFormat
from cognite.pygen._generator.dtype_converter import (
    _PYTHON_FILTER_NAMES,
    PythonDataTypeConverter,
    TypeScriptDataTypeConverter,
    get_converter_by_format,
)
from cognite.pygen._python.instance_api.models.dtype_filters import DataTypeFilter
from tests.test_python.utils import get_concrete_subclasses

_DEFAULT_ARGS: Mapping[str, Any] = {
    "container": ContainerReference(space="default_space", external_id="default_container"),
    "container_property_identifier": "default_property",
    "constraint_state": {},
}
_DEFAULT_EDGE_ARGS: Mapping[str, Any] = {
    "source": ViewReference(space="default_space", external_id="default_source", version="v1"),
    "type": NodeReference(space="default_space", external_id="default_type"),
}


class TestPythonDataTypeConverter:
    @pytest.mark.parametrize(
        "prop, expected",
        [
            (ViewCorePropertyResponse(type=TextProperty(list=False), nullable=False, **_DEFAULT_ARGS), "str"),
            (ViewCorePropertyResponse(type=Int64Property(list=False), nullable=True, **_DEFAULT_ARGS), "int | None"),
            (ViewCorePropertyResponse(type=Float32Property(list=True), nullable=False, **_DEFAULT_ARGS), "list[float]"),
            (
                ViewCorePropertyResponse(type=BooleanProperty(list=True), nullable=True, **_DEFAULT_ARGS),
                "list[bool] | None",
            ),
            (MultiEdgeProperty(**_DEFAULT_EDGE_ARGS), "Any"),
        ],
    )
    def test_create_write_type_hint(self, prop: ViewResponseProperty, expected: str) -> None:
        converter = PythonDataTypeConverter("write")
        assert converter.create_type_hint(prop) == expected

    @pytest.mark.parametrize(
        "prop, expected",
        [
            (ViewCorePropertyResponse(type=TextProperty(list=False), nullable=False, **_DEFAULT_ARGS), "str"),
            (ViewCorePropertyResponse(type=Int64Property(list=False), nullable=True, **_DEFAULT_ARGS), "int | None"),
            (
                ViewCorePropertyResponse(
                    type=Int64Property(list=False), auto_increment=True, nullable=False, **_DEFAULT_ARGS
                ),
                "int",
            ),
            (ViewCorePropertyResponse(type=Float32Property(list=True), nullable=False, **_DEFAULT_ARGS), "list[float]"),
            (
                ViewCorePropertyResponse(type=TimestampProperty(list=True), nullable=True, **_DEFAULT_ARGS),
                "list[int] | None",
            ),
            (MultiEdgeProperty(**_DEFAULT_EDGE_ARGS), "Any"),
        ],
    )
    def test_create_read_type_hint(self, prop: ViewResponseProperty, expected: str) -> None:
        converter = PythonDataTypeConverter("read")
        assert converter.create_type_hint(prop) == expected

    @pytest.mark.parametrize(
        "prop, expected",
        [
            (ViewCorePropertyResponse(type=TextProperty(list=False), nullable=False, **_DEFAULT_ARGS), "TextFilter"),
            (ViewCorePropertyResponse(type=Int64Property(list=False), nullable=True, **_DEFAULT_ARGS), "IntegerFilter"),
            (ViewCorePropertyResponse(type=Float32Property(list=True), nullable=False, **_DEFAULT_ARGS), None),
            (ViewCorePropertyResponse(type=BooleanProperty(list=True), nullable=True, **_DEFAULT_ARGS), None),
            (MultiEdgeProperty(**_DEFAULT_EDGE_ARGS), None),
        ],
    )
    def test_get_filter_name(self, prop: ViewResponseProperty, expected: str) -> None:
        converter = PythonDataTypeConverter("read")
        assert converter.get_filter_name(prop) == expected

    def test_filter_name_mapping_complete(self) -> None:
        existing_filter_name = {cls_.__name__ for cls_ in get_concrete_subclasses(DataTypeFilter)}
        target_filter_names = set(_PYTHON_FILTER_NAMES.values())
        assert existing_filter_name == target_filter_names


class TestTypeScriptDataTypeConverter:
    @pytest.mark.parametrize(
        "prop, expected",
        [
            (ViewCorePropertyResponse(type=TextProperty(list=False), nullable=False, **_DEFAULT_ARGS), "string"),
            (
                ViewCorePropertyResponse(type=Int64Property(list=False), nullable=True, **_DEFAULT_ARGS),
                "number | undefined",
            ),
            (
                ViewCorePropertyResponse(type=Float32Property(list=True), nullable=False, **_DEFAULT_ARGS),
                "readonly number[]",
            ),
            (
                ViewCorePropertyResponse(type=BooleanProperty(list=True), nullable=True, **_DEFAULT_ARGS),
                "readonly boolean[] | undefined",
            ),
            (MultiEdgeProperty(**_DEFAULT_EDGE_ARGS), "unknown"),
        ],
    )
    def test_create_write_type_hint(self, prop: ViewResponseProperty, expected: str) -> None:
        converter = TypeScriptDataTypeConverter("write")
        assert converter.create_type_hint(prop) == expected

    @pytest.mark.parametrize(
        "prop, expected",
        [
            (ViewCorePropertyResponse(type=TextProperty(list=False), nullable=False, **_DEFAULT_ARGS), "string"),
            (
                ViewCorePropertyResponse(type=Int64Property(list=False), nullable=True, **_DEFAULT_ARGS),
                "number | undefined",
            ),
            (
                ViewCorePropertyResponse(
                    type=Int64Property(list=False), auto_increment=True, nullable=False, **_DEFAULT_ARGS
                ),
                "number",
            ),
            (
                ViewCorePropertyResponse(type=Float32Property(list=True), nullable=False, **_DEFAULT_ARGS),
                "readonly number[]",
            ),
            (
                ViewCorePropertyResponse(type=TimestampProperty(list=True), nullable=True, **_DEFAULT_ARGS),
                "readonly Date[] | undefined",
            ),
            (MultiEdgeProperty(**_DEFAULT_EDGE_ARGS), "unknown"),
        ],
    )
    def test_create_read_type_hint(self, prop: ViewResponseProperty, expected: str) -> None:
        converter = TypeScriptDataTypeConverter("read")
        assert converter.create_type_hint(prop) == expected

    @pytest.mark.parametrize(
        "prop, expected",
        [
            (ViewCorePropertyResponse(type=TextProperty(list=False), nullable=False, **_DEFAULT_ARGS), "TextFilter"),
            (ViewCorePropertyResponse(type=Int64Property(list=False), nullable=True, **_DEFAULT_ARGS), "IntegerFilter"),
            (ViewCorePropertyResponse(type=Float32Property(list=True), nullable=False, **_DEFAULT_ARGS), None),
            (ViewCorePropertyResponse(type=BooleanProperty(list=True), nullable=True, **_DEFAULT_ARGS), None),
            (MultiEdgeProperty(**_DEFAULT_EDGE_ARGS), None),
        ],
    )
    def test_get_filter_name(self, prop: ViewResponseProperty, expected: str | None) -> None:
        converter = TypeScriptDataTypeConverter("read")
        assert converter.get_filter_name(prop) == expected


class TestGetConverterByFormat:
    @pytest.mark.parametrize(
        "format, expected_cls",
        [
            ("python", PythonDataTypeConverter),
            ("typescript", TypeScriptDataTypeConverter),
        ],
    )
    def test_get_converter_by_format(self, format: OutputFormat, expected_cls: type) -> None:
        converter = get_converter_by_format(format, "read")
        assert isinstance(converter, expected_cls)

    def test_get_converter_by_format_invalid(self) -> None:
        with pytest.raises(ValueError, match="Unsupported output format: java"):
            get_converter_by_format("java", "read")  # type: ignore[arg-type]
