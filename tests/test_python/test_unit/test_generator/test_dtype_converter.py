from typing import Any, ClassVar

import pytest

from cognite.pygen._client.models import (
    BooleanProperty,
    ContainerReference,
    Float32Property,
    Int64Property,
    TextProperty,
    TimestampProperty,
    ViewCorePropertyResponse,
    ViewResponseProperty,
)
from cognite.pygen._generator.dtype_converter import PythonDataTypeConverter, TypeScriptDataTypeConverter


class TestPythonDataTypeConverter:
    _DEFAULT_ARGS: ClassVar[dict[str, Any]] = {
        "container": ContainerReference(space="default_space", external_id="default_container"),
        "container_property_identifier": "default_property",
        "constraint_state": {},
    }

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
        ],
    )
    def test_get_filter_name(self, prop: ViewResponseProperty, expected: str) -> None:
        converter = PythonDataTypeConverter("read")
        assert converter.get_filter_name(prop) == expected


class TestTypeScriptDataTypeConverter:
    _DEFAULT_ARGS: ClassVar[dict[str, Any]] = {
        "container": ContainerReference(space="default_space", external_id="default_container"),
        "container_property_identifier": "default_property",
        "constraint_state": {},
    }

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
        ],
    )
    def test_get_filter_name(self, prop: ViewResponseProperty, expected: str | None) -> None:
        converter = TypeScriptDataTypeConverter("read")
        assert converter.get_filter_name(prop) == expected
