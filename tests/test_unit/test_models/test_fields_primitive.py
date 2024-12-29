from __future__ import annotations

from collections.abc import Callable

import pytest

from cognite.pygen._core.models.fields import Field


class TestConnections:
    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "PrimitiveNullable",
                "text",
                "Optional[str] = None",
                id="Nullable string",
            ),
            pytest.param(
                "PrimitiveNullable",
                "boolean",
                "Optional[bool] = None",
                id="Nullable boolean",
            ),
            pytest.param(
                "PrimitiveNullable",
                "float64",
                'Optional[float] = Field(None, alias="float64")',
                id="Nullable float",
            ),
            pytest.param(
                "PrimitiveNullable",
                "int64",
                'Optional[int] = Field(None, alias="int64")',
                id="Nullable int",
            ),
            pytest.param(
                "PrimitiveNullable",
                "timestamp",
                "Optional[datetime.datetime] = None",
                id="Nullable timestamp",
            ),
            pytest.param(
                "PrimitiveNullable",
                "date",
                "Optional[datetime.date] = None",
                id="Nullable date",
            ),
            pytest.param(
                "PrimitiveNullable",
                "json",
                'Optional[dict] = Field(None, alias="json")',
                id="Nullable json",
            ),
        ],
    )
    def test_as_read_type_hint(
        self,
        view_ext_id: str,
        property_id: str,
        expected: str,
        omni_field_factory: Callable[[str, str], Field],
    ) -> None:
        # Arrange
        field_ = omni_field_factory(view_ext_id, property_id)

        # Act
        actual = field_.as_read_type_hint()

        # Assert
        assert actual == expected

    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "PrimitiveNullable",
                "text",
                "Optional[str] = None",
                id="Nullable string",
            ),
            pytest.param(
                "PrimitiveNullable",
                "boolean",
                "Optional[bool] = None",
                id="Nullable boolean",
            ),
            pytest.param(
                "PrimitiveNullable",
                "float64",
                'Optional[float] = Field(None, alias="float64")',
                id="Nullable float",
            ),
            pytest.param(
                "PrimitiveNullable",
                "int64",
                'Optional[int] = Field(None, alias="int64")',
                id="Nullable int",
            ),
            pytest.param(
                "PrimitiveNullable",
                "timestamp",
                "Optional[datetime.datetime] = None",
                id="Nullable timestamp",
            ),
            pytest.param(
                "PrimitiveNullable",
                "date",
                "Optional[datetime.date] = None",
                id="Nullable date",
            ),
            pytest.param(
                "PrimitiveNullable",
                "json",
                'Optional[dict] = Field(None, alias="json")',
                id="Nullable json",
            ),
        ],
    )
    def test_as_write_type_hint(
        self,
        view_ext_id: str,
        property_id: str,
        expected: str,
        omni_field_factory: Callable[[str, str], Field],
    ) -> None:
        # Arrange
        field_ = omni_field_factory(view_ext_id, property_id)

        # Act
        actual = field_.as_write_type_hint()

        # Assert
        assert actual == expected

    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "PrimitiveNullable",
                "text",
                "Optional[str] = None",
                id="Nullable string",
            ),
            pytest.param(
                "PrimitiveNullable",
                "boolean",
                "Optional[bool] = None",
                id="Nullable boolean",
            ),
            pytest.param(
                "PrimitiveNullable",
                "float64",
                'Optional[float] = Field(None, alias="float64")',
                id="Nullable float",
            ),
            pytest.param(
                "PrimitiveNullable",
                "int64",
                'Optional[int] = Field(None, alias="int64")',
                id="Nullable int",
            ),
            pytest.param(
                "PrimitiveNullable",
                "timestamp",
                "Optional[datetime.datetime] = None",
                id="Nullable timestamp",
            ),
            pytest.param(
                "PrimitiveNullable",
                "date",
                "Optional[datetime.date] = None",
                id="Nullable date",
            ),
            pytest.param(
                "PrimitiveNullable",
                "json",
                'Optional[dict] = Field(None, alias="json")',
                id="Nullable json",
            ),
        ],
    )
    def test_as_graphql_type_hint(
        self,
        view_ext_id: str,
        property_id: str,
        expected: str,
        omni_field_factory: Callable[[str, str], Field],
    ) -> None:
        # Arrange
        field_ = omni_field_factory(view_ext_id, property_id)

        # Act
        actual = field_.as_graphql_type_hint()

        # Assert
        assert actual == expected
