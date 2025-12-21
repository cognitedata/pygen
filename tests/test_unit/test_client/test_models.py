from typing import Any, get_args

import pytest

from cognite.pygen._client.models import (
    Constraint,
    ConstraintDefinition,
    DataType,
    Index,
    IndexDefinition,
    PropertyTypeDefinition,
    SpaceReference,
    SpaceRequest,
    SpaceResponse,
    ViewPropertyDefinition,
    ViewRequestProperty,
    ViewResponseProperty,
)
from cognite.pygen._utils.collection import humanize_collection
from tests.utils import get_concrete_subclasses


@pytest.fixture(scope="module")
def example_space_resource() -> dict[str, Any]:
    return {
        "space": "my_space",
        "name": "example_space",
        "description": "An example space for testing.",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "isGlobal": False,
    }


class TestSpace:
    def test_space_response(self, example_space_resource: dict[str, Any]) -> None:
        space_response = SpaceResponse.model_validate(example_space_resource)
        assert isinstance(space_response.as_reference(), SpaceReference)
        assert isinstance(space_response.as_request(), SpaceRequest)


class TestContainer:
    def test_all_indexes_are_in_union(self) -> None:
        all_indices = get_concrete_subclasses(IndexDefinition, exclude_direct_abc_inheritance=True)
        all_union_indices = get_args(Index.__args__[0])
        missing = set(all_indices) - set(all_union_indices)
        assert not missing, (
            f"The following IndexDefinition subclasses are "
            f"missing from the Index union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_all_constraints_are_in_union(self) -> None:
        all_constraints = get_concrete_subclasses(ConstraintDefinition, exclude_direct_abc_inheritance=True)
        all_union_constraints = get_args(Constraint.__args__[0])
        missing = set(all_constraints) - set(all_union_constraints)
        assert not missing, (
            f"The following ConstraintDefinition subclasses are "
            f"missing from the Constraint union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_all_property_types_are_in_union(self) -> None:
        all_property_types = get_concrete_subclasses(PropertyTypeDefinition, exclude_direct_abc_inheritance=True)
        all_union_property_types = get_args(DataType.__args__[0])
        missing = set(all_property_types) - set(all_union_property_types)
        assert not missing, (
            f"The following PropertyTypeDefinition subclasses are "
            f"missing from the DataType union: {humanize_collection([cls.__name__ for cls in missing])}"
        )


class TestView:
    def test_all_view_properties_are_in_union(self) -> None:
        all_view_properties = get_concrete_subclasses(ViewPropertyDefinition, exclude_direct_abc_inheritance=True)
        all_response_properties = get_args(ViewResponseProperty.__args__[0])
        all_request_properties = get_args(ViewRequestProperty.__args__[0])
        missing = set(all_view_properties) - set(all_response_properties) - set(all_request_properties)
        assert not missing, (
            "The following ViewPropertyDefinition subclasses are "
            "missing from the ViewResponseProperty/ViewRequestProperty union:"
            f" {humanize_collection([cls.__name__ for cls in missing])}"
        )
