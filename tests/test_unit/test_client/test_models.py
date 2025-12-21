from typing import Any, get_args

import pytest

from cognite.pygen._client.models import (
    Constraint,
    ConstraintDefinition,
    ContainerReference,
    ContainerRequest,
    ContainerResponse,
    DataModelReference,
    DataModelRequest,
    DataModelResponse,
    DataType,
    Index,
    IndexDefinition,
    PropertyTypeDefinition,
    SpaceReference,
    SpaceRequest,
    SpaceResponse,
    ViewPropertyDefinition,
    ViewReference,
    ViewRequest,
    ViewRequestProperty,
    ViewResponse,
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
        assert space_response.model_dump(by_alias=True) == example_space_resource


@pytest.fixture(scope="module")
def example_container_resource() -> dict[str, Any]:
    return {
        "space": "my_space",
        "externalId": "my_container",
        "name": "example_container",
        "description": "An example container for testing.",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "isGlobal": False,
        "usedFor": "node",
        "properties": {
            "name": {
                "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                "nullable": True,
                "immutable": False,
            }
        },
        "constraints": {},
        "indexes": {},
    }


class TestContainer:
    def test_container_response(self, example_container_resource: dict[str, Any]) -> None:
        container_response = ContainerResponse.model_validate(example_container_resource)
        assert isinstance(container_response.as_reference(), ContainerReference)
        assert isinstance(container_response.as_request(), ContainerRequest)
        assert container_response.model_dump(by_alias=True, exclude_unset=True) == example_container_resource

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


@pytest.fixture(scope="module")
def example_view_resource() -> dict[str, Any]:
    return {
        "space": "my_space",
        "externalId": "my_view",
        "version": "v1",
        "name": "example_view",
        "description": "An example view for testing.",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "isGlobal": False,
        "writable": True,
        "queryable": True,
        "usedFor": "node",
        "filter": None,
        "implements": [],
        "properties": {},
        "mappedContainers": [],
    }


class TestViewResponse:
    def test_view_response(self, example_view_resource: dict[str, Any]) -> None:
        view_response = ViewResponse.model_validate(example_view_resource)
        assert isinstance(view_response.as_reference(), ViewReference)
        assert isinstance(view_response.as_request(), ViewRequest)
        assert view_response.model_dump(by_alias=True) == example_view_resource


@pytest.fixture(scope="module")
def example_data_model_resource() -> dict[str, Any]:
    return {
        "space": "my_space",
        "externalId": "my_data_model",
        "version": "v1",
        "description": "An example data model for testing.",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "isGlobal": False,
        "views": [],
    }


class TestDataModel:
    def test_data_model_response(self, example_data_model_resource: dict[str, Any]) -> None:
        data_model_response = DataModelResponse.model_validate(example_data_model_resource)
        assert isinstance(data_model_response.as_reference(), DataModelReference)
        assert isinstance(data_model_response.as_request(), DataModelRequest)
        assert data_model_response.model_dump(by_alias=True) == example_data_model_resource
