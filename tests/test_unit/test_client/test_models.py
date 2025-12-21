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
        ref = space_response.as_reference()
        assert isinstance(ref, SpaceReference)
        assert str(ref) == "my_space"
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
            },
            "value": {
                "type": {"type": "float64"},
                "nullable": True,
                "immutable": False,
            },
        },
        "constraints": {
            "unique_name": {
                "constraintType": "uniqueness",
                "properties": ["name"],
            },
            "requires_other": {
                "constraintType": "requires",
                "require": {"space": "my_space", "externalId": "other_container", "type": "container"},
            },
        },
        "indexes": {
            "name_index": {
                "indexType": "btree",
                "properties": ["name"],
                "cursorable": True,
            },
            "name_inverted": {
                "indexType": "inverted",
                "properties": ["name"],
            },
        },
    }


class TestContainer:
    def test_container_response(self, example_container_resource: dict[str, Any]) -> None:
        container_response = ContainerResponse.model_validate(example_container_resource)
        ref = container_response.as_reference()
        assert isinstance(ref, ContainerReference)
        assert str(ref) == "my_space:my_container"
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

    def test_view_request_used_containers(self) -> None:
        """Test that ViewRequest.used_containers returns all container references from core properties."""
        view_request_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "filter": None,
            "properties": {
                "name": {
                    "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                    "containerPropertyIdentifier": "name",
                },
                "value": {
                    "container": {"space": "my_space", "externalId": "other_container", "type": "container"},
                    "containerPropertyIdentifier": "value",
                },
                "edges": {
                    "connectionType": "multi_edge_connection",
                    "source": {"space": "my_space", "externalId": "edge_view", "version": "v1", "type": "view"},
                    "type": {"space": "my_space", "externalId": "edge_type"},
                    "direction": "outwards",
                },
            },
        }
        view_request = ViewRequest.model_validate(view_request_data)
        used_containers = view_request.used_containers
        assert len(used_containers) == 2
        container_ids = {c.external_id for c in used_containers}
        assert container_ids == {"my_container", "other_container"}

    def test_view_request_core_property_with_source_serialization(self) -> None:
        """Test that ViewCorePropertyRequest serializes source correctly."""
        view_request_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "filter": None,
            "properties": {
                "related": {
                    "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                    "containerPropertyIdentifier": "related",
                    "source": {"space": "my_space", "externalId": "target_view", "version": "v1", "type": "view"},
                },
            },
        }
        view_request = ViewRequest.model_validate(view_request_data)
        dumped = view_request.model_dump(by_alias=True)
        assert dumped["properties"]["related"]["source"]["type"] == "view"
        assert dumped["properties"]["related"]["source"]["externalId"] == "target_view"


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
        "implements": [
            {"space": "parent_space", "externalId": "parent_view", "version": "v1", "type": "view"},
        ],
        "properties": {
            "name": {
                "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                "containerPropertyIdentifier": "name",
                "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                "nullable": True,
                "immutable": False,
                "constraintState": {"nullability": "current"},
            },
            "related": {
                "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                "containerPropertyIdentifier": "related",
                "type": {
                    "type": "direct",
                    "container": {"space": "my_space", "externalId": "my_container"},
                    "source": {"space": "my_space", "externalId": "target_view", "version": "v1"},
                },
                "nullable": True,
                "immutable": False,
                "constraintState": {"nullability": "current"},
            },
            "edges": {
                "connectionType": "multi_edge_connection",
                "source": {"space": "my_space", "externalId": "edge_view", "version": "v1", "type": "view"},
                "type": {"space": "my_space", "externalId": "edge_type"},
                "direction": "outwards",
            },
            "reverse": {
                "connectionType": "single_reverse_direct_relation",
                "source": {"space": "my_space", "externalId": "reverse_view", "version": "v1", "type": "view"},
                "through": {
                    "source": {"space": "my_space", "externalId": "my_container", "type": "container"},
                    "identifier": "related",
                },
                "targetsList": False,
            },
        },
        "mappedContainers": [{"space": "my_space", "externalId": "my_container", "type": "container"}],
    }


class TestViewResponse:
    def test_view_response(self, example_view_resource: dict[str, Any]) -> None:
        view_response = ViewResponse.model_validate(example_view_resource)
        ref = view_response.as_reference()
        assert isinstance(ref, ViewReference)
        assert str(ref) == "my_space:my_view(version=v1)"
        assert isinstance(view_response.as_request(), ViewRequest)

    def test_view_response_with_none_implements(self) -> None:
        """Test that view response with None implements serializes correctly."""
        view_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {},
            "mappedContainers": [],
        }
        view_response = ViewResponse.model_validate(view_data)
        dumped = view_response.model_dump(by_alias=True)
        assert dumped["implements"] is None

    def test_view_response_as_request_with_direct_node_relation_source(self) -> None:
        """Test as_request method with DirectNodeRelation that has a source."""
        view_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "related": {
                    "container": {"space": "my_space", "externalId": "my_container", "type": "container"},
                    "containerPropertyIdentifier": "related",
                    "type": {
                        "type": "direct",
                        "container": {"space": "my_space", "externalId": "my_container"},
                        "source": {"space": "my_space", "externalId": "target_view", "version": "v1"},
                    },
                    "nullable": True,
                    "immutable": False,
                    "constraintState": {"nullability": "current"},
                },
            },
            "mappedContainers": [{"space": "my_space", "externalId": "my_container", "type": "container"}],
        }
        view_response = ViewResponse.model_validate(view_data)
        view_request = view_response.as_request()
        # The source should be moved from DirectNodeRelation to the ViewCorePropertyRequest
        assert view_request.properties["related"].source is not None
        assert view_request.properties["related"].source.space == "my_space"
        assert view_request.properties["related"].source.external_id == "target_view"

    def test_view_response_as_request_with_multi_reverse_direct_relation(self) -> None:
        """Test as_request method with MultiReverseDirectRelationPropertyResponse."""
        view_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "reverse_multi": {
                    "connectionType": "multi_reverse_direct_relation",
                    "source": {"space": "my_space", "externalId": "reverse_view", "version": "v1", "type": "view"},
                    "through": {
                        "source": {"space": "my_space", "externalId": "my_container", "type": "container"},
                        "identifier": "related",
                    },
                    "targetsList": False,
                },
            },
            "mappedContainers": [],
        }
        view_response = ViewResponse.model_validate(view_data)
        view_request = view_response.as_request()
        assert isinstance(view_request, ViewRequest)

    def test_view_response_as_request_with_single_edge_property(self) -> None:
        """Test as_request method with SingleEdgeProperty."""
        view_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "single_edge": {
                    "connectionType": "single_edge_connection",
                    "source": {"space": "my_space", "externalId": "edge_view", "version": "v1", "type": "view"},
                    "type": {"space": "my_space", "externalId": "edge_type"},
                    "direction": "outwards",
                },
            },
            "mappedContainers": [],
        }
        view_response = ViewResponse.model_validate(view_data)
        view_request = view_response.as_request()
        assert isinstance(view_request, ViewRequest)

    def test_view_response_with_reverse_direct_relation_through_view(self) -> None:
        """Test ReverseDirectRelationProperty with ViewDirectReference through (not ContainerDirectReference)."""
        view_data = {
            "space": "my_space",
            "externalId": "my_view",
            "version": "v1",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "filter": None,
            "implements": None,
            "properties": {
                "reverse_via_view": {
                    "connectionType": "single_reverse_direct_relation",
                    "source": {"space": "my_space", "externalId": "reverse_view", "version": "v1", "type": "view"},
                    "through": {
                        "source": {"space": "my_space", "externalId": "through_view", "version": "v1", "type": "view"},
                        "identifier": "related",
                    },
                    "targetsList": False,
                },
            },
            "mappedContainers": [],
        }
        view_response = ViewResponse.model_validate(view_data)
        dumped = view_response.model_dump(by_alias=True)
        # Verify that the through source is serialized with "view" type
        assert dumped["properties"]["reverse_via_view"]["through"]["source"]["type"] == "view"


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
        "views": [
            {"space": "my_space", "externalId": "my_view", "version": "v1", "type": "view"},
        ],
    }


class TestDataModel:
    def test_data_model_response(self, example_data_model_resource: dict[str, Any]) -> None:
        data_model_response = DataModelResponse.model_validate(example_data_model_resource)
        ref = data_model_response.as_reference()
        assert isinstance(ref, DataModelReference)
        assert str(ref) == "my_space:my_data_model(version=v1)"
        assert isinstance(data_model_response.as_request(), DataModelRequest)
        assert data_model_response.model_dump(by_alias=True) == example_data_model_resource

    def test_data_model_response_with_none_views(self) -> None:
        """Test that data model response with None views serializes correctly."""
        data_model_data = {
            "space": "my_space",
            "externalId": "my_data_model",
            "version": "v1",
            "description": "An example data model for testing.",
            "createdTime": 1625247600000,
            "lastUpdatedTime": 1625247600000,
            "isGlobal": False,
            "views": None,
        }
        data_model_response = DataModelResponse.model_validate(data_model_data)
        dumped = data_model_response.model_dump(by_alias=True)
        assert dumped["views"] is None
