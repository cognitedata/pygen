from typing import Any

import pytest


@pytest.fixture(scope="session")
def example_space_resource() -> dict[str, Any]:
    return {
        "space": "my_space",
        "name": "example_space",
        "description": "An example space for testing.",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "isGlobal": False,
    }


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
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
                    "source": {"space": "my_space", "externalId": "target_view", "version": "v1", "type": "view"},
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


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def example_data_model_with_views_resource(
    example_data_model_resource: dict[str, Any], example_view_resource: dict[str, Any]
) -> dict[str, Any]:
    copy = example_data_model_resource.copy()
    copy["views"] = [example_view_resource]
    return copy
