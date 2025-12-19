from __future__ import annotations

import warnings
from typing import Any, Literal, overload, TYPE_CHECKING

from cognite.client import data_modeling as dm

from wind_turbine.data_classes._core.constants import DEFAULT_INSTANCE_SPACE

if TYPE_CHECKING:
    from wind_turbine.data_classes._core.base import DomainModel, T_DomainModel


def as_node_id(value: dm.DirectRelationReference) -> dm.NodeId:
    return dm.NodeId(space=value.space, external_id=value.external_id)


@overload
def as_direct_relation_reference(
    value: dm.DirectRelationReference | dm.NodeId | tuple[str, str] | dict[str, Any] | Any | None,
    return_none: Literal[False],
) -> dm.DirectRelationReference: ...


@overload
def as_direct_relation_reference(
    value: dm.DirectRelationReference | dm.NodeId | tuple[str, str] | dict[str, Any] | Any | None,
    return_none: Literal[True] = True,
) -> dm.DirectRelationReference | None: ...


def as_direct_relation_reference(
    value: dm.DirectRelationReference | dm.NodeId | tuple[str, str] | dict[str, Any] | Any | None,
    return_none: bool = True,
) -> dm.DirectRelationReference | None:
    if (value is None and return_none) or isinstance(value, dm.DirectRelationReference):
        return value
    elif isinstance(value, dm.NodeId):
        return dm.DirectRelationReference(space=value.space, external_id=value.external_id)
    elif isinstance(value, tuple):
        return dm.DirectRelationReference(space=value[0], external_id=value[1])
    elif isinstance(value, dict) and "space" in value and "externalId" in value:
        return dm.DirectRelationReference(space=value["space"], external_id=value["externalId"])
    elif isinstance(value, dict) and "space" in value and "external_id" in value:
        return dm.DirectRelationReference(space=value["space"], external_id=value["external_id"])
    elif value is not None and hasattr(value, "space") and hasattr(value, "external_id"):
        return dm.DirectRelationReference(space=value.space, external_id=value.external_id)
    raise TypeError(f"Expected DirectRelationReference, NodeId or tuple, got {type(value)}")


# Any is to make mypy happy, while the rest is a hint of what the function expects
def as_instance_dict_id(value: str | dm.NodeId | tuple[str, str] | dm.DirectRelationReference | Any) -> dict[str, str]:
    if isinstance(value, str):
        return {"space": DEFAULT_INSTANCE_SPACE, "externalId": value}
    if isinstance(value, dm.NodeId):
        return {"space": value.space, "externalId": value.external_id}
    elif isinstance(value, tuple) and is_tuple_id(value):
        return {"space": value[0], "externalId": value[1]}
    elif isinstance(value, dm.DirectRelationReference):
        return {"space": value.space, "externalId": value.external_id}
    elif isinstance(value, dict) and "space" in value and "externalId" in value:
        return {"space": value["space"], "externalId": value["externalId"]}
    elif value is not None and hasattr(value, "space") and hasattr(value, "external_id"):
        return {"space": value.space, "externalId": value.external_id}  # type: ignore[union-attr]
    raise TypeError(f"Expected str, NodeId, tuple or DirectRelationReference, got {type(value)}")


def is_tuple_id(value: Any) -> bool:
    return isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], str) and isinstance(value[1], str)


def as_pygen_node_id(value: DomainModel | dm.NodeId | str) -> dm.NodeId | str:
    if isinstance(value, str):
        return value
    elif value.space == DEFAULT_INSTANCE_SPACE:
        return value.external_id
    elif isinstance(value, dm.NodeId):
        return value
    return value.as_id()


def are_nodes_equal(node1: DomainModel | str | dm.NodeId, node2: DomainModel | str | dm.NodeId) -> bool:
    if isinstance(node1, str | dm.NodeId):
        node1_id = node1
    else:
        node1_id = node1.as_id() if node1.space != DEFAULT_INSTANCE_SPACE else node1.external_id
    if isinstance(node2, str | dm.NodeId):
        node2_id = node2
    else:
        node2_id = node2.as_id() if node2.space != DEFAULT_INSTANCE_SPACE else node2.external_id
    return node1_id == node2_id


def select_best_node(
    node1: T_DomainModel | str | dm.NodeId, node2: T_DomainModel | str | dm.NodeId
) -> T_DomainModel | str | dm.NodeId:
    if isinstance(node1, DomainModel):
        return node1  # type: ignore[return-value]
    elif isinstance(node2, DomainModel):
        return node2  # type: ignore[return-value]
    else:
        return node1


def parse_single_connection(value: Any, field_name: str | None) -> Any:
    if isinstance(value, list):
        if len(value) > 1:
            warnings.warn(
                f"Expected a single connection {field_name or 'MISSING'}, "
                f"got {len(value)} connections. Using the first one."
            )
        value = value[0]
    if isinstance(value, dict) and len(value) == 2 and "space" in value and "externalId" in value:
        if value["space"] == DEFAULT_INSTANCE_SPACE:
            return value["externalId"]
        return dm.NodeId(space=value["space"], external_id=value["externalId"])
    return value
