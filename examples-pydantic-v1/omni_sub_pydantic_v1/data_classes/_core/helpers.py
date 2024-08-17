from __future__ import annotations

from cognite.client import data_modeling as dm
from .base import DomainModel, T_DomainModel


def as_node_id(value: dm.DirectRelationReference) -> dm.NodeId:
    return dm.NodeId(space=value.space, external_id=value.external_id)


def as_direct_relation_reference(
    value: dm.DirectRelationReference | dm.NodeId | tuple[str, str] | None
) -> dm.DirectRelationReference | None:
    if value is None or isinstance(value, dm.DirectRelationReference):
        return value
    if isinstance(value, dm.NodeId):
        return dm.DirectRelationReference(space=value.space, external_id=value.external_id)
    if isinstance(value, tuple):
        return dm.DirectRelationReference(space=value[0], external_id=value[1])
    raise TypeError(f"Expected DirectRelationReference, NodeId or tuple, got {type(value)}")


def as_pygen_node_id(value: DomainModel | dm.NodeId) -> dm.NodeId:
    if isinstance(value, dm.NodeId):
        return value
    return value.as_id()


def are_nodes_equal(node1: DomainModel | dm.NodeId, node2: DomainModel | dm.NodeId) -> bool:
    if isinstance(node1, dm.NodeId):
        node1_id = node1
    else:
        node1_id = node1.as_id()
    if isinstance(node2, dm.NodeId):
        node2_id = node2
    else:
        node2_id = node2.as_id()
    return node1_id == node2_id


def select_best_node(node1: T_DomainModel | dm.NodeId, node2: T_DomainModel | dm.NodeId) -> T_DomainModel | dm.NodeId:
    if isinstance(node1, DomainModel):
        return node1  # type: ignore[return-value]
    elif isinstance(node2, DomainModel):
        return node2  # type: ignore[return-value]
    else:
        return node1
