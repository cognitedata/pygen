from collections.abc import Iterable

from cognite.client import data_modeling as dm

from cognite.pygen._core.models import DataClass, EdgeDataClass, NodeDataClass
from cognite.pygen.config import PygenConfig


def to_data_class_by_view_id(
    views: Iterable[dm.View], pygen_config: PygenConfig
) -> tuple[dict[dm.ViewId, NodeDataClass], dict[dm.ViewId, EdgeDataClass]]:
    node_by_view_id: dict[dm.ViewId, NodeDataClass] = {}
    edge_by_view_id: dict[dm.ViewId, EdgeDataClass] = {}
    for view in views:
        if view.used_for == "all":
            base_name = DataClass.to_base_name(view)
            node_by_view_id[view.as_id()] = NodeDataClass.from_view(
                view, f"{base_name}Node", "node", pygen_config.naming.data_class
            )
            edge_by_view_id[view.as_id()] = EdgeDataClass.from_view(
                view, f"{base_name}Edge", "edge", pygen_config.naming.data_class
            )
        elif view.used_for == "node":
            node_by_view_id[view.as_id()] = NodeDataClass.from_view(
                view, DataClass.to_base_name(view), "node", pygen_config.naming.data_class
            )
        elif view.used_for == "edge":
            edge_by_view_id[view.as_id()] = EdgeDataClass.from_view(
                view, DataClass.to_base_name(view), "edge", pygen_config.naming.data_class
            )

    return node_by_view_id, edge_by_view_id
