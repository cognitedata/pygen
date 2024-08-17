from __future__ import annotations

from collections.abc import Callable

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.models.data_classes import EdgeDataClass, NodeDataClass
from cognite.pygen._core.models.fields import Field
from cognite.pygen.config import PygenConfig
from tests.utils import to_data_class_by_view_id


@pytest.fixture(scope="session")
def omni_data_classes_by_view_id(
    omni_views: dict[str, dm.View], pygen_config: PygenConfig
) -> tuple[dict[dm.ViewId, NodeDataClass], dict[dm.ViewId, EdgeDataClass]]:
    return to_data_class_by_view_id(omni_views.values(), pygen_config)


@pytest.fixture(scope="session")
def omni_field_factory(
    omni_views: dict[str, dm.View],
    omni_data_classes_by_view_id,
    pygen_config: PygenConfig,
) -> Callable[[str, str], Field]:
    node_class_by_view_id, edge_class_by_view_id = omni_data_classes_by_view_id

    def factory(view_ext_id: str, property_name: str) -> Field:
        view = omni_views[view_ext_id]
        prop = view.properties[property_name]
        return Field.from_property(
            property_name, prop, node_class_by_view_id, edge_class_by_view_id, pygen_config, view.as_id(), "Field", True
        )

    return factory
