from __future__ import annotations

from collections.abc import Callable

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.models.data_classes import DataClass
from cognite.pygen._core.models.fields import Field
from cognite.pygen.config import PygenConfig


@pytest.fixture(scope="session")
def data_classes_by_view_id(omni_views: dict[str, dm.View], pygen_config: PygenConfig) -> dict[dm.ViewId, DataClass]:
    return {
        v.as_id(): DataClass.from_view(v, DataClass.to_base_name(v), pygen_config.naming.data_class)
        for v in omni_views.values()
    }


@pytest.fixture(scope="session")
def omni_field_factory(
    omni_views: dict[str, dm.View],
    data_classes_by_view_id: dict[dm.ViewId, DataClass],
    pygen_config: PygenConfig,
) -> Callable[[str, str], Field]:
    def factory(view_ext_id: str, property_name: str) -> Field:
        view = omni_views[view_ext_id]
        prop = view.properties[property_name]
        return Field.from_property(property_name, prop, data_classes_by_view_id, pygen_config, view.as_id(), "Field")

    return factory


class TestConnections:
    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            (
                "ConnectionItemA",
                "outwards",
                "Union[list[ConnectionItemB], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)",
            )
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
