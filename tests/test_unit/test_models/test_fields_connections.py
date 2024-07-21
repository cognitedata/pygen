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
            property_name, prop, node_class_by_view_id, edge_class_by_view_id, pygen_config, view.as_id(), "Field"
        )

    return factory


class TestConnections:
    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "ConnectionItemA",
                "outwards",
                "Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(default=None, repr=False)",
                id="Outwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemA",
                "otherDirect",
                "Union[ConnectionItemCNode, str, dm.NodeId, None] = "
                'Field(default=None, repr=False, alias="otherDirect")',
                id="Single direct relation",
            ),
            pytest.param(
                "ConnectionItemA",
                "selfDirect",
                'Union[ConnectionItemA, str, dm.NodeId, None] = Field(default=None, repr=False, alias="selfDirect")',
                id="Single direct relation to self",
            ),
            pytest.param(
                "ConnectionItemB",
                "inwards",
                "Optional[list[Union[ConnectionItemA, str, dm.NodeId]]] = Field(default=None, repr=False)",
                id="Inwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemE",
                "directNoSource",
                'Union[str, dm.NodeId, None] = Field(default=None, alias="directNoSource")',
                id="Single direct relation with no source",
            ),
            pytest.param(
                "ConnectionItemF",
                "outwardsMulti",
                'Optional[list[ConnectionEdgeA]] = Field(default=None, repr=False, alias="outwardsMulti")',
                id="Outwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemG",
                "inwardsMultiProperty",
                'Optional[list[ConnectionEdgeA]] = Field(default=None, repr=False, alias="inwardsMultiProperty")',
                id="Inwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemD",
                "outwardsSingle",
                "Union[ConnectionItemE, str, dm.NodeId, None] = "
                'Field(default=None, repr=False, alias="outwardsSingle")',
                id="Outwards SingleEdge no properties",
            ),
            pytest.param(
                "ConnectionItemE",
                "inwardsSingle",
                "Optional[list[Union[ConnectionItemD, str, dm.NodeId]]] = "
                'Field(default=None, repr=False, alias="inwardsSingle")',
                id="Inwards SingleEdge no properties",
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
                "ConnectionItemA",
                "outwards",
                "Optional[list[Union[ConnectionItemBWrite, str, dm.NodeId]]] = Field(default=None, repr=False)",
                id="Outwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemA",
                "otherDirect",
                'Union[str, dm.NodeId, None] = Field(default=None, alias="otherDirect")',
                id="Single Direct not writable",
            ),
            pytest.param(
                "ConnectionItemA",
                "selfDirect",
                "Union[ConnectionItemAWrite, str, dm.NodeId, None] = "
                'Field(default=None, repr=False, alias="selfDirect")',
                id="Single Direct to self",
            ),
            pytest.param(
                "ConnectionItemB",
                "inwards",
                "Optional[list[Union[ConnectionItemAWrite, str, dm.NodeId]]] = Field(default=None, repr=False)",
                id="Inwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemE",
                "directNoSource",
                'Union[str, dm.NodeId, None] = Field(default=None, alias="directNoSource")',
                id="Single direct relation with no source",
            ),
            pytest.param(
                "ConnectionItemF",
                "outwardsMulti",
                'Optional[list[ConnectionEdgeAWrite]] = Field(default=None, repr=False, alias="outwardsMulti")',
                id="Outwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemG",
                "inwardsMultiProperty",
                'Optional[list[ConnectionEdgeAWrite]] = Field(default=None, repr=False, alias="inwardsMultiProperty")',
                id="Inwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemD",
                "outwardsSingle",
                "Union[ConnectionItemEWrite, str, dm.NodeId, None] = "
                'Field(default=None, repr=False, alias="outwardsSingle")',
                id="Outwards SingleEdge no properties",
            ),
            pytest.param(
                "ConnectionItemE",
                "inwardsSingle",
                "Optional[list[Union[ConnectionItemDWrite, str, dm.NodeId]]] = "
                'Field(default=None, repr=False, alias="inwardsSingle")',
                id="Inwards SingleEdge no properties",
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
                "ConnectionItemA",
                "outwards",
                "Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False)",
                id="Outwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemA",
                "otherDirect",
                'Optional[ConnectionItemCNodeGraphQL] = Field(default=None, repr=False, alias="otherDirect")',
                id="Single Direct relation, not writable",
            ),
            pytest.param(
                "ConnectionItemA",
                "selfDirect",
                'Optional[ConnectionItemAGraphQL] = Field(default=None, repr=False, alias="selfDirect")',
                id="Single Direct to self",
            ),
            pytest.param(
                "ConnectionItemB",
                "inwards",
                "Optional[list[ConnectionItemAGraphQL]] = Field(default=None, repr=False)",
                id="Inwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemE",
                "directNoSource",
                'Optional[str] = Field(default=None, alias="directNoSource")',
                id="Single Direct, no source",
            ),
            pytest.param(
                "ConnectionItemF",
                "outwardsMulti",
                'Optional[list[ConnectionEdgeAGraphQL]] = Field(default=None, repr=False, alias="outwardsMulti")',
                id="Outwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemG",
                "inwardsMultiProperty",
                "Optional[list[ConnectionEdgeAGraphQL]] = "
                'Field(default=None, repr=False, alias="inwardsMultiProperty")',
                id="Inwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemD",
                "outwardsSingle",
                'Optional[ConnectionItemEGraphQL] = Field(default=None, repr=False, alias="outwardsSingle")',
                id="Outwards SingleEdge no properties",
            ),
            pytest.param(
                "ConnectionItemE",
                "inwardsSingle",
                'Optional[list[ConnectionItemDGraphQL]] = Field(default=None, repr=False, alias="inwardsSingle")',
                id="Inwards SingleEdge no properties",
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

    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "ConnectionItemA",
                "outwards",
                "[outward.as_write() if isinstance(outward, DomainModel) else outward "
                "for outward in self.outwards or []]",
                id="Outwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemA",
                "otherDirect",
                "self.other_direct.as_id()\nif isinstance(self.other_direct, DomainModel)\nelse self.other_direct",
                id="Direct is_list=False, not writable",
            ),
            pytest.param(
                "ConnectionItemA",
                "selfDirect",
                "self.self_direct.as_write()\nif isinstance(self.self_direct, DomainModel)\nelse self.self_direct",
                id="Direct to self is_list=False",
            ),
            pytest.param(
                "ConnectionItemB",
                "inwards",
                "[inward.as_write() if isinstance(inward, DomainModel) else inward "
                "for inward in self.inwards or []]",
                id="Inwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemE",
                "directNoSource",
                "self.direct_no_source",
                id="Direct is_list=False, no source",
            ),
            pytest.param(
                "ConnectionItemG",
                "inwardsMultiProperty",
                "[inwards_multi_property.as_write() for inwards_multi_property in self.inwards_multi_property or []]",
                id="Inwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemD",
                "outwardsSingle",
                "self.outwards_single.as_write()\nif isinstance(self.outwards_single, DomainModel)"
                "\nelse self.outwards_single",
                id="Outwards SingleEdge no properties",
            ),
            pytest.param(
                "ConnectionItemE",
                "inwardsSingle",
                "[inwards_single.as_write() if isinstance(inwards_single, DomainModel) else inwards_single "
                "for inwards_single in self.inwards_single or []]",
                id="Inwards SingleEdge no properties",
            ),
        ],
    )
    def test_as_write(
        self,
        view_ext_id: str,
        property_id: str,
        expected: str,
        omni_field_factory: Callable[[str, str], Field],
    ) -> None:
        # Arrange
        field_ = omni_field_factory(view_ext_id, property_id)

        # Act
        actual = field_.as_write()

        # Assert
        assert actual == expected

    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "ConnectionItemA",
                "outwards",
                "[outward.as_write() for outward in self.outwards or []]",
                id="Outwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemA",
                "otherDirect",
                "self.other_direct.as_write()\nif isinstance(self.other_direct, GraphQLCore)\nelse self.other_direct",
                id="Single Direct, not writable",
            ),
            pytest.param(
                "ConnectionItemA",
                "selfDirect",
                "self.self_direct.as_write()\nif isinstance(self.self_direct, GraphQLCore)\nelse self.self_direct",
                id="Single Direct to self.",
            ),
            pytest.param(
                "ConnectionItemB",
                "inwards",
                "[inward.as_write() for inward in self.inwards or []]",
                id="Inwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemE",
                "directNoSource",
                "self.direct_no_source",
                id="Direct is_list=False, no source",
            ),
            pytest.param(
                "ConnectionItemF",
                "outwardsMulti",
                "[outwards_multi.as_write() for outwards_multi in self.outwards_multi or []]",
                id="Outwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemG",
                "inwardsMultiProperty",
                "[inwards_multi_property.as_write() for inwards_multi_property in self.inwards_multi_property or []]",
                id="Inwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemD",
                "outwardsSingle",
                "self.outwards_single.as_write()\nif isinstance(self.outwards_single, GraphQLCore)"
                "\nelse self.outwards_single",
                id="Outwards SingleEdge no properties",
            ),
            pytest.param(
                "ConnectionItemE",
                "inwardsSingle",
                "[inwards_single.as_write() for inwards_single in self.inwards_single or []]",
                id="Inwards SingleEdge no properties",
            ),
        ],
    )
    def test_as_write_graphql(
        self,
        view_ext_id: str,
        property_id: str,
        expected: str,
        omni_field_factory: Callable[[str, str], Field],
    ) -> None:
        # Arrange
        field_ = omni_field_factory(view_ext_id, property_id)

        # Act
        actual = field_.as_write_graphql()

        # Assert
        assert actual == expected

    @pytest.mark.parametrize(
        "view_ext_id, property_id, expected",
        [
            pytest.param(
                "ConnectionItemA",
                "outwards",
                "[outward.as_read() for outward in self.outwards or []]",
                id="Outwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemA",
                "otherDirect",
                "self.other_direct.as_read()\nif isinstance(self.other_direct, GraphQLCore)\nelse self.other_direct",
                id="Single Direct, not writable",
            ),
            pytest.param(
                "ConnectionItemA",
                "selfDirect",
                "self.self_direct.as_read()\nif isinstance(self.self_direct, GraphQLCore)\nelse self.self_direct",
                id="Single Direct to self",
            ),
            pytest.param(
                "ConnectionItemB",
                "inwards",
                "[inward.as_read() for inward in self.inwards or []]",
                id="Inwards MultiEdge",
            ),
            pytest.param(
                "ConnectionItemE",
                "directNoSource",
                "self.direct_no_source",
                id="Direct is_list=False, no source",
            ),
            pytest.param(
                "ConnectionItemF",
                "outwardsMulti",
                "[outwards_multi.as_read() for outwards_multi in self.outwards_multi or []]",
                id="Outwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemG",
                "inwardsMultiProperty",
                "[inwards_multi_property.as_read() for inwards_multi_property in self.inwards_multi_property or []]",
                id="Inwards MultiEdge with properties",
            ),
            pytest.param(
                "ConnectionItemD",
                "outwardsSingle",
                "self.outwards_single.as_read()\nif isinstance(self.outwards_single, GraphQLCore)\nelse "
                "self.outwards_single",
                id="Outwards SingleEdge no properties",
            ),
            pytest.param(
                "ConnectionItemE",
                "inwardsSingle",
                "[inwards_single.as_read() for inwards_single in self.inwards_single or []]",
                id="Inwards SingleEdge no properties",
            ),
        ],
    )
    def test_as_read_graphql(
        self,
        view_ext_id: str,
        property_id: str,
        expected: str,
        omni_field_factory: Callable[[str, str], Field],
    ) -> None:
        # Arrange
        field_ = omni_field_factory(view_ext_id, property_id)

        # Act
        actual = field_.as_read_graphql()

        # Assert
        assert actual == expected
