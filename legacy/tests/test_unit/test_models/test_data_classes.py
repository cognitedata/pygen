import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._core.models import EdgeDataClass, NodeDataClass
from cognite.pygen._warnings import (
    NameCollisionDataClassNameWarning,
    NameCollisionFileNameWarning,
)
from cognite.pygen.config import PygenConfig
from tests.omni_constants import OMNI_SPACE
from tests.utils import to_data_class_by_view_id


class TestDataClasses:
    def test_dependency_named_Field(self, pygen_config: PygenConfig) -> None:
        # Arrange
        views = dm.ViewList.load(_DEPENDENCY_NAMED_FIELD).data

        node_class_by_view_id, edge_class_by_view_id = to_data_class_by_view_id(views, pygen_config)

        # Act
        for data_class, view in zip(node_class_by_view_id.values(), views, strict=False):
            data_class.update_fields(
                view.properties,
                node_class_by_view_id,
                edge_class_by_view_id,
                list(views),
                True,
                {},
                {},
                pygen_config,
            )

        # Assert
        country_data_class = node_class_by_view_id[dm.ViewId("ReportedBugs", "Country", "c93d79472dd1cb")]

        assert country_data_class.pydantic_field == "Field"
        assert country_data_class.import_pydantic_field == "import pydantic"
        wrong_field = [field.name for field in country_data_class if field.pydantic_field != "pydantic.Field"]
        assert not wrong_field, f"Wrong pydantic field for the following field(s): {wrong_field}"

    def test_has_date_field(self, omni_multi_api_generator: MultiAPIGenerator) -> None:
        # Arrange
        api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
            dm.ViewId(OMNI_SPACE, "PrimitiveRequired", "1")
        ]

        # Assert
        assert api_generator.data_class.has_primitive_field_of_type(dm.Date)

    def test_variable_and_variable_list_named_the_same(self, pygen_config: PygenConfig) -> None:
        view = dm.View.load(_SINGULAR_PLURAL_EQUALS)
        base_name = NodeDataClass.to_base_name(view)
        node_class = NodeDataClass.from_view(view, base_name, "node", pygen_config.naming.data_class)

        assert node_class.variable != node_class.variable_list

    def test_data_class_is_time(self, pygen_config: PygenConfig) -> None:
        # Arrange
        view = dm.View.load(_VIEW_WITH_TIME_PROPERTY_RAW)

        # Act
        data_class = NodeDataClass.from_view(
            view, NodeDataClass.to_base_name(view), "node", pygen_config.naming.data_class
        )
        data_class.update_fields(
            view.properties,
            {
                view.as_id(): data_class,
            },
            {},
            [view],
            True,
            {},
            {},
            pygen_config,
        )

        # Assert
        assert data_class.has_primitive_field_of_type(dm.TimeSeriesReference) is True

    @pytest.mark.parametrize(
        "name, expected_name",
        [
            ("DomainModel", "DomainModel_"),
            ("DomainModelList", "DomainModelList_"),
            ("DomainModelWrite", "DomainModelWrite_"),
        ],
    )
    def test_data_class_from_view_expected_warning(
        self, name: str, expected_name: str, pygen_config: PygenConfig
    ) -> None:
        # Arrange
        view = dm.View(
            space="dummy",
            external_id=name,
            version="dummy",
            properties={},
            name=name,
            last_updated_time=1,
            created_time=1,
            is_global=False,
            description="missing",
            used_for="node",
            writable=True,
            filter=None,
            implements=None,
        )

        # Act
        with pytest.warns(NameCollisionDataClassNameWarning):
            actual = NodeDataClass.from_view(
                view, NodeDataClass.to_base_name(view), "node", pygen_config.naming.data_class
            )
        # Assert
        assert actual.read_name == expected_name

    @pytest.mark.parametrize(
        "name, expected_name",
        [
            ("Core", "_core_"),
        ],
    )
    def test_data_class_from_view_expected_warning_file_name(
        self, name: str, expected_name: str, pygen_config: PygenConfig
    ) -> None:
        # Arrange
        view = dm.View(
            space="dummy",
            external_id=name,
            version="dummy",
            properties={},
            name=name,
            last_updated_time=1,
            created_time=1,
            is_global=False,
            description="missing",
            used_for="node",
            writable=True,
            filter=None,
            implements=None,
        )

        # Act
        with pytest.warns(NameCollisionFileNameWarning):
            actual = NodeDataClass.from_view(
                view, NodeDataClass.to_base_name(view), "node", pygen_config.naming.data_class
            )
        # Assert
        assert actual.file_name == expected_name

    def test_edge_class_init_import(self) -> None:
        # Arrange
        view = dm.View.load(_CDF3D_ENTITY_YAML)
        expected = [
            "Cdf3dEntityEdge",
            "Cdf3dEntityEdgeFields",
            "Cdf3dEntityEdgeGraphQL",
            "Cdf3dEntityEdgeList",
            "Cdf3dEntityEdgeTextFields",
        ]

        # Act
        data_class = EdgeDataClass.from_view(
            view, f"{NodeDataClass.to_base_name(view)}Edge", "edge", PygenConfig().naming.data_class
        )

        # Assert
        assert data_class.init_import == f"from .{data_class.file_name} import {', '.join(sorted(expected))}"


_DEPENDENCY_NAMED_FIELD = """
-   space: ReportedBugs
    externalId: Field
    name: Field
    version: c50e79569752a2
    writable: true
    usedFor: node
    isGlobal: false
    properties:
      name:
        container:
          space: ReportedBugs
          externalId: Field
        containerPropertyIdentifier: name
        type:
          list: false
          collation: ucs_basic
          type: text
        nullable: true
        autoIncrement: false
        immutable: false
        source: null
        defaultValue: null
        name: name
        description: null
      value:
        container:
          space: ReportedBugs
          externalId: Field
        containerPropertyIdentifier: value
        type:
          list: false
          type: float64
        nullable: true
        autoIncrement: false
        immutable: false
        source: null
        defaultValue: null
        name: value
        description: null
    lastUpdatedTime: 1705560037472
    createdTime: 1705560037472
-   space: ReportedBugs
    externalId: Country
    name: Country
    version: c93d79472dd1cb
    writable: true
    usedFor: node
    isGlobal: false
    properties:
      name:
        container:
          space: ReportedBugs
          externalId: Country
        containerPropertyIdentifier: name
        type:
          list: false
          collation: ucs_basic
          type: text
        nullable: true
        autoIncrement: false
        immutable: false
        source: null
        defaultValue: null
        name: name
        description: null
      fields:
        type:
          space: ReportedBugs
          externalId: Country.fields
        source:
          space: ReportedBugs
          externalId: Field
          version: c50e79569752a2
          type: view
        name: fields
        description: null
        edgeSource: null
        direction: outwards
        connectionType: multi_edge_connection
    lastUpdatedTime: 1705560037472
    createdTime: 1705560037472
"""

_SINGULAR_PLURAL_EQUALS = """
space: "power-ops"
externalId: "Series"
name: "Series"
version: "59d189398e78be"
writable: True
usedFor: "node"
isGlobal: False
properties: {}
lastUpdatedTime: 1695295084756
createdTime: 1695295084756
"""

_VIEW_WITH_TIME_PROPERTY_RAW = {
    "space": "power-ops",
    "externalId": "PriceArea",
    "name": "PriceArea",
    "version": "6849ae787cd368",
    "writable": True,
    "usedFor": "node",
    "isGlobal": False,
    "properties": {
        "name": {
            "container": {"space": "power-ops", "externalId": "PriceArea"},
            "containerPropertyIdentifier": "name",
            "type": {"list": False, "collation": "ucs_basic", "type": "text"},
            "nullable": True,
            "autoIncrement": False,
            "immutable": False,
            "source": None,
            "defaultValue": None,
            "name": "name",
            "description": None,
        },
        "description": {
            "container": {"space": "power-ops", "externalId": "PriceArea"},
            "containerPropertyIdentifier": "description",
            "type": {"list": False, "collation": "ucs_basic", "type": "text"},
            "nullable": True,
            "autoIncrement": False,
            "immutable": False,
            "source": None,
            "defaultValue": None,
            "name": "description",
            "description": None,
        },
        "dayAheadPrice": {
            "container": {"space": "power-ops", "externalId": "PriceArea"},
            "containerPropertyIdentifier": "dayAheadPrice",
            "type": {"list": False, "type": "timeseries"},
            "nullable": True,
            "autoIncrement": False,
            "immutable": False,
            "source": None,
            "defaultValue": None,
            "name": "dayAheadPrice",
            "description": None,
        },
    },
    "lastUpdatedTime": 1692020117686,
    "createdTime": 1692020117686,
}


_CDF3D_ENTITY_YAML = """space: cdf_3d_schema
externalId: Cdf3dEntity
implements: []
version: '1'
writable: false
usedFor: all
isGlobal: true
properties:
  inModel3d:
    type:
      space: cdf_3d_schema
      externalId: cdf3dEntityConnection
    source:
      space: cdf_3d_schema
      externalId: Cdf3dModel
      version: '1'
      type: view
    name: null
    description: Cdf3dModel the Cdf3dEntity is part of
    edgeSource:
      space: cdf_3d_schema
      externalId: Cdf3dConnectionProperties
      version: '1'
      type: view
    direction: outwards
    connectionType: multi_edge_connection
lastUpdatedTime: 1720165928804
createdTime: 1690467703871
"""
