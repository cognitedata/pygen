from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen.config import PygenConfig
from tests.utils import to_data_class_by_view_id


class TestDataClasses:
    def test_dependency_named_Field(self, pygen_config: PygenConfig) -> None:
        # Arrange
        views = dm.ViewList.load(_DEPENDENCY_NAMED_FIELD).data

        node_class_by_view_id, edge_class_by_view_id = to_data_class_by_view_id(views, pygen_config)

        # Act
        for data_class, view in zip(node_class_by_view_id.values(), views):
            data_class.update_fields(
                view.properties, node_class_by_view_id, edge_class_by_view_id, list(views), pygen_config
            )

        # Assert
        country_data_class = node_class_by_view_id[dm.ViewId("ReportedBugs", "Country", "c93d79472dd1cb")]

        assert country_data_class.pydantic_field == "pydantic.Field"
        wrong_field = [field.name for field in country_data_class if field.pydantic_field != "pydantic.Field"]
        assert not wrong_field, f"Wrong pydantic field for the following field(s): {wrong_field}"

    def test_has_date_field(self, omni_multi_api_generator: MultiAPIGenerator) -> None:
        # Arrange
        api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
            dm.ViewId("pygen-models", "PrimitiveRequired", "1")
        ]

        # Assert
        assert api_generator.data_class.has_primitive_field_of_type(dm.Date)


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
