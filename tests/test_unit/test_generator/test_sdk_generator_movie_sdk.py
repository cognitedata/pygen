from __future__ import annotations

from unittest.mock import MagicMock

from cognite.client import data_modeling as dm

from cognite.pygen._core.models import (
    FilterCondition,
    FilterConditionOnetoOneEdge,
    FilterMethod,
    FilterParameter,
    NodeDataClass,
)
from cognite.pygen.config import PygenConfig


def test_create_list_method(person_view: dm.View, pygen_config: PygenConfig) -> None:
    # Arrange
    data_class = NodeDataClass.from_view(
        person_view, NodeDataClass.to_base_name(person_view), pygen_config.naming.data_class
    )

    data_class.update_fields(
        person_view.properties,
        {dm.ViewId(space="IntegrationTestsImmutable", external_id="Role", version="3"): MagicMock(spec=NodeDataClass)},
        [],
        config=pygen_config,
    )
    parameters = [
        FilterParameter("min_birth_year", "int", description="The minimum value of the birth year to filter on."),
        FilterParameter("max_birth_year", "int", description="The maximum value of the birth year to filter on."),
        FilterParameter("name", "str | list[str]", description="The name to filter on."),
        FilterParameter("name_prefix", "str", description="The prefix of the name to filter on."),
        FilterParameter("external_id_prefix", "str", description="The prefix of the external ID to filter on."),
        FilterParameter("space", "str | list[str]", description="The space to filter on."),
    ]
    expected = FilterMethod(
        parameters=parameters,
        filters=[
            FilterCondition(
                dm.filters.Range, "birthYear", dict(gte=parameters[0], lte=parameters[1]), is_edge_class=False
            ),
            FilterCondition(dm.filters.Equals, "name", dict(value=parameters[2]), is_edge_class=False),
            FilterCondition(dm.filters.In, "name", dict(values=parameters[2]), is_edge_class=False),
            FilterCondition(dm.filters.Prefix, "name", dict(value=parameters[3]), is_edge_class=False),
            FilterCondition(dm.filters.Prefix, "externalId", dict(value=parameters[4]), is_edge_class=False),
            FilterCondition(dm.filters.Equals, "space", dict(value=parameters[5]), is_edge_class=False),
            FilterCondition(dm.filters.In, "space", dict(values=parameters[5]), is_edge_class=False),
        ],
    )

    # Act
    actual = FilterMethod.from_fields(data_class.fields, pygen_config.filtering)

    # Assert
    assert actual.parameters == expected.parameters
    for a, e in zip(actual.filters, expected.filters):
        assert a.filter == e.filter
        assert a.prop_name == e.prop_name
        assert a.keyword_arguments == e.keyword_arguments
        assert a == e
    assert actual == expected


def test_create_list_method_actors(actor_view: dm.View, pygen_config: PygenConfig) -> None:
    # Arrange
    data_class = NodeDataClass.from_view(
        actor_view, NodeDataClass.to_base_name(actor_view), pygen_config.naming.data_class
    )

    person_data_class = MagicMock(spec=NodeDataClass)
    person_data_class.view_id = dm.ViewId(space="IntegrationTestsImmutable", external_id="Person", version="3")
    data_class_by_view_id = {
        dm.ViewId(space="IntegrationTestsImmutable", external_id="Movie", version="3"): MagicMock(spec=NodeDataClass),
        dm.ViewId(space="IntegrationTestsImmutable", external_id="Nomination", version="3"): MagicMock(
            spec=NodeDataClass
        ),
        dm.ViewId(space="IntegrationTestsImmutable", external_id="Person", version="3"): person_data_class,
    }
    data_class.update_fields(
        actor_view.properties,
        data_class_by_view_id,
        [],
        config=pygen_config,
    )
    parameters = [
        FilterParameter(
            "person",
            "str | tuple[str, str] | list[str] | list[tuple[str, str]]",
            space="IntegrationTestsImmutable",
            description="The person to filter on.",
        ),
        FilterParameter("won_oscar", "bool", description="The won oscar to filter on."),
        FilterParameter("external_id_prefix", "str", description="The prefix of the external ID to filter on."),
        FilterParameter("space", "str | list[str]", description="The space to filter on."),
    ]
    expected = FilterMethod(
        parameters=parameters,
        filters=[
            FilterConditionOnetoOneEdge(
                dm.filters.Equals, "person", dict(value=parameters[0]), instance_type=str, is_edge_class=False
            ),
            FilterConditionOnetoOneEdge(
                dm.filters.Equals, "person", dict(value=parameters[0]), instance_type=tuple, is_edge_class=False
            ),
            FilterConditionOnetoOneEdge(
                dm.filters.In, "person", dict(values=parameters[0]), instance_type=str, is_edge_class=False
            ),
            FilterConditionOnetoOneEdge(
                dm.filters.In, "person", dict(values=parameters[0]), instance_type=tuple, is_edge_class=False
            ),
            FilterCondition(dm.filters.Equals, "wonOscar", dict(value=parameters[1]), is_edge_class=False),
            FilterCondition(dm.filters.Prefix, "externalId", dict(value=parameters[2]), is_edge_class=False),
            FilterCondition(dm.filters.Equals, "space", dict(value=parameters[3]), is_edge_class=False),
            FilterCondition(dm.filters.In, "space", dict(values=parameters[3]), is_edge_class=False),
        ],
    )

    # Act
    actual = FilterMethod.from_fields(data_class.fields, pygen_config.filtering)

    # Assert
    assert actual.parameters == expected.parameters
    for a, e in zip(actual.filters, expected.filters):
        assert a.filter == e.filter
        assert a.prop_name == e.prop_name
        assert a.keyword_arguments == e.keyword_arguments
        assert a == e
    assert actual == expected
