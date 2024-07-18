from __future__ import annotations

from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._core.models import (
    FilterImplementation,
    FilterImplementationOnetoOneEdge,
    FilterMethod,
    FilterParameter,
)
from cognite.pygen.config import PygenConfig


def test_create_list_method_primitive_nullable(
    omni_multi_api_generator: MultiAPIGenerator, pygen_config: PygenConfig
) -> None:
    # Arrange
    data_class = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId("pygen-models", "PrimitiveNullable", "1")
    ].data_class
    parameters = [
        FilterParameter("boolean", "bool", description="The boolean to filter on."),
        FilterParameter("min_date", "datetime.date", description="The minimum value of the date to filter on."),
        FilterParameter("max_date", "datetime.date", description="The maximum value of the date to filter on."),
        FilterParameter("min_float_32", "float", description="The minimum value of the float 32 to filter on."),
        FilterParameter("max_float_32", "float", description="The maximum value of the float 32 to filter on."),
        FilterParameter("min_float_64", "float", description="The minimum value of the float 64 to filter on."),
        FilterParameter("max_float_64", "float", description="The maximum value of the float 64 to filter on."),
        FilterParameter("min_int_32", "int", description="The minimum value of the int 32 to filter on."),
        FilterParameter("max_int_32", "int", description="The maximum value of the int 32 to filter on."),
        FilterParameter("min_int_64", "int", description="The minimum value of the int 64 to filter on."),
        FilterParameter("max_int_64", "int", description="The maximum value of the int 64 to filter on."),
        FilterParameter("text", "str | list[str]", description="The text to filter on."),
        FilterParameter("text_prefix", "str", description="The prefix of the text to filter on."),
        FilterParameter(
            "min_timestamp", "datetime.datetime", description="The minimum value of the timestamp to filter on."
        ),
        FilterParameter(
            "max_timestamp", "datetime.datetime", description="The maximum value of the timestamp to filter on."
        ),
        FilterParameter("external_id_prefix", "str", description="The prefix of the external ID to filter on."),
        FilterParameter("space", "str | list[str]", description="The space to filter on."),
    ]
    expected = FilterMethod(
        parameters=parameters,
        implementations=[
            FilterImplementation(dm.filters.Equals, "boolean", dict(value=parameters[0]), is_edge_class=False),
            FilterImplementation(
                dm.filters.Range, "date", dict(gte=parameters[1], lte=parameters[2]), is_edge_class=False
            ),
            FilterImplementation(
                dm.filters.Range, "float32", dict(gte=parameters[3], lte=parameters[4]), is_edge_class=False
            ),
            FilterImplementation(
                dm.filters.Range, "float64", dict(gte=parameters[5], lte=parameters[6]), is_edge_class=False
            ),
            FilterImplementation(
                dm.filters.Range, "int32", dict(gte=parameters[7], lte=parameters[8]), is_edge_class=False
            ),
            FilterImplementation(
                dm.filters.Range, "int64", dict(gte=parameters[9], lte=parameters[10]), is_edge_class=False
            ),
            FilterImplementation(dm.filters.Equals, "text", dict(value=parameters[11]), is_edge_class=False),
            FilterImplementation(dm.filters.In, "text", dict(values=parameters[11]), is_edge_class=False),
            FilterImplementation(dm.filters.Prefix, "text", dict(value=parameters[12]), is_edge_class=False),
            FilterImplementation(
                dm.filters.Range, "timestamp", dict(gte=parameters[13], lte=parameters[14]), is_edge_class=False
            ),
            FilterImplementation(dm.filters.Prefix, "externalId", dict(value=parameters[15]), is_edge_class=False),
            FilterImplementation(dm.filters.Equals, "space", dict(value=parameters[16]), is_edge_class=False),
            FilterImplementation(dm.filters.In, "space", dict(values=parameters[16]), is_edge_class=False),
        ],
    )

    # Act
    actual = FilterMethod.from_fields(data_class.fields, pygen_config.filtering)

    # Assert
    assert actual.parameters == expected.parameters
    for act, exp in zip(actual.implementations, expected.implementations):
        assert act.filter == exp.filter
        assert act.prop_name == exp.prop_name
        assert act.keyword_arguments == exp.keyword_arguments
        assert act == exp
    assert actual == expected


def test_create_list_method_connection_item_a(
    omni_multi_api_generator: MultiAPIGenerator, pygen_config: PygenConfig
) -> None:
    # Arrange
    data_class = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId("pygen-models", "ConnectionItemA", "1")
    ].data_class
    parameters = [
        FilterParameter("name", "str | list[str]", description="The name to filter on."),
        FilterParameter("name_prefix", "str", description="The prefix of the name to filter on."),
        FilterParameter(
            "other_direct",
            "str | tuple[str, str] | list[str] | list[tuple[str, str]]",
            description="The other direct to filter on.",
        ),
        FilterParameter(
            "self_direct",
            "str | tuple[str, str] | list[str] | list[tuple[str, str]]",
            description="The self direct to filter on.",
        ),
        FilterParameter("external_id_prefix", "str", description="The prefix of the external ID to filter on."),
        FilterParameter("space", "str | list[str]", description="The space to filter on."),
    ]
    expected = FilterMethod(
        parameters=parameters,
        implementations=[
            FilterImplementation(dm.filters.Equals, "name", dict(value=parameters[0]), is_edge_class=False),
            FilterImplementation(dm.filters.In, "name", dict(values=parameters[0]), is_edge_class=False),
            FilterImplementation(dm.filters.Prefix, "name", dict(value=parameters[1]), is_edge_class=False),
            FilterImplementationOnetoOneEdge(
                dm.filters.Equals, "otherDirect", dict(value=parameters[2]), is_edge_class=False, instance_type=str
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.Equals, "otherDirect", dict(value=parameters[2]), is_edge_class=False, instance_type=tuple
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.In, "otherDirect", dict(values=parameters[2]), is_edge_class=False, instance_type=str
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.In, "otherDirect", dict(values=parameters[2]), is_edge_class=False, instance_type=tuple
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.Equals, "selfDirect", dict(value=parameters[3]), is_edge_class=False, instance_type=str
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.Equals, "selfDirect", dict(value=parameters[3]), is_edge_class=False, instance_type=tuple
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.In, "selfDirect", dict(values=parameters[3]), is_edge_class=False, instance_type=str
            ),
            FilterImplementationOnetoOneEdge(
                dm.filters.In, "selfDirect", dict(values=parameters[3]), is_edge_class=False, instance_type=tuple
            ),
            FilterImplementation(dm.filters.Prefix, "externalId", dict(value=parameters[4]), is_edge_class=False),
            FilterImplementation(dm.filters.Equals, "space", dict(value=parameters[5]), is_edge_class=False),
            FilterImplementation(dm.filters.In, "space", dict(values=parameters[5]), is_edge_class=False),
        ],
    )

    # Act
    actual = FilterMethod.from_fields(data_class.fields, pygen_config.filtering)

    # Assert
    assert actual.parameters == expected.parameters
    for act, exp in zip(actual.implementations, expected.implementations):
        assert act.filter == exp.filter
        assert act.prop_name == exp.prop_name
        assert act.keyword_arguments == exp.keyword_arguments
        assert act == exp
    assert actual == expected
