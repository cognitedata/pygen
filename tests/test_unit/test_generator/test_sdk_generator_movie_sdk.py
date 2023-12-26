from __future__ import annotations

import platform
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import APIGenerator, MultiAPIGenerator, SDKGenerator
from cognite.pygen._core.models import (
    FilterCondition,
    FilterConditionOnetoOneEdge,
    FilterMethod,
    FilterParameter,
    NodeDataClass,
)
from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from tests.constants import IS_PYDANTIC_V1, IS_PYDANTIC_V2, MovieSDKFiles


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "movie_domain_pydantic_v1.client"
    else:
        return "movie_domain.client"


@pytest.fixture
def client_name() -> str:
    return "MovieClient"


@pytest.fixture
def sdk_generator(movie_model, top_level_package, client_name) -> SDKGenerator:
    return SDKGenerator(top_level_package, client_name, movie_model)


@pytest.fixture
def multi_api_generator(
    movie_model: dm.DataModel[dm.View], top_level_package: str, pygen_config: PygenConfig
) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        top_level_package, "MovieClient", movie_model.views, movie_model.space, config=pygen_config
    )


@pytest.fixture()
def person_api_generator(multi_api_generator: MultiAPIGenerator, person_view: dm.View) -> APIGenerator:
    api_generator = multi_api_generator[person_view.as_id()]
    assert api_generator is not None, "Could not find API generator for actor view"
    return api_generator


@pytest.fixture()
def actor_api_generator(multi_api_generator: MultiAPIGenerator, actor_view: dm.View) -> APIGenerator:
    api_generator = multi_api_generator[actor_view.as_id()]
    assert api_generator is not None, "Could not find API generator for actor view"
    return api_generator


def test_generate_data_class_file_persons(
    person_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.persons_data.read_text()

    # Act
    actual = person_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_data_class_actors(
    actor_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actors_data.read_text()

    # Act
    actual = actor_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_actors(
    actor_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actors_api.read_text()

    # Act
    actual = actor_api_generator.generate_api_file(top_level_package, client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_query_api_actors(
    actor_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actor_query_api.read_text()

    # Act
    actual = actor_api_generator.generate_api_query_file(top_level_package, client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_actor_movie_edge_api(
    actor_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.actor_movies_api.read_text()

    # Act
    _, actual = next(actor_api_generator.generate_edge_api_files(top_level_package, client_name))
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_persons(
    person_api_generator: APIGenerator, top_level_package: str, client_name: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MovieSDKFiles.persons_api.read_text()

    # Act
    actual = person_api_generator.generate_api_file(top_level_package, client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_data_class_init_file(multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MovieSDKFiles.data_init.read_text()

    # Act
    actual = multi_api_generator.generate_data_classes_init_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


@pytest.mark.skipif(
    not platform.platform().startswith("Windows"),
    reason="There is currently some strange problem with the diff on non-windows",
)
def test_create_api_client(sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MovieSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


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
