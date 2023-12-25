import json
from datetime import datetime, timezone
from typing import Callable

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties

from cognite.pygen.utils.external_id_factories import (
    create_incremental_factory,
    create_sha256_factory,
    create_uuid_factory,
    sha256_factory,
    uuid_factory,
)
from tests.constants import IS_PYDANTIC_V2, WindMillFiles

if IS_PYDANTIC_V2:
    from markets.client.data_classes import ValueTransformation
    from movie_domain.client import data_classes as movie
    from pydantic import TypeAdapter
    from shop.client.data_classes import CaseApply, CommandConfigApply
    from windmill.client.data_classes import DomainModelApply, ResourcesApply, WindmillApply
else:
    from pydantic import parse_obj_as
    from windmill_pydantic_v1.client.data_classes import DomainModelApply, ResourcesApply, WindmillApply

    from markets_pydantic_v1.client.data_classes import ValueTransformation
    from movie_domain_pydantic_v1.client import data_classes as movie
    from shop_pydantic_v1.client.data_classes import CaseApply, CommandConfigApply


class TestFromInstance:
    def test_person_from_instance(self) -> None:
        # Arrange
        node = dm.Node.load(
            {
                "instanceType": "node",
                "space": "IntegrationTestsImmutable",
                "externalId": "person:christoph_waltz",
                "version": 1,
                "lastUpdatedTime": 1684170308732,
                "createdTime": 1684170308732,
                "properties": {
                    "IntegrationTestsImmutable": {"Person/2": {"name": "Christoph Waltz", "birthYear": 1956}}
                },
            }
        )

        # Act
        person = movie.Person.from_instance(node)

        # Assert
        assert person.name == "Christoph Waltz"

    def test_from_node_with_json(self) -> None:
        # Arrange
        node = dm.Node(
            space="market",
            external_id="myExternalId",
            version=1,
            last_updated_time=0,
            created_time=0,
            properties=Properties({dm.ViewId("market", "myView", "1"): {"arguments": {"a": 1}, "method": "myMethod"}}),
            deleted_time=None,
            type=None,
        )
        expected = ValueTransformation(
            external_id="myExternalId",
            version=1,
            last_updated_time=datetime.fromtimestamp(0, tz=timezone.utc),
            created_time=datetime.fromtimestamp(0, tz=timezone.utc),
            arguments={"a": 1},
            method="myMethod",
        )

        # Act
        actual = ValueTransformation.from_instance(node)

        # Assert
        assert actual == expected


class TestToPandas:
    def test_person_to_pandas(self):
        # Arrange
        node = dm.Node.load(
            {
                "instanceType": "node",
                "space": "IntegrationTestsImmutable",
                "externalId": "person:christoph_waltz",
                "version": 1,
                "lastUpdatedTime": 1684170308732,
                "createdTime": 1684170308732,
                "properties": {
                    "IntegrationTestsImmutable": {"Person/2": {"name": "Christoph Waltz", "birthYear": 1956}}
                },
            }
        )
        person = movie.Person.from_instance(node)
        persons = movie.PersonList([person])

        # Act
        df = persons.to_pandas()

        # Assert
        assert not df.empty
        assert len(df) == 1


def person_apply_to_instances_test_cases():
    person = movie.PersonApply(name="Christoph Waltz", birth_year=1956, external_id="person:christoph_waltz")
    expected = dm.InstancesApply(
        dm.NodeApplyList(
            [
                dm.NodeApply(
                    "IntegrationTestsImmutable",
                    "person:christoph_waltz",
                    sources=[
                        dm.NodeOrEdgeData(
                            source=dm.ViewId("IntegrationTestsImmutable", "Person", "3"),
                            properties={"name": "Christoph Waltz", "birthYear": 1956},
                        )
                    ],
                )
            ]
        ),
        dm.EdgeApplyList([]),
    )
    yield pytest.param(person, expected, id="Person no extra dependencies")
    return
    person = movie.PersonApply(
        name="Quentin Tarantino",
        birth_year=1963,
        external_id="person:quentin_tarantino",
        roles=[
            movie.ActorApply(
                external_id="actor:quentin_tarantino",
                won_oscar=False,
                person="person:quentin_tarantino",
                movies=[
                    movie.MovieApply(
                        external_id="movie:pulp_fiction",
                        title="Pulp Fiction",
                        release_year=1994,
                        actors=["actor:quentin_tarantino"],
                        directors=["director:quentin_tarantino"],
                        run_time_minutes=154,
                        meta={"imdb": {"rating": 8.9, "votes": 1780000}},
                        rating=movie.RatingApply(
                            external_id="rating:pulp_fiction",
                            score="rating:pulp_fiction",
                            votes="vote_count:pulp_fiction",
                        ),
                    )
                ],
            ),
            movie.DirectorApply(
                external_id="director:quentin_tarantino",
                won_oscar=True,
                person="person:quentin_tarantino",
                nomination=[
                    movie.NominationApply(
                        external_id="director:quentin_tarantino:pulp_fiction", name="Best Director", year=1995
                    )
                ],
            ),
        ],
    )
    expected_nodes = [
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "person:quentin_tarantino",
            "sources": [
                {
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "externalId": "Person",
                        "type": "view",
                        "version": "2",
                    },
                    "properties": {"name": "Quentin Tarantino", "birthYear": 1963},
                }
            ],
        },
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "actor:quentin_tarantino",
            "sources": [
                {
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "externalId": "Role",
                        "type": "view",
                        "version": "2",
                    },
                    "properties": {
                        "person": {"externalId": "person:quentin_tarantino", "space": "IntegrationTestsImmutable"},
                        "wonOscar": False,
                    },
                }
            ],
        },
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "movie:pulp_fiction",
            "sources": [
                {
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "externalId": "Movie",
                        "type": "view",
                        "version": "2",
                    },
                    "properties": {
                        "meta": {"imdb": {"rating": 8.9, "votes": 1780000}},
                        "rating": {"externalId": "rating:pulp_fiction", "space": "IntegrationTestsImmutable"},
                        "releaseYear": 1994,
                        "runTimeMinutes": 154.0,
                        "title": "Pulp Fiction",
                    },
                }
            ],
        },
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "rating:pulp_fiction",
            "sources": [
                {
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "externalId": "Rating",
                        "type": "view",
                        "version": "2",
                    },
                    "properties": {"score": "rating:pulp_fiction", "votes": "vote_count:pulp_fiction"},
                }
            ],
        },
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "director:quentin_tarantino",
            "sources": [
                {
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "externalId": "Role",
                        "type": "view",
                        "version": "2",
                    },
                    "properties": {
                        "person": {"externalId": "person:quentin_tarantino", "space": "IntegrationTestsImmutable"},
                        "wonOscar": True,
                    },
                }
            ],
        },
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "director:quentin_tarantino:pulp_fiction",
            "sources": [
                {
                    "source": {
                        "space": "IntegrationTestsImmutable",
                        "externalId": "Nomination",
                        "type": "view",
                        "version": "2",
                    },
                    "properties": {"name": "Best Director", "year": 1995},
                }
            ],
        },
    ]
    expected_edges = [
        {
            "endNode": {"externalId": "actor:quentin_tarantino", "space": "IntegrationTestsImmutable"},
            "externalId": "person:quentin_tarantino:actor:quentin_tarantino",
            "instanceType": "edge",
            "source": None,
            "space": "IntegrationTestsImmutable",
            "startNode": {"externalId": "person:quentin_tarantino", "space": "IntegrationTestsImmutable"},
            "type": {"externalId": "Person.roles", "space": "IntegrationTestsImmutable"},
        },
        {
            "instanceType": "edge",
            "space": "IntegrationTestsImmutable",
            "externalId": "actor:quentin_tarantino:movie:pulp_fiction",
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
            "startNode": {"space": "IntegrationTestsImmutable", "externalId": "actor:quentin_tarantino"},
            "endNode": {"space": "IntegrationTestsImmutable", "externalId": "movie:pulp_fiction"},
        },
        {
            "instanceType": "edge",
            "space": "IntegrationTestsImmutable",
            "externalId": "movie:pulp_fiction:actor:quentin_tarantino",
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Movie.actors"},
            "startNode": {"space": "IntegrationTestsImmutable", "externalId": "movie:pulp_fiction"},
            "endNode": {"space": "IntegrationTestsImmutable", "externalId": "actor:quentin_tarantino"},
        },
        {
            "instanceType": "edge",
            "space": "IntegrationTestsImmutable",
            "externalId": "movie:pulp_fiction:director:quentin_tarantino",
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Movie.directors"},
            "startNode": {"space": "IntegrationTestsImmutable", "externalId": "movie:pulp_fiction"},
            "endNode": {"space": "IntegrationTestsImmutable", "externalId": "director:quentin_tarantino"},
        },
        {
            "endNode": {"externalId": "director:quentin_tarantino", "space": "IntegrationTestsImmutable"},
            "externalId": "person:quentin_tarantino:director:quentin_tarantino",
            "instanceType": "edge",
            "space": "IntegrationTestsImmutable",
            "startNode": {"externalId": "person:quentin_tarantino", "space": "IntegrationTestsImmutable"},
            "type": {"externalId": "Person.roles", "space": "IntegrationTestsImmutable"},
        },
        {
            "instanceType": "edge",
            "space": "IntegrationTestsImmutable",
            "externalId": "director:quentin_tarantino:director:quentin_tarantino:pulp_fiction",
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
            "startNode": {"space": "IntegrationTestsImmutable", "externalId": "director:quentin_tarantino"},
            "endNode": {
                "space": "IntegrationTestsImmutable",
                "externalId": "director:quentin_tarantino:pulp_fiction",
            },
        },
    ]
    expected_edges = dm.EdgeApplyList.load(expected_edges)
    for edge in expected_edges:
        edge.sources = None

    yield pytest.param(
        person,
        dm.InstancesApply(nodes=dm.NodeApplyList.load(expected_nodes), edges=expected_edges),
        id="Person with extra dependencies",
    )


class TestToInstancesApply:
    @pytest.mark.parametrize("person, expected", list(person_apply_to_instances_test_cases()))
    def test_person_to_apply_instances(self, person: movie.PersonApply, expected: dm.InstancesApply):
        # Act
        actual = person.to_instances_apply()

        # Assert
        assert actual.nodes.dump(camel_case=True) == expected.nodes.dump(camel_case=True)
        assert actual.edges.dump(camel_case=True) == expected.edges.dump(camel_case=True)

    def test_to_instances_with_recursive(self) -> None:
        # Arrange
        person = movie.PersonApply(external_id="person:anders", name="Anders", birth_year=0, roles=[])
        actor = movie.ActorApply(external_id="actor:anders", movies=[], nomination=[], person=person, won_oscar=False)
        person.roles.append(actor)

        # Act
        instances = person.to_instances_apply()

        # Assert
        assert len(instances.nodes) == 2
        assert len(instances.edges) == 1

    def test_person_apply_setting_space(self) -> None:
        # Arrange
        space = "myCustomSpace"
        person = movie.ActorApply(
            space=space,
            external_id="actor:christoph_waltz",
            person=movie.PersonApply(
                name="Christoph Waltz",
                birth_year=1956,
                external_id="person:christoph_waltz",
                space=space,
            ),
            won_oscar=True,
            movies=[
                movie.MovieApply(
                    external_id="movie:django_unchained",
                    title="Django Unchained",
                    space=space,
                    release_year=2012,
                    actors=["actor:christoph_waltz"],
                    directors=["director:quentin_tarantino"],
                    run_time_minutes=165,
                )
            ],
        )

        # Act
        instances = person.to_instances_apply()

        # Assert
        assert not (
            nodes := [n for n in instances.nodes if n.space != space]
        ), f"Found nodes with unexpected space: {nodes}"
        assert not (
            edges := [e for e in instances.edges if e.space != space]
        ), f"Found edges with unexpected space: {edges}"

    def test_to_instances_apply_case(self) -> None:
        # Arrange
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        case = CaseApply(
            external_id="shop:case:integration_test",
            name="Integration test",
            scenario="Integration test",
            start_time=datetime.strptime("2021-01-01T00:00:00Z", date_format),
            end_time=datetime.strptime("2021-01-01T00:00:00Z", date_format),
            commands=CommandConfigApply(
                external_id="shop:command_config:integration_test", configs=["BlueViolet", "Red"]
            ),
            cut_files=["shop:cut_file:1"],
            bid="shop:bid_matrix:8",
            bid_history=["shop:bid_matrix:9"],
            run_status="Running",
            arguments="Integration test",
        )

        # Act
        instances = case.to_instances_apply()

        # Assert
        assert len(instances.nodes) == 2
        assert len(instances.edges) == 0


@pytest.fixture(scope="module")
def person_and_person_apply() -> tuple[movie.Person, movie.PersonApply]:
    person = movie.Person(
        external_id="person:christoph_waltz",
        birthYear=1956,
        name="Christoph Waltz",
        roles=["actor:christoph_waltz"],
        version=1,
        created_time=datetime(2023, 6, 7),
        last_updated_time=datetime(2023, 8, 8),
    )

    person_apply = movie.PersonApply(
        name="Christoph Waltz",
        birth_year=1956,
        external_id="person:christoph_waltz",
        roles=["actor:christoph_waltz"],
    )
    return person, person_apply


class TestAsApply:
    def test_as_apply(self, person_and_person_apply: tuple[movie.Person, movie.PersonApply]):
        # Arrange
        person, person_apply = person_and_person_apply

        # Act
        actual = person.as_apply()

        # Assert
        assert actual == person_apply

    def test_as_apply_list(self, person_and_person_apply: tuple[movie.Person, movie.PersonApply]):
        # Arrange
        person, person_apply = person_and_person_apply

        # Act
        actual = movie.PersonList([person]).as_apply()

        # Assert
        assert actual == movie.PersonApplyList([person_apply])


@pytest.mark.parametrize(
    "factory, expected_node_count, expected_edge_count",
    [
        # There are none unique sensor positions in the windmill data
        # so hashing it will lead to fewer nodes
        (sha256_factory, 135, 105),
        (create_incremental_factory(), 145, 105),
        (uuid_factory, 145, 105),
        (create_sha256_factory(True), 135, 105),
        (create_uuid_factory(True), 145, 105),
    ],
)
def test_load_windmills_from_json(
    factory: Callable[[type, dict], str],
    expected_node_count: int,
    expected_edge_count: int,
) -> None:
    # Arrange
    raw_json = WindMillFiles.Data.wind_mill_json.read_text()
    try:
        DomainModelApply.external_id_factory = factory

        loaded_json = json.loads(raw_json)

        # Act
        if IS_PYDANTIC_V2:
            windmills = TypeAdapter(list[WindmillApply]).validate_json(raw_json)
        else:
            windmills = parse_obj_as(list[WindmillApply], raw_json)
        created = ResourcesApply()
        for item in windmills:
            created.extend(item.to_instances_apply())

        # Assert
        exclude = {"external_id", "space"}
        for windmill, json_item in zip(windmills, loaded_json):
            if IS_PYDANTIC_V2:
                dumped_windmill = json.loads(
                    windmill.model_dump_json(by_alias=True, exclude=exclude, exclude_none=True)
                )
            else:
                dumped_windmill = json.loads(windmill.json(by_alias=True, exclude=exclude, exclude_none=True))
            # The exclude=True is not recursive in pydantic, so we need to do it manually
            _recursive_exclude(dumped_windmill, exclude)
            assert dumped_windmill == json_item

        assert len(created.nodes) == expected_node_count
        assert len(created.edges) == expected_edge_count
    finally:
        DomainModelApply.external_id_factory = None


def _recursive_exclude(d: dict, exclude: set[str]) -> None:
    for key in list(d.keys()):
        value = d[key]
        if isinstance(value, dict):
            _recursive_exclude(value, exclude)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _recursive_exclude(item, exclude)
        elif key in exclude:
            d.pop(key)
