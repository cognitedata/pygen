import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties

from movie_domain.client import data_classes as movie
from movie_domain.client.data_classes._core import InstancesApply, unpack_properties


def test_person_from_node():
    # Arrange
    node = dm.Node.load(
        {
            "instance_type": "node",
            "space": "IntegrationTestsImmutable",
            "external_id": "person:christoph_waltz",
            "version": 1,
            "last_updated_time": 1684170308732,
            "created_time": 1684170308732,
            "properties": {"IntegrationTestsImmutable": {"Person/2": {"name": "Christoph Waltz", "birthYear": 1956}}},
        }
    )

    # Act
    person = movie.Person.from_node(node)

    # Assert
    assert person.name == "Christoph Waltz"


def test_person_one_to_many_fields():
    # Arrange
    expected = ["roles"]

    # Act
    actual = movie.Person.one_to_many_fields()

    # Assert
    assert actual == expected


def person_apply_to_instances_test_cases():
    person = movie.PersonApply(name="Christoph Waltz", birth_year=1956, external_id="person:christoph_waltz")
    expected = InstancesApply(
        [
            dm.NodeApply(
                "IntegrationTestsImmutable",
                "person:christoph_waltz",
                sources=[
                    dm.NodeOrEdgeData(
                        source=dm.ContainerId("IntegrationTestsImmutable", "Person"),
                        properties={"name": "Christoph Waltz", "birthYear": 1956},
                    )
                ],
            )
        ],
        [],
    )
    yield pytest.param(person, expected, id="Person no extra dependencies")

    person = movie.PersonApply(
        name="Quentin Tarantino",
        birth_year=1963,
        external_id="person:quentin_tarantino",
        roles=[
            movie.RoleApply(
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
            movie.RoleApply(
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

    yield pytest.param(person, expected, id="Person with extra dependencies")


@pytest.mark.parametrize("person, expected", list(person_apply_to_instances_test_cases()))
def test_person_to_apply_instances(person: movie.PersonApply, expected: InstancesApply):
    # Act
    actual = person.to_instances_apply()

    # Assert
    assert actual.nodes == expected.nodes
    assert actual.edges == expected.edges


def unpack_properties_test_cases():
    properties = {
        "IntegrationTestsImmutable": {
            "Person/2": {
                "name": "Christoph Waltz",
                "birthYear": 1956,
            }
        }
    }
    expected = {
        "name": "Christoph Waltz",
        "birthYear": 1956,
    }
    yield pytest.param(properties, expected, id="Person")

    properties = {
        "IntegrationTestsImmutable": {
            "Actor/2": {
                "person": {"space": "IntegrationTestsImmutable", "externalId": "person:ethan_coen"},
                "wonOscar": True,
            }
        }
    }
    expected = {"person": "person:ethan_coen", "wonOscar": True}
    yield pytest.param(properties, expected, id="Actor")


@pytest.mark.parametrize("raw_properties, expected", list(unpack_properties_test_cases()))
def test_unpack_properties(raw_properties: dict, expected: dict):
    # Arrange
    properties = Properties.load(raw_properties)

    # Act
    actual = unpack_properties(properties)

    # Assert
    assert actual == expected
