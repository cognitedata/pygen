import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties

try:
    from movie_domain.client import data_classes as movie
    from movie_domain.client.data_classes._core import unpack_properties
except AttributeError as e:
    if "has no attribute 'model_rebuild" in str(e):
        # is pydantic v1
        from movie_domain_pydantic_v1.client import data_classes as movie
        from movie_domain_pydantic_v1.client.data_classes._core import unpack_properties
    else:
        raise e


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


def test_person_to_pandas():
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
    person = movie.Person.from_node(node)
    persons = movie.PersonList([person])

    # Act
    df = persons.to_pandas()

    # Assert
    assert not df.empty
    assert len(df) == 1


def test_person_one_to_many_fields():
    # Arrange
    expected = ["roles"]

    # Act
    actual = movie.Person.one_to_many_fields()

    # Assert
    assert actual == expected


def person_apply_to_instances_test_cases():
    person = movie.PersonApply(name="Christoph Waltz", birth_year=1956, external_id="person:christoph_waltz")
    expected = dm.InstancesApply(
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
    expected_nodes = [
        {
            "instanceType": "node",
            "space": "IntegrationTestsImmutable",
            "externalId": "person:quentin_tarantino",
            "sources": [
                {
                    "source": {"space": "IntegrationTestsImmutable", "externalId": "Person", "type": "container"},
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
                    "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "type": "container"},
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
                    "source": {"space": "IntegrationTestsImmutable", "externalId": "Movie", "type": "container"},
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
                    "source": {"space": "IntegrationTestsImmutable", "externalId": "Rating", "type": "container"},
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
                    "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "type": "container"},
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
                    "source": {"space": "IntegrationTestsImmutable", "externalId": "Nomination", "type": "container"},
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
            "endNode": {"space": "IntegrationTestsImmutable", "externalId": "director:quentin_tarantino:pulp_fiction"},
        },
    ]

    yield pytest.param(
        person,
        dm.InstancesApply(
            nodes=[dm.NodeApply.load(e) for e in expected_nodes], edges=[dm.EdgeApply.load(e) for e in expected_edges]
        ),
        id="Person with extra dependencies",
    )


@pytest.mark.parametrize("person, expected", list(person_apply_to_instances_test_cases()))
def test_person_to_apply_instances(person: movie.PersonApply, expected: dm.InstancesApply):
    # Act
    actual = person.to_instances_apply()

    # Assert
    assert [n.dump(camel_case=True) for n in actual.nodes] == [n.dump(camel_case=True) for n in expected.nodes]
    assert [e.dump(camel_case=True) for e in actual.edges] == [e.dump(camel_case=True) for e in expected.edges]


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


def test_to_instances_with_recursive():
    # Arrange
    person = movie.PersonApply(external_id="person:anders", name="Anders", birth_year=0)
    actor = movie.ActorApply(external_id="actor:anders", movies=[], nomination=[], person=person, won_oscar=False)
    person.roles.append(actor)

    # Act
    instances = person.to_instances_apply()

    # Assert
    assert len(instances.nodes) == 2
    assert len(instances.edges) == 1
