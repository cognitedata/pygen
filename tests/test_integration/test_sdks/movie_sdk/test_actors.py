from __future__ import annotations

import pytest
from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from movie_domain.client import MovieClient
    from movie_domain.client import data_classes as m
else:
    from movie_domain_pydantic_v1.client import MovieClient
    from movie_domain_pydantic_v1.client import data_classes as m


def test_actor_list(movie_client: MovieClient):
    actors = movie_client.actor.list(limit=-1)

    assert len(actors) > 0
    assert all(isinstance(movie, str) for actor in actors for movie in actor.movies or [])
    assert all(isinstance(nomination, str) for actor in actors for nomination in actor.nomination or [])


def test_actor_apply_retrieve_with_person(movie_client: MovieClient, cognite_client: CogniteClient):
    # Arrange
    actor = m.ActorApply(
        external_id="actor:anders",
        movies=[],
        nomination=[],
        person=m.PersonApply(external_id="person:anders", name="Anders", birth_year=0),
    )
    resources = actor.to_instances_apply()
    node_ids = resources.nodes.as_ids()

    try:
        # Act
        created = movie_client.actor.apply(actor)

        # Assert
        assert len(created.nodes) == 2
        assert len(created.edges) == 0

        # Act
        retrieve = movie_client.actor.retrieve(external_id=actor.external_id)

        # Assert
        assert retrieve is not None
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node_ids)


@pytest.mark.parametrize(
    "person, expected_count, expected_person",
    [
        ("person:ethan_coen", 1, ["person:ethan_coen"]),
        (["person:ethan_coen", "person:joel_coen"], 2, ["person:ethan_coen", "person:joel_coen"]),
        (("IntegrationTestsImmutable", "person:ethan_coen"), 1, ["person:ethan_coen"]),
        (
            [("IntegrationTestsImmutable", "person:ethan_coen"), ("IntegrationTestsImmutable", "person:joel_coen")],
            2,
            ["person:ethan_coen", "person:joel_coen"],
        ),
    ],
)
def test_director_list_filter_on_direct_edge(
    person: str | list[str] | tuple[str, str] | list[tuple[str, str]],
    expected_count: int,
    expected_person: list[str],
    movie_client: MovieClient,
) -> None:
    # Act
    directors = movie_client.director.list(person=person, limit=expected_count + 1, retrieve_edges=False)

    # Assert
    assert len(directors) == expected_count
    assert sorted([actor.person for actor in directors]) == sorted(expected_person)


def test_circular_query_from_actor(movie_client: MovieClient):
    actors = movie_client.actor(person="person:quentin_tarantino", limit=-1).movies(limit=-1).actors(limit=-1).query()

    assert len(actors) > 0
    for actor in actors:
        assert isinstance(actor, m.Actor)
        assert len(actor.movies or []) > 0
        for movie in actor.movies:
            assert isinstance(movie, m.Movie)
            assert len(movie.actors or []) > 0
            for movie_actor in movie.actors:
                assert isinstance(movie_actor, m.Actor)


def test_actor_retrieve_missing(movie_client: MovieClient) -> None:
    # Act
    actor = movie_client.actor.retrieve("actor:missing")

    # Assert
    assert actor is None


def test_actor_query_direct_relation(movie_client: MovieClient):
    actors = movie_client.actor(limit=2).query(retrieve_person=True)

    assert len(actors) == 2
    for actor in actors:
        assert isinstance(actor, m.Actor)
        assert isinstance(actor.person, m.Person)
        assert actor.person.external_id.split(":")[1] == actor.external_id.split(":")[1]


def test_actor_filter_on_boolean(movie_client: MovieClient):
    actors = movie_client.actor.list(won_oscar=False, limit=-1)

    assert len(actors) > 0
    assert not (
        won_oscar := [actor for actor in actors if actor.won_oscar is True]
    ), f"Found actors with oscars {won_oscar}"
