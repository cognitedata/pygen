from __future__ import annotations

import pytest

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


def test_actor_apply_with_person(movie_client: MovieClient):
    # Arrange
    actor = m.ActorApply(
        external_id="actor:anders",
        movies=[],
        nomination=[],
        person=m.PersonApply(external_id="person:anders", name="Anders", birth_year=0),
    )

    try:
        # Act
        created = movie_client.actor.apply(actor)

        # Assert
        assert len(created.nodes) == 2
        assert len(created.edges) == 0
    finally:
        movie_client.actor.delete(actor.external_id)
        movie_client.person.delete(actor.person.external_id)


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
def test_actor_list_filter_on_direct_edge(
    person: str | list[str] | tuple[str, str] | list[tuple[str, str]],
    expected_count: int,
    expected_person: list[str],
    movie_client: MovieClient,
) -> None:
    # Act
    actors = movie_client.actor.list(person=person, limit=expected_count + 1, retrieve_edges=False)

    # Assert
    assert len(actors) == expected_count
    assert sorted([actor.person for actor in actors]) == sorted(expected_person)


def test_circular_query_from_actor(movie_client: MovieClient):
    actors = movie_client.actorz(person="person:quentin_tarantino", limit=-1).movies(limit=-1).actors(limit=-1).query()

    assert len(actors) > 0
    for actor in actors:
        assert isinstance(actor, m.Actor)
        for movie in actor.movies:
            assert isinstance(movie, m.Movie)
            for movie_actor in movie.actors:
                assert isinstance(movie_actor, m.Actor)
