from __future__ import annotations

from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from movie_domain.client import MovieClient
    from movie_domain.client import data_classes as m
else:
    from movie_domain_pydantic_v1.client import MovieClient
    from movie_domain_pydantic_v1.client import data_classes as m


def test_actor_apply_retrieve_with_person(movie_client: MovieClient, cognite_client: CogniteClient):
    # Arrange
    actor = m.ActorApply(
        external_id="actor:anders",
        movies=[
            m.MovieApply(
                external_id="movie:anders",
                title="Anders",
                release_year=1999,
            )
        ],
        nomination=[],
        person=m.PersonApply(external_id="person:anders", name="Anders", birth_year=0),
    )
    resources = actor.to_instances_apply()
    node_ids = resources.nodes.as_ids()
    edge_ids = resources.edges.as_ids()

    try:
        # Act
        created = movie_client.actor.apply(actor)

        # Assert
        assert len(created.nodes) == 3
        assert len(created.edges) == 1

        # Act
        retrieved = movie_client.actor.retrieve(external_id=actor.external_id)

        # Assert
        assert retrieved is not None
        assert len(retrieved.movies or []) == 1
        assert len(retrieved.nomination or []) == 0
        assert retrieved.person is not None
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node_ids, edges=edge_ids)


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


def test_actor_query_direct_relation(movie_client: MovieClient):
    actors = movie_client.actor(limit=2).query(retrieve_person=True)

    assert len(actors) == 2
    for actor in actors:
        assert isinstance(actor, m.Actor)
        assert isinstance(actor.person, m.Person)
        assert actor.person.external_id.split(":")[1] == actor.external_id.split(":")[1]
