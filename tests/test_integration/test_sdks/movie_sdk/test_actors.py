from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from movie_domain.client import MovieClient
    from movie_domain.client import data_classes as m
else:
    from movie_domain_pydantic_v1.client import MovieClient
    from movie_domain_pydantic_v1.client import data_classes as m


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
