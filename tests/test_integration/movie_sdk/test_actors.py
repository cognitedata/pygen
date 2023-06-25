from movie_domain.client import MovieClient
from movie_domain.client import data_classes as m


def test_actor_list(movie_client: MovieClient):
    actors = movie_client.actors.list(limit=-1)

    assert isinstance(actors, m.ActorList)
    assert all(isinstance(movie, str) for actor in actors for movie in actor.movies)
    assert all(isinstance(nomination, str) for actor in actors for nomination in actor.nomination)
