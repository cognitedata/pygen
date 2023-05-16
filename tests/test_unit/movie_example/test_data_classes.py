from examples.movie_domain.data.load import MovieModel
from examples.movie_domain.local.client import MovieClient


def test_dump_circular_model(movie_model: MovieModel):
    a_movie = movie_model.movies[0]

    assert a_movie.dict(exclude={"external_id"})


def test_repr_circular_model(movie_model: MovieModel):
    an_actor = movie_model.actors[0]

    repr(an_actor)


def test_local_movie_client(movie_model):
    client = MovieClient(movie_model)

    client.movies.list()
