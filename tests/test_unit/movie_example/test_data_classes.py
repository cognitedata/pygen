import pytest

from examples.movie_domain.data.load import MovieModel
from examples.movie_domain.sdk.data_classes import Movie
from examples.movie_domain.sdk.local import MovieClientLocal
from movie_domain.sdk import MovieClient


@pytest.fixture()
def pulp_fiction_movie(movie_model) -> Movie:
    return next(movie for movie in movie_model.movies if movie.title == "Pulp Fiction")


def test_dump_circular_model(movie_model: MovieModel):
    a_movie = movie_model.movies[0]

    assert a_movie.dict(exclude={"external_id"})


def test_repr_circular_model(movie_model: MovieModel):
    an_actor = movie_model.actors[0]

    repr(an_actor)


def test_local_movie_client(movie_model):
    client = MovieClientLocal(movie_model, MovieClient())

    client.movies.list()


def test_traverse_circular_model_depth_0(pulp_fiction_movie: Movie):
    assert pulp_fiction_movie.traverse(depth=0).dict() == pulp_fiction_movie.copy().dict()


def test_traverse_circular_model_depth_1(pulp_fiction_movie: Movie):
    # Arrange
    output = pulp_fiction_movie.copy()
    output.directors = [director.copy() for director in pulp_fiction_movie.directors]
    output.actors = [actor.copy() for actor in pulp_fiction_movie.actors]

    # Act and Assert
    assert pulp_fiction_movie.traverse(depth=1) == output


def test_traverse_circular_model_depth_2(pulp_fiction_movie: Movie):
    # Arrange
    output = pulp_fiction_movie.copy()
    directors = []
    for director in pulp_fiction_movie.directors:
        new_copy = director.copy()
        new_copy.person = director.person.copy()
        new_copy.nomination = [nomination.copy() for nomination in director.nomination]
        new_copy.movies = [output if movie is pulp_fiction_movie else movie.copy() for movie in director.movies]
        directors.append(new_copy)
    output.directors = directors

    actors = []
    for actor in pulp_fiction_movie.actors:
        new_copy = actor.copy()
        new_copy.person = actor.person.copy()
        new_copy.nomination = [nomination.copy() for nomination in actor.nomination]
        new_copy.movies = [output if movie is pulp_fiction_movie else movie.copy() for movie in actor.movies]
        actors.append(new_copy)
    output.actors = actors

    # Act and Assert
    assert pulp_fiction_movie.traverse(depth=1) == output
