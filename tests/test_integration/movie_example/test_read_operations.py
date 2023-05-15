from examples.movie_domain.sdk import MovieClient


def test_type_listing(movie_client: MovieClient):
    movies = movie_client.movies.list(limit=-1)

    assert movies


def test_type_retrieve(movie_client: MovieClient):
    expected = None

    pulp_fiction = movie_client.movies.retrieve("Pulp Fiction")

    assert pulp_fiction == expected
