from examples.movie_domain.client import MovieClient
from examples.movie_domain.client import data_classes as movie


def test_person_listing(movie_client: MovieClient):
    people = movie_client.persons.list(limit=-1)

    assert isinstance(people, movie.PersonList)


#
# def test_type_retrieve(movie_client: MovieClient):
#     expected = None
#
#     pulp_fiction = movie_client.movies.retrieve("Pulp Fiction")
#
#     assert pulp_fiction == expected
