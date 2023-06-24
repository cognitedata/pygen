from examples.movie_domain.client import MovieClient
from examples.movie_domain.client import data_classes as movie


def test_person_list_traversal_count_0(movie_client: MovieClient):
    people = movie_client.persons.list(traversal_count=0, limit=-1)

    assert isinstance(people, movie.PersonList)
    assert all(isinstance(role, movie.RoleId) for person in people for role in person.roles)


# def test_type_retrieve(movie_client: MovieClient):
#     expected = None
#
#     pulp_fiction = movie_client.movies.retrieve("Pulp Fiction")
#
#     assert pulp_fiction == expected
