from examples.movie_domain.client import MovieClient
from examples.movie_domain.client import data_classes as movie


def test_person_list(movie_client: MovieClient):
    people = movie_client.persons.list(traversal_count=0, limit=-1)

    assert isinstance(people, movie.PersonList)
    assert all(isinstance(role, movie.RoleId) for person in people for role in person.roles)


def test_person_retrieve(movie_client: MovieClient):
    quentin = movie_client.persons.retrieve("person:quentin_tarantino")

    assert isinstance(quentin, movie.Person)
    assert len(quentin.roles) == 2


def test_person_retrieve_multiple(movie_client: MovieClient):
    people = movie_client.persons.retrieve(["person:quentin_tarantino", "person:john_travolta"])

    assert isinstance(people, movie.PersonList)
    assert len(people) == 2
    assert all(isinstance(role, movie.RoleId) for person in people for role in person.roles)


# def test_type_retrieve(movie_client: MovieClient):
#     expected = None
#
#     pulp_fiction = movie_client.movies.retrieve("Pulp Fiction")
#
#     assert pulp_fiction == expected
