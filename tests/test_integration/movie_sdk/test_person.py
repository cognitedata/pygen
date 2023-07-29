from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from movie_domain_pydantic_v1.client import MovieClient
    from movie_domain_pydantic_v1.client import data_classes as movie
else:
    from movie_domain.client import MovieClient
    from movie_domain.client import data_classes as movie


def test_person_list(movie_client: MovieClient):
    people = movie_client.persons.list(limit=-1)

    assert len(people) > 0
    assert all(isinstance(role, str) for person in people for role in person.roles)


def test_person_retrieve(movie_client: MovieClient):
    quentin = movie_client.persons.retrieve("person:quentin_tarantino")

    assert quentin.external_id == "person:quentin_tarantino"
    assert len(quentin.roles) == 2


def test_person_retrieve_multiple(movie_client: MovieClient):
    people = movie_client.persons.retrieve(["person:quentin_tarantino", "person:john_travolta"])

    assert len(people) == 2
    assert all(isinstance(role, str) for person in people for role in person.roles)


def test_person_apply_and_delete(movie_client: MovieClient):
    # Arrange
    person = movie.PersonApply(external_id="person:anders", name="Anders", birth_year=0)

    # Act
    try:
        result = movie_client.persons.apply(person)

        assert len(result.nodes) == 1
    finally:
        movie_client.persons.delete(person.external_id)
