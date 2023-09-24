from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from movie_domain_pydantic_v1.client import MovieClient
    from movie_domain_pydantic_v1.client import data_classes as movie
else:
    from movie_domain.client import MovieClient
    from movie_domain.client import data_classes as movie


def test_person_list(movie_client: MovieClient):
    people = movie_client.person.list(limit=-1)

    assert len(people) > 0
    assert all(isinstance(role, str) for person in people for role in person.roles)


def test_person_retrieve(movie_client: MovieClient):
    quentin = movie_client.person.retrieve("person:quentin_tarantino")

    assert quentin.external_id == "person:quentin_tarantino"
    assert len(quentin.roles) == 2


def test_person_retrieve_multiple(movie_client: MovieClient):
    people = movie_client.person.retrieve(["person:quentin_tarantino", "person:john_travolta"])

    assert len(people) == 2
    assert all(isinstance(role, str) for person in people for role in person.roles)


def test_person_apply_and_delete(movie_client: MovieClient):
    # Arrange
    person = movie.PersonApply(external_id="person:anders", name="Anders", birth_year=0)

    # Act
    try:
        result = movie_client.person.apply(person)

        assert len(result.nodes) == 1
    finally:
        movie_client.person.delete(person.external_id)


def test_person_list_born_before_1960(movie_client: MovieClient) -> None:
    persons = movie_client.person.list(max_birth_year=1960, limit=-1)

    assert len(persons) > 0
    people_born_after_1960 = [person for person in persons if person.birth_year > 1960]
    assert not people_born_after_1960, "Found people born after 1960"


def test_person_apply_multiple(movie_client: MovieClient) -> None:
    # Arrange
    persons = [movie.PersonApply(external_id=f"person:anders{i}", name=f"Anders{i}", birth_year=0) for i in range(2)]

    # Act
    try:
        result = movie_client.person.apply(persons)

        assert len(result.nodes) == 2
    finally:
        movie_client.person.delete([person.external_id for person in persons])
