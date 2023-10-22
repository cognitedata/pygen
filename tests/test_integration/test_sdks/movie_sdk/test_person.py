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


def test_person_apply_multiple_requests(movie_client: MovieClient) -> None:
    # Arrange
    person = movie.PersonApply(
        external_id="person1",
        name="Person 1",
        birth_year=1990,
        roles=[
            movie.RoleApply(
                external_id="actor1",
                person="person1",
                won_oscar=True,
                movies=[
                    movie.MovieApply(
                        external_id="movie1",
                        title="Movie 1",
                        release_year=2020,
                        actors=["actor1"],
                        run_time_minutes=120,
                    )
                ],
            ),
        ],
    )

    limit = movie_client.person._client.data_modeling.instances._CREATE_LIMIT
    try:
        movie_client.person._client.data_modeling.instances._CREATE_LIMIT = 1

        # Act
        movie_client.person.apply(person)
    finally:
        movie_client.person._client.data_modeling.instances._CREATE_LIMIT = limit

    instances = person.to_instances_apply()
    movie_client.person._client.data_modeling.instances.delete(instances.nodes.as_ids(), instances.edges.as_ids())


def test_list_above_5000_persons(movie_client: MovieClient) -> None:
    # Arrange
    persons = [
        movie.PersonApply(external_id=f"person_5000:{i}", birth_year=1980, name=f"Person {i}") for i in range(5001)
    ]
    movie_client.person.apply(persons)

    # Act
    persons = movie_client.person.list(limit=-1, external_id_prefix="person_5000:")

    # Assert
    assert len(persons) == 5001


def test_search_person(movie_client: MovieClient) -> None:
    # Act
    results = movie_client.person.search("Quentin", limit=1)

    # Assert
    assert len(results) == 1
    assert results[0].external_id == "person:quentin_tarantino"
    assert results[0].name == "Quentin Tarantino"
    assert results[0].birth_year == 1963
