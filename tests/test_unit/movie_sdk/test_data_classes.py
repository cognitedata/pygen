# from examples.movie_domain.client.data_classes.data_classes import Movie
# from examples.movie_domain.data.load import MovieModel
# from examples.movie_domain.local.local import MovieClientLocal
# from movie_domain.client import MovieClient
from cognite.client import data_modeling as dm

from examples.movie_domain.client import data_classes as movie

#
# @pytest.fixture()
# def pulp_fiction_movie(movie_model) -> Movie:
#     return next(movie for movie in movie_model.movies if movie.title == "Pulp Fiction")
#
#
# @pytest.fixture()
# def local_client(movie_model) -> MovieClientLocal:
#     return MovieClientLocal(movie_model, MovieClient())
#
#
# def test_dump_circular_model(movie_model: MovieModel):
#     a_movie = movie_model.movies[0]
#
#     assert a_movie.dict(exclude={"external_id"})
#
#
# def test_repr_circular_model(movie_model: MovieModel):
#     an_actor = movie_model.actors[0]
#
#     repr(an_actor)
#
#
# def test_local_movie_client(local_client):
#     local_client.movies.list()
#
#
# def test_traverse_circular_model_depth_0(pulp_fiction_movie: Movie):
#     assert pulp_fiction_movie.traverse(depth=0).dict() == pulp_fiction_movie.copy().dict()
#
#
# def test_traverse_circular_model_depth_1(pulp_fiction_movie: Movie):
#     # Arrange
#     output = pulp_fiction_movie.copy()
#     output.directors = [director.copy() for director in pulp_fiction_movie.directors]
#     output.actors = [actor.copy() for actor in pulp_fiction_movie.actors]
#
#     # Act and Assert
#     assert pulp_fiction_movie.traverse(depth=1) == output
#
#
# def test_traverse_circular_model_depth_2(pulp_fiction_movie: Movie):
#     # Arrange
#     output = pulp_fiction_movie.copy()
#     directors = []
#     for director in pulp_fiction_movie.directors:
#         new_copy = director.copy()
#         new_copy.person = director.person.copy()
#         new_copy.nomination = [nomination.copy() for nomination in director.nomination]
#         new_copy.movies = [output if movie is pulp_fiction_movie else movie.copy() for movie in director.movies]
#         directors.append(new_copy)
#     output.directors = directors
#
#     actors = []
#     for actor in pulp_fiction_movie.actors:
#         new_copy = actor.copy()
#         new_copy.person = actor.person.copy()
#         new_copy.nomination = [nomination.copy() for nomination in actor.nomination]
#         new_copy.movies = [output if movie is pulp_fiction_movie else movie.copy() for movie in actor.movies]
#         actors.append(new_copy)
#     output.actors = actors
#
#     # Act and Assert
#     assert pulp_fiction_movie.traverse(depth=1) == output
#
#
# def test_retrieve_movie(local_client: MovieClientLocal, pulp_fiction_movie: Movie):
#     pulp_fiction = local_client.movies.retrieve(pulp_fiction_movie.external_id, traversal_count=2)
#
#     assert pulp_fiction.actors is not None
#     assert pulp_fiction.actors[0].person is not None
#
#
# def test_retrieve_person_zero_propagation(local_client: MovieClientLocal):
#     quentin = local_client.persons.retrieve("person:quentin_tarantino")
#
#     assert quentin.roles is None


def test_person_from_node():
    # Arrange
    node = dm.Node.load(
        {
            "instance_type": "node",
            "space": "IntegrationTestsImmutable",
            "external_id": "person:christoph_waltz",
            "version": 1,
            "last_updated_time": 1684170308732,
            "created_time": 1684170308732,
            "properties": {"IntegrationTestsImmutable": {"Person/2": {"name": "Christoph Waltz", "birthYear": 1956}}},
        }
    )

    # Act
    person = movie.Person.from_node(node)

    # Assert
    assert person.name == "Christoph Waltz"


def test_person_one_to_many_fields():
    # Arrange
    expected = ["roles"]

    # Act
    actual = movie.Person.one_to_many_fields()

    # Assert
    assert actual == expected


def test_person_apply_to_node_apply():
    # Arrange
    person = movie.PersonApply(name="Christoph Waltz", birth_year=1956, external_id="person:christoph_waltz")
    expected = dm.NodeApply(
        "IntegrationTestsImmutable",
        "person:christoph_waltz",
        sources=[
            dm.NodeOrEdgeData(
                source=dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
                properties={"name": "Christoph Waltz", "birthYear": 1956},
            )
        ],
    )

    # Act
    actual = person.to_node()

    # Assert
    assert actual == expected
