from movie_domain.client import MovieClient
from movie_domain.client import data_classes as m


def test_actor_list(movie_client: MovieClient):
    actors = movie_client.actors.list(limit=-1)

    assert len(actors) > 0
    assert all(isinstance(movie, str) for actor in actors for movie in actor.movies)
    assert all(isinstance(nomination, str) for actor in actors for nomination in actor.nominations)


def test_actor_apply_with_person(movie_client: MovieClient):
    # Arrange
    actor = m.ActorApply(
        external_id="actor:anders",
        movies=[],
        nominations=[],
        person=m.PersonApply(external_id="person:anders", name="Anders", birth_year=0),
    )

    try:
        # Act
        created = movie_client.actors.apply(actor)

        # Assert
        assert len(created.nodes) == 2
        assert len(created.edges) == 0
    finally:
        movie_client.actors.delete(actor.external_id)
        movie_client.persons.delete(actor.person.external_id)
