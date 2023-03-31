from __future__ import annotations

import logging
from typing import List, cast

from cinematography_domain.client import CineClient, get_cine_client
from cinematography_domain.schema import Movie, Person


def _upload_data(client: CineClient) -> None:
    """Uploading some dummy instances (a.k.a. "items")."""
    client.person.delete(client.person.list(resolve_relationships=False))
    client.movie.delete(client.movie.list(resolve_relationships=False))

    movies = [
        Movie(
            externalId="movie1",
            title="Casablanca",
            director=Person(externalId="person1", name="Michael Curtiz"),
            actors=[
                Person(externalId="person2", name="Humphrey Bogart"),
                Person(externalId="person3", name="Ingrid Bergman"),
            ],
        ),
        Movie(
            externalId="movie2",
            title="Star Wars: Episode IV – A New Hope",
            director=Person(externalId="person4", name="George Lucas"),
            actors=[
                Person(externalId="person5", name="Mark Hamill"),
                Person(externalId="person6", name="Harrison Ford"),
                Person(externalId="person7", name="Carrie Fisher"),
            ],
        ),
    ]
    client.movie.create(movies)


def _main() -> None:
    logging.basicConfig()
    logging.getLogger("cognite.fdm").setLevel("DEBUG")

    client = get_cine_client()
    persons = client.person.list()

    if not persons:
        print("(first time only) Populating FDM with data.\n")
        _upload_data(client)

    movies = client.movie.list()
    for movie in movies:
        print(movie.title)
        print(f"  - directed by: {movie.director.name if movie.director else 'None'}")
        actors = cast(List[Person], movie.actors or [])  # to keep mypy happy *
        print("  * starring *")
        for actor in actors:
            print(f"  {actor.name}")
        print()

    # * in FDM we cannot do `actors: [Person!]` nor `[Person!]!`, we can only do `[Person]`


if __name__ == "__main__":
    _main()
