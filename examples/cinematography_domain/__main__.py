from __future__ import annotations

import logging
from typing import List, cast

from cinematography_domain.client import CineClient, get_cine_client
from cinematography_domain.schema import Movie, Person

from cognite.dm_clients.custom_types import JSONObject, Timestamp


def _delete_data(client: CineClient) -> None:
    client.person.delete(client.person.list(resolve_relationships=False))
    client.movie.delete(client.movie.list(resolve_relationships=False))


def _upload_data(client: CineClient) -> None:
    """Uploading some dummy instances (a.k.a. "items")."""

    movies = [
        Movie(
            externalId="movie1",
            title="Casablanca",
            release="1942-11-26T11:12:13Z",  # type: ignore[arg-type]
            director=Person(externalId="person1", name="Michael Curtiz"),
            actors=[
                Person(externalId="person2", name="Humphrey Bogart"),
                Person(externalId="person3", name="Ingrid Bergman"),
            ],
            meta={"run_time": 102},  # type: ignore[arg-type]
        ),
        Movie(
            externalId="movie2",
            title="Star Wars: Episode IV – A New Hope",
            release=Timestamp("1977-05-25T00:00:00-06"),
            director=Person(externalId="person4", name="George Lucas"),
            actors=[
                Person(externalId="person5", name="Mark Hamill"),
                Person(externalId="person6", name="Harrison Ford"),
                Person(externalId="person7", name="Carrie Fisher"),
            ],
            meta=JSONObject({"run_time": 121}),
        ),
    ]
    client.movie.create(movies)


def _main() -> None:
    logging.basicConfig()
    logging.getLogger("cognite.dm_clients").setLevel("DEBUG")

    client = get_cine_client()

    # _delete_data(client)

    persons = client.person.list()
    if not persons:
        print("(first time only) Populating DM with data.\n")
        _upload_data(client)

    movies = client.movie.list()
    for movie in movies:
        print(movie.title)
        print(f"  - released: {movie.release.datetime().date() if movie.release else 'N/A'}")
        print(f"  - directed by: {movie.director.name if movie.director else 'N/A'}")
        actors = cast(List[Person], movie.actors or [])  # to keep mypy happy *
        print("  * starring *")
        for actor in actors:
            print(f"  {actor.name}")
        print()

    # * in DM we cannot do `actors: [Person!]` nor `[Person!]!`, we can only do `[Person]`


if __name__ == "__main__":
    _main()
