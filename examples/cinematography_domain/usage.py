from __future__ import annotations

import logging
from typing import List, cast

from cinematography_domain.client import CineClient, get_cine_client
from cinematography_domain.schema import MovieSimple, PersonSimple
from cognite.pygen.dm_clients.custom_types import JSONObject, Timestamp


def _delete_data(client: CineClient) -> None:
    print("Deleting MovieSimple and Person data.")
    client.person_simple.delete(client.person_simple.list(resolve_relationships=False))
    client.movie_simple.delete(client.movie_simple.list(resolve_relationships=False))


def _upload_data(client: CineClient) -> None:
    """Uploading some dummy instances (a.k.a. "items")."""

    movies = [
        MovieSimple(
            externalId="movie1",
            title="Casablanca",
            genres=["drama"],
            release="1942-11-26T11:12:13Z",  # type: ignore[arg-type]
            director=PersonSimple(externalId="person1", name="Michael Curtiz"),
            actors=[
                PersonSimple(externalId="person2", name="Humphrey Bogart"),
                PersonSimple(externalId="person3", name="Ingrid Bergman"),
            ],
            meta={"run_time": 102},  # type: ignore[arg-type]
        ),
        MovieSimple(
            externalId="movie2",
            title="Thor",
            genres=["action", "fantasy"],
            release=Timestamp("2011-04-17T00:00:00+10"),
            director=PersonSimple(externalId="person4", name="Kenneth Branagh"),
            actors=[
                PersonSimple(externalId="person5", name="Chris Hemsworth"),
                PersonSimple(externalId="person6", name="Natalie Portman"),
                PersonSimple(externalId="person7", name="Tom Hiddleston"),
            ],
            meta=JSONObject({"run_time": 114}),
        ),
    ]
    client.movie_simple.apply(movies)

    # add another movie, showing the use of _reference
    ragnarok = MovieSimple(
        externalId="movie3",
        title="Thor: Ragnarok",
        genres=["action", "fantasy"],
        release=Timestamp("2017-10-10T00:00:00-08"),
        director=PersonSimple(externalId="person8", name="Taika Waititi"),
        actors=[
            # These persons already exist. We need not fill all fields
            # only externalId and set _reference=True.
            # Note: we could instead fill all fields instead (just "name" in
            # this example), and the API would update the fields. By using
            # _reference=True, we don't update those field, only create the relations
            PersonSimple.ref(externalId="person5"),
            PersonSimple.ref(externalId="person7"),
            PersonSimple(externalId="person9", name="Cate Blanchett"),
        ],
        meta=JSONObject({"run_time": 130}),
    )
    client.movie_simple.apply([ragnarok])

    # add a relationship knowing only external_id values (first create another actor):
    client.person_simple.apply([PersonSimple(externalId="person10", name="Tessa Thompson")])
    client.movie_simple.connect.actors("movie3", ["person10"])


def _main() -> None:
    logging.basicConfig()
    logging.getLogger("cognite.pygen.dm_clients").setLevel("DEBUG")

    client = get_cine_client()

    # _delete_data(client)

    persons = client.person_simple.list()
    if not persons:
        print("(first time only) Populating DM with data.\n")
        _upload_data(client)

    movies = client.movie_simple.list()
    for movie in movies:
        print(movie.title)
        print(f"  - released: {movie.release.datetime().date() if movie.release else 'N/A'}")
        print(f"  - directed by: {movie.director.name if movie.director else 'N/A'}")
        print(f"  - genres: {', '.join(movie.genres)}")
        actors = cast(List[PersonSimple], movie.actors or [])  # to keep mypy happy *
        print("  * starring *")
        for actor in actors:
            print(f"  {actor.name}")
        print()

    # * in DM we cannot do `actors: [Person!]` nor `[Person!]!`, we can only do `[Person]`


if __name__ == "__main__":
    _main()
