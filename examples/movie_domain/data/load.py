from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from examples.movie_domain.sdk.data_classes import (
    Actor,
    BestDirector,
    BestLeadingActor,
    BestLeadingActress,
    Director,
    Movie,
    Nomination,
    Person,
    Role,
)

_this_file = Path(__file__).resolve().parent


@dataclass
class MovieModel:
    movies: list[Movie]
    persons: list[Person]
    directors: list[Director]
    actors: list[Actor]
    roles: list[Role]
    nomination: list[Nomination]
    best_directors: list[BestDirector]
    best_actors: list[BestLeadingActor]
    best_actress: list[BestLeadingActress]


def load() -> MovieModel:
    movie_df = pd.read_csv(_this_file / "movies.csv")
    person_df = pd.read_csv(_this_file / "persons.csv")
    role_df = pd.read_csv(_this_file / "roles.csv")
    relation_df = pd.read_csv(_this_file / "relation_role_movies.csv")
    nomination_df = pd.read_csv(_this_file / "nominations.csv")

    movies = [Movie(**entry) for entry in movie_df.to_dict(orient="records")]
    persons = {entry["name"]: Person(**entry) for entry in person_df.to_dict(orient="records")}
    actors = [
        Actor(**entry, person=persons[entry["person_name"]])
        for entry in role_df.to_dict(orient="records")
        if entry["role"] == "actor"
    ]
    directors = [
        Director(**entry, person=persons[entry["person_name"]])
        for entry in role_df.to_dict(orient="records")
        if entry["role"] == "director"
    ]
    all_nominations: list[Nomination] = []

    roles: list[Role] = actors.copy()
    roles.extend(directors)

    for role in roles:
        role_type = "actor" if isinstance(role, Actor) else "director"

        is_role = (relation_df["person_name"] == role.person.name) & (relation_df["role"] == role_type)
        in_movie = set(relation_df.loc[is_role, "movie"])
        role.movies = [movie for movie in movies if movie.title in in_movie]

        nominations = []
        for entry in nomination_df.to_dict(orient="records"):
            if entry["person"] != role.person.name:
                continue
            NominationClass = {  # noqa
                "Best Director": BestDirector,
                "Best Actor in a Leading Role": BestLeadingActor,
                "Best Actress in a Leading Role": BestLeadingActress,
            }[entry["name"]]
            nomination = NominationClass(**entry)
            nominations.append(nomination)
            all_nominations.append(nomination)
        role.nomination = nominations

    for person in persons.values():
        person.roles = [role for role in roles if role.person.name == person.name]

    for movie in movies:
        is_movie = relation_df["movie"] == movie.title
        in_movie = set(relation_df.loc[is_movie & (relation_df["role"] == "actor"), "person_name"])
        movie.actors = [actor for actor in actors if actor.person.name in in_movie]

        in_movie = set(relation_df.loc[is_movie & (relation_df["role"] == "director"), "person_name"])
        movie.directors = [director for director in directors if director.person.name in in_movie]

    return MovieModel(
        movies,
        list(persons.values()),
        directors,
        actors,
        roles,
        all_nominations,
        [nomination for nomination in all_nominations if isinstance(nomination, BestDirector)],
        [nomination for nomination in all_nominations if isinstance(nomination, BestLeadingActor)],
        [nomination for nomination in all_nominations if isinstance(nomination, BestLeadingActress)],
    )


if __name__ == "__main__":
    model = load()
    print("Model is loaded")  # noqa
