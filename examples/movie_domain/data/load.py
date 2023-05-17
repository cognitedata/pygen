from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from examples.movie_domain.sdk.core import NumericDataPoint, TimeSeries
from examples.movie_domain.sdk.data_classes import (
    Actor,
    BestDirector,
    BestLeadingActor,
    BestLeadingActress,
    Director,
    Movie,
    Nomination,
    Person,
    Rating,
    Role,
)

_this_file = Path(__file__).resolve().parent


@dataclass
class MovieModel:
    movies: list[Movie]
    ratings: list[Rating]
    persons: list[Person]
    directors: list[Director]
    actors: list[Actor]
    roles: list[Role]
    nomination: list[Nomination]
    best_directors: list[BestDirector]
    best_leading_actors: list[BestLeadingActor]
    best_leading_actress: list[BestLeadingActress]


def to_id(raw: str) -> str:
    return raw.lower().replace(" ", "_")


def load() -> MovieModel:
    movie_df = pd.read_csv(_this_file / "movies.csv")
    person_df = pd.read_csv(_this_file / "persons.csv")
    role_df = pd.read_csv(_this_file / "roles.csv")
    relation_df = pd.read_csv(_this_file / "relation_role_movies.csv")
    nomination_df = pd.read_csv(_this_file / "nominations.csv")
    rating_df = pd.read_csv(_this_file / "ratings.csv")
    rating_df["Date"] = pd.to_datetime(rating_df["Date"], format="%d/%m/%Y").apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    rating_df.set_index("Date", inplace=True)

    ratings = {}
    for movie_title, sub_df in rating_df.groupby("Movie Title"):
        votes = [NumericDataPoint(timestamp=t, value=v) for (t, v) in sub_df["Number of Votes"].items()]
        score = [NumericDataPoint(timestamp=t, value=v) for (t, v) in sub_df["IMDb Rating"].items()]
        movie_id = to_id(str(movie_title))
        rating = Rating(
            external_id=f"rating:{movie_id}",
            score=TimeSeries(external_id=f"rating:{movie_id}", name=f"{movie_title} Rating", data_points=score),
            votes=TimeSeries(external_id=f"vote_count:{movie_id}", name=f"{movie_title} Vote Count", data_points=votes),
        )
        ratings[movie_title] = rating

    movies = [
        Movie(**entry, external_id=f"movie:{to_id(entry['title'])}", rating=ratings[entry["title"]])
        for entry in movie_df.to_dict(orient="records")
    ]
    persons = {
        entry["name"]: Person(**entry, external_id=f"person:{to_id(entry['name'])}")
        for entry in person_df.to_dict(orient="records")
    }
    actors = [
        Actor(**entry, person=persons[entry["person_name"]], external_id=f"actor:{to_id(entry['person_name'])}")
        for entry in role_df.to_dict(orient="records")
        if entry["role"] == "actor"
    ]
    directors = [
        Director(**entry, person=persons[entry["person_name"]], external_id=f"director:{to_id(entry['person_name'])}")
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
            NominationClass, prefix = {  # noqa
                "Best Director": (BestDirector, "director:"),
                "Best Actor in a Leading Role": (BestLeadingActor, "leadingactor:"),
                "Best Actress in a Leading Role": (BestLeadingActress, "leadingactress"),
            }[entry["name"]]
            nomination = NominationClass(
                **entry, external_id=f"{prefix}{to_id(entry['person'])}:{to_id(entry['movie'])}"
            )
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
        list(ratings.values()),
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
