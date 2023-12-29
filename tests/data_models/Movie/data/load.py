#
#
#
# # from examples.movie_domain.client.data_classes.data_classes import (
# #     Actor,
# #     BestDirector,
# #     BestLeadingActor,
# #     BestLeadingActress,
# #     Director,
# #     Movie,
# #     Nomination,
# #     Person,
# #     Rating,
# #     Role,
#
#
#
# @dataclass
# class MovieModel:
#
#
# def to_id(raw: str) -> str:
#
#
# def load() -> MovieModel:
#     rating_df["Date"] = pd.to_datetime(rating_df["Date"], format="%d/%m/%Y").apply(
#
#     for movie_title, sub_df in rating_df.groupby("Movie Title"):
#
#         for entry in movie_df.to_dict(orient="records")
#         for entry in person_df.to_dict(orient="records")
#         for entry in role_df.to_dict(orient="records")
#         if entry["role"] == "actor"
#         for entry in role_df.to_dict(orient="records")
#         if entry["role"] == "director"
#
#
#     for role in roles:
#
#
#         for entry in nomination_df.to_dict(orient="records"):
#             if entry["person"] != role.person.name:
#                 "Best Director": (BestDirector, "director:"),
#                 "Best Actor in a Leading Role": (BestLeadingActor, "leadingactor:"),
#                 "Best Actress in a Leading Role": (BestLeadingActress, "leadingactress"),
#             }[entry["name"]]
#                 **entry, external_id=f"{prefix}{to_id(entry['person'])}:{to_id(entry['movie'])}"
#
#     for person in persons.values():
#
#     for movie in movies:
#
#
#     return MovieModel(
#         movies,
#         directors,
#         actors,
#         roles,
#         all_nominations,
#
#
# if __name__ == "__main__":
