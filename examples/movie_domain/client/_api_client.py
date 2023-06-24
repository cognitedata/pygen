from __future__ import annotations

import getpass
from pathlib import Path

import toml
from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

import movie_domain.client.data_classes.persons

from . import data_classes
from ._api.persons import PersonsAPI


class MovieClient:
    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        # self.movies = api.MovieAPI(
        #     data_classes.Movie,
        #     list_data_classes.MovieList,
        # )
        self.persons = PersonsAPI(
            client,
            dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
            data_classes.Person,
            data_classes.PersonApply,
            movie_domain.client.data_classes.persons.PersonList,
        )
        # self.ratings = api.RatingsAPI(data_classes.Rating, list_data_classes.RatingList)
        # self.actors = api.ActorsAPI(data_classes.Actor, list_data_classes.ActorList)
        # self.directors = api.DirectorAPI(data_classes.Director, list_data_classes.DirectorList)
        # self.best_directors = api.BestDirectorAPI(data_classes.BestDirector, list_data_classes.BestDirectorList)
        # self.best_leading_actor = api.BestLeadingActorAPI(
        #     data_classes.BestLeadingActor, list_data_classes.BestLeadingActorList
        # )
        # self.best_leading_actress = api.BestLeadingActressAPI(
        #     data_classes.BestLeadingActress, list_data_classes.BestLeadingActressList
        # )

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> MovieClient:
        base_url = f"https://{cdf_cluster}.cognitedata.com/"
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"{base_url}.default"],
        )
        config = ClientConfig(
            project=project,
            credentials=credentials,
            client_name=getpass.getuser(),
            base_url=base_url,
        )

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str) -> MovieClient:
        return cls.azure_project(**toml.load(file_path)["cognite"])
