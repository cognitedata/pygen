from __future__ import annotations

import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.actors import ActorsAPI
from ._api.best_directors import BestDirectorsAPI
from ._api.best_leading_actors import BestLeadingActorsAPI
from ._api.best_leading_actress import BestLeadingActressAPI
from ._api.directors import DirectorsAPI
from ._api.movies import MoviesAPI
from ._api.nominations import NominationsAPI
from ._api.persons import PersonsAPI
from ._api.ratings import RatingsAPI
from ._api.roles import RolesAPI


class MovieClient:
    """
    Movie Client

    Generated with:
        pygen = 0.10.0
        cognite-sdk = 6.4.8
        pydantic = 1.10.9

    Data Model:
        space: IntegrationTestsImmutable
        externalId: Movie
        version: 2
    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        self.actors = ActorsAPI(client)
        self.best_directors = BestDirectorsAPI(client)
        self.best_leading_actors = BestLeadingActorsAPI(client)
        self.best_leading_actress = BestLeadingActressAPI(client)
        self.directors = DirectorsAPI(client)
        self.movies = MoviesAPI(client)
        self.nominations = NominationsAPI(client)
        self.persons = PersonsAPI(client)
        self.ratings = RatingsAPI(client)
        self.roles = RolesAPI(client)

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
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
