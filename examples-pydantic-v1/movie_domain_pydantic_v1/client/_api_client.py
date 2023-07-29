from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from movie_domain_pydantic_v1.client._api.actors import ActorsAPI
from movie_domain_pydantic_v1.client._api.best_directors import BestDirectorsAPI
from movie_domain_pydantic_v1.client._api.best_leading_actors import BestLeadingActorsAPI
from movie_domain_pydantic_v1.client._api.best_leading_actresses import BestLeadingActressesAPI
from movie_domain_pydantic_v1.client._api.directors import DirectorsAPI
from movie_domain_pydantic_v1.client._api.movies import MoviesAPI
from movie_domain_pydantic_v1.client._api.nominations import NominationsAPI
from movie_domain_pydantic_v1.client._api.persons import PersonsAPI
from movie_domain_pydantic_v1.client._api.ratings import RatingsAPI
from movie_domain_pydantic_v1.client._api.roles import RolesAPI


class MovieClient:
    """
    MovieClient

    Generated with:
        pygen = 0.12.3
        cognite-sdk = 6.8.4
        pydantic = 1.10.7

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
        self.best_leading_actresses = BestLeadingActressesAPI(client)
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
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str) -> MovieClient:
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
