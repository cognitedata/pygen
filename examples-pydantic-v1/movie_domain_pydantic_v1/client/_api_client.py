from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.actor import ActorsAPI
from ._api.best_director import BestDirectorsAPI
from ._api.best_leading_actor import BestLeadingActorsAPI
from ._api.best_leading_actress import BestLeadingActressesAPI
from ._api.director import DirectorsAPI
from ._api.movie import MoviesAPI
from ._api.nomination import NominationsAPI
from ._api.person import PersonsAPI
from ._api.rating import RatingsAPI
from ._api.role import RolesAPI


class MovieClient:
    """
    MovieClient

    Generated with:
        pygen = 0.18.1
        cognite-sdk = 6.25.1
        pydantic = 1.10.7

    Data Model:
        space: IntegrationTestsImmutable
        externalId: Movie
        version: 2
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.actor = ActorsAPI(client)
        self.best_director = BestDirectorsAPI(client)
        self.best_leading_actor = BestLeadingActorsAPI(client)
        self.best_leading_actress = BestLeadingActressesAPI(client)
        self.director = DirectorsAPI(client)
        self.movie = MoviesAPI(client)
        self.nomination = NominationsAPI(client)
        self.person = PersonsAPI(client)
        self.rating = RatingsAPI(client)
        self.role = RolesAPI(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> MovieClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> MovieClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError:
                raise ValueError(f"Could not find section '{section}' in {file_path}")

        return cls.azure_project(**toml_content)
