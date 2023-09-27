from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.actor import ActorAPI
from ._api.best_director import BestDirectorAPI
from ._api.best_leading_actor import BestLeadingActorAPI
from ._api.best_leading_actress import BestLeadingActressAPI
from ._api.director import DirectorAPI
from ._api.movie import MovieAPI
from ._api.nomination import NominationAPI
from ._api.person import PersonAPI
from ._api.rating import RatingAPI
from ._api.role import RoleAPI


class MovieClient:
    """
    MovieClient

    Generated with:
        pygen = 0.21.1
        cognite-sdk = 6.25.3
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
        self.actor = ActorAPI(client, dm.ViewId("IntegrationTestsImmutable", "Actor", "2"))
        self.best_director = BestDirectorAPI(client, dm.ViewId("IntegrationTestsImmutable", "BestDirector", "2"))
        self.best_leading_actor = BestLeadingActorAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "BestLeadingActor", "2")
        )
        self.best_leading_actress = BestLeadingActressAPI(
            client, dm.ViewId("IntegrationTestsImmutable", "BestLeadingActress", "2")
        )
        self.director = DirectorAPI(client, dm.ViewId("IntegrationTestsImmutable", "Director", "2"))
        self.movie = MovieAPI(client, dm.ViewId("IntegrationTestsImmutable", "Movie", "2"))
        self.nomination = NominationAPI(client, dm.ViewId("IntegrationTestsImmutable", "Nomination", "2"))
        self.person = PersonAPI(client, dm.ViewId("IntegrationTestsImmutable", "Person", "2"))
        self.rating = RatingAPI(client, dm.ViewId("IntegrationTestsImmutable", "Rating", "2"))
        self.role = RoleAPI(client, dm.ViewId("IntegrationTestsImmutable", "Role", "2"))

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
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
