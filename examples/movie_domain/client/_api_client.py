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
from . import data_classes


class MovieClient:
    """
    MovieClient

    Generated with:
        pygen = 0.30.1
        cognite-sdk = 6.39.1
        pydantic = 2.4.2

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
        view_by_write_class = {
            data_classes.ActorApply: dm.ViewId("IntegrationTestsImmutable", "Actor", "2"),
            data_classes.BestDirectorApply: dm.ViewId("IntegrationTestsImmutable", "BestDirector", "2"),
            data_classes.BestLeadingActorApply: dm.ViewId("IntegrationTestsImmutable", "BestLeadingActor", "2"),
            data_classes.BestLeadingActressApply: dm.ViewId("IntegrationTestsImmutable", "BestLeadingActress", "2"),
            data_classes.DirectorApply: dm.ViewId("IntegrationTestsImmutable", "Director", "2"),
            data_classes.MovieApply: dm.ViewId("IntegrationTestsImmutable", "Movie", "2"),
            data_classes.NominationApply: dm.ViewId("IntegrationTestsImmutable", "Nomination", "2"),
            data_classes.PersonApply: dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
            data_classes.RatingApply: dm.ViewId("IntegrationTestsImmutable", "Rating", "2"),
            data_classes.RoleApply: dm.ViewId("IntegrationTestsImmutable", "Role", "2"),
        }

        self.actor = ActorAPI(client, view_by_write_class)
        self.best_director = BestDirectorAPI(client, view_by_write_class)
        self.best_leading_actor = BestLeadingActorAPI(client, view_by_write_class)
        self.best_leading_actress = BestLeadingActressAPI(client, view_by_write_class)
        self.director = DirectorAPI(client, view_by_write_class)
        self.movie = MovieAPI(client, view_by_write_class)
        self.nomination = NominationAPI(client, view_by_write_class)
        self.person = PersonAPI(client, view_by_write_class)
        self.rating = RatingAPI(client, view_by_write_class)
        self.role = RoleAPI(client, view_by_write_class)

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
