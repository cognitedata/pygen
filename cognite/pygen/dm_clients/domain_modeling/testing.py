from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Literal, Type
from unittest.mock import MagicMock
from urllib.parse import urlparse

from cachelib import SimpleCache
from cognite.client import ClientConfig
from cognite.client.credentials import CredentialProvider
from cognite.client.testing import monkeypatch_cognite_client
from pydantic import BaseModel, Field
from requests.models import Response

from cognite.pygen.dm_clients.cdf.data_classes_dm_v3 import View
from cognite.pygen.dm_clients.domain_modeling import DomainClient, DomainModelAPI, Schema
from cognite.pygen.dm_clients.domain_modeling.schema import DomainModelT
from cognite.pygen.dm_clients.misc import to_camel


class Config:
    space = "test-space"
    data_model = "test-model"
    client_name = "test-client"
    project = "test-project"
    version = 42

    @classmethod
    @property
    def base_url(cls):
        return f"/api/v1/projects/{cls.project}/models"


class ApplyResponse(BaseModel):
    external_id: str
    instance_type: Literal["node", "edge"] = "node"
    version: int = Config.version
    was_modified: bool = False
    space: str = Config.space
    created_time: int = Field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    last_updated_time: int = Field(default_factory=lambda: datetime.now(timezone.utc).timestamp())

    class Config:
        alias_generator = to_camel
        allow_population_by_field_name = True


@contextmanager
def create_test_client_factory(
    ClientClass: Type[DomainClient],  # noqa: N803
    cine_schema: Schema[DomainModelT],
    return_jsons: list[list[dict[str, Any]]] = None,
) -> Type[DomainClient]:
    return_jsons = return_jsons or []
    with monkeypatch_cognite_client() as client:
        config = ClientConfig(
            client_name=Config.client_name, project=Config.project, credentials=MagicMock(spec_set=CredentialProvider)
        )
        client._config = config

        def _mock_get(url: str, params: dict[str, Any] = None, headers: dict[str, Any] = None) -> Response:
            url = urlparse(url)
            response = MagicMock(spec=Response)
            response.status_code = 200

            if url.path == f"{Config.base_url}/views":
                return_value = []
                for _, class_ in cine_schema.types_map.items():
                    view = View(
                        space=Config.space,
                        externalId=class_.__name__,
                        version=str(Config.version),
                        name=class_.__name__,
                        properties={},
                    )
                    return_value.append(view.dict())
                response.json.return_value = {"items": return_value}

            return response

        def mock_post(
            url: str, json: dict[str, Any], params: dict[str, Any] = None, headers: dict[str, Any] = None
        ) -> Response:
            response = MagicMock(spec=Response)
            response.status_code = 200
            if return_jsons:
                response.json.return_value = {"items": return_jsons.pop(0)}
            return response

        client.get = _mock_get
        client.post = mock_post

        dm_client = ClientClass(
            schema=cine_schema,
            domain_model_api_class=DomainModelAPI,
            cache=SimpleCache(),
            config=config,
            space_id=Config.space,
            data_model=Config.data_model,
            schema_version=Config.version,
        )
        dm_client.cognite_client = client
        yield dm_client
