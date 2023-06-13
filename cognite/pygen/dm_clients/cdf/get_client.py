from __future__ import annotations

import getpass
from pathlib import Path
from typing import List, Optional, Union

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, OAuthInteractive
from pydantic import BaseSettings, validator

from cognite.pygen.dm_clients.config import settings


class CogniteConfig(BaseSettings):
    project: str
    cdf_cluster: str
    tenant_id: str
    client_id: Optional[str]
    client_secret: Optional[str]
    token_client_id: Optional[str]
    token_client_secret: Optional[str]
    token_scopes: Optional[List[str]]
    token_url: Optional[str]

    client_name: str = ""
    authority_host_uri: str = "https://login.microsoftonline.com"
    port: int = 53000
    token_cache_path: Optional[Path] = None

    @property
    def base_url(self) -> str:
        return f"https://{self.cdf_cluster}.cognitedata.com/"

    @property
    def scopes(self) -> list[str]:
        return [f"{self.base_url}.default"]

    @property
    def authority_uri(self) -> str:
        return f"{self.authority_host_uri}/{self.tenant_id}"

    @validator("client_name", always=True)
    def replace_none_with_user(cls, value):
        return getpass.getuser() if value is None else value


def get_cognite_config() -> CogniteConfig:
    return CogniteConfig(**settings.get("cognite", {}))


def get_client_config(cognite_config: Optional[CogniteConfig] = None) -> ClientConfig:
    if cognite_config is None:
        cognite_config = get_cognite_config()

    credentials: Union[OAuthClientCredentials, OAuthInteractive]
    if cognite_config.client_id and cognite_config.client_secret:
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{cognite_config.tenant_id}/oauth2/v2.0/token",
            client_id=cognite_config.client_id,
            client_secret=cognite_config.client_secret,
            scopes=cognite_config.scopes,
        )
    elif cognite_config.client_id and cognite_config.authority_uri:
        credentials = OAuthInteractive(
            authority_url=cognite_config.authority_uri,
            client_id=cognite_config.client_id,
            scopes=cognite_config.scopes,
            redirect_port=cognite_config.port,
            token_cache_path=cognite_config.token_cache_path,  # type: ignore
        )
    else:
        raise ValueError("Missing authentication details for CDF")
    return ClientConfig(
        client_name=cognite_config.client_name,
        project=cognite_config.project,
        base_url=cognite_config.base_url,
        credentials=credentials,
    )


def get_cognite_client() -> CogniteClient:
    client_config = get_client_config()
    return CogniteClient(client_config)
