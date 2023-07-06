from __future__ import annotations

import getpass
from pathlib import Path
from typing import Optional

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from pydantic import BaseModel, FieldValidationInfo, field_validator


class Argument(BaseModel):
    default: Optional[str] = None
    help: str


class PygenSettings(BaseModel):
    space: Argument = Argument(default=None, help="Location of Data Model")
    external_id: Argument = Argument(default=None, help="External ID of Data Model")
    version: Argument = Argument(default=None, help="Version of Data Model")
    tenant_id: Argument = Argument(default=None, help="Azure Tenant ID for connecting to CDF")
    client_id: Argument = Argument(default=None, help="Azure Client ID for connecting to CDF")
    cdf_cluster: Argument = Argument(default=None, help="CDF Cluster to connect to")
    cdf_project: Argument = Argument(default=None, help="CDF Project to connect to")
    output_dir: Argument = Argument(default=None, help="Output directory for generated SDK")
    top_level_package: Argument = Argument(default="my_domain.client", help="Package name for the generated client.")
    client_name: Argument = Argument(default="MyClient", help="Client name for the generated client.")

    @field_validator("*", mode="before")
    def parse_string(cls, value, info: FieldValidationInfo) -> Argument:  # noqa: N805
        field = cls.model_fields[info.field_name]
        if value and isinstance(value, str):
            help_ = field.default.help
            return Argument(default=value, help=help_)
        return field.default


def load_settings(pyproject_toml_path: Path) -> PygenSettings | None:
    if not pyproject_toml_path.exists():
        return None
    import toml

    pyproject_toml = toml.loads(pyproject_toml_path.read_text())
    if "pygen" in pyproject_toml.get("tool", {}):
        return PygenSettings(**pyproject_toml["tool"]["pygen"])
    return None


def get_cognite_client(project, cdf_cluster, tenant_id, client_id, client_secret):
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
    return CogniteClient(config)


def load_cognite_client_from_toml(file_path: Path | str, section: str | None = "cognite") -> CogniteClient:
    import toml

    return get_cognite_client(**toml.load(Path(file_path))[section])
