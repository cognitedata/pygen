from __future__ import annotations

from pathlib import Path
from typing import Any, Union

try:
    from pydantic import BaseModel, Field, FieldValidationInfo, field_validator  # type: ignore[attr-defined]

    is_pydantic_v2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator

    is_pydantic_v2 = False


class Argument(BaseModel):
    default: Union[str, bool, None] = None
    help: str


def _parse_string(value: str, field) -> Argument:
    if value and isinstance(value, str):
        help_ = field.default.help
        return Argument(default=value, help=help_)
    return field.default


class PygenSettings(BaseModel):
    space: Argument = Argument(default=None, help="Location of Data Model")
    external_id: Argument = Argument(default=None, help="External ID of Data Model")
    version: Argument = Argument(default=None, help="Version of Data Model")
    tenant_id: Argument = Argument(default=None, help="Azure Tenant ID for connecting to CDF")
    token_url: Argument = Argument(default=None, help="Identity providers token url (non AzureAD)")
    scopes: Argument = Argument(default=None, help="Scopes (non AzureAD)")
    audience: Argument = Argument(default=None, help="Audience (non AzureAD)")
    client_id: Argument = Argument(default=None, help="Client ID for connecting to CDF")
    client_secret: Argument = Argument(default=None, help="Client secret for connecting to CDF")
    cdf_cluster: Argument = Argument(default=None, help="CDF Cluster to connect to")
    cdf_project: Argument = Argument(default=None, help="CDF Project to connect to")
    output_dir: Argument = Argument(default=None, help="Output directory for generated SDK")
    top_level_package: Argument = Argument(default="my_domain.client", help="Package name for the generated client.")
    client_name: Argument = Argument(default="MyClient", help="Client name for the generated client.")
    overwrite: Argument = Argument(
        default=False, help="Whether to overwrite existing files in output directory with the new SDK."
    )
    skip_formatting: Argument = Argument(default=False, help="Whether to skip formatting the generated SDK with black.")
    data_models: list[tuple[str, str, str]] = Field(
        default_factory=list,
        help="Data models to generate SDK for.",
    )  # type: ignore[call-arg]

    if is_pydantic_v2:

        @field_validator("*", mode="before")
        def parse_string(cls, value, info: FieldValidationInfo) -> Argument:  # type: ignore[valid-type]
            if info.field_name == "data_models":  # type: ignore[attr-defined]
                return value
            field = cls.model_fields[info.field_name]  # type: ignore[attr-defined]
            return _parse_string(value, field)

    else:

        @validator("*", pre=True)  # type: ignore[type-var]
        def parse_string(cls, value, field) -> Argument:
            if field.name == "data_models":
                return value
            return _parse_string(value, field)


def _load_pyproject_toml(pyproject_toml_path: Path | None = None) -> dict[str, Any]:
    if pyproject_toml_path is None:
        pyproject_toml_path = Path.cwd() / "pyproject.toml"
    if not pyproject_toml_path.exists():
        return {}
    import toml

    return toml.loads(pyproject_toml_path.read_text())


def _load_secret_toml(secret_toml_path: Path | None = None) -> dict[str, Any]:
    if secret_toml_path is None:
        secret_toml_path = Path.cwd() / ".secret.toml"
    if not secret_toml_path.exists():
        return {}
    import toml

    content = toml.loads(secret_toml_path.read_text())
    if "cognite" not in content:
        raise ValueError(f"Secret file {secret_toml_path} does not contain a cognite section")
    return content["cognite"]


def load_settings(
    pyproject_toml_path: Path | None = None, secret_toml_path: Path | None = None
) -> PygenSettings | None:
    pyproject_toml = _load_pyproject_toml(pyproject_toml_path)
    secret_toml = _load_secret_toml(secret_toml_path)
    if settings := pyproject_toml.get("tool", {}).get("pygen", {}):
        if "client_secret" in settings:
            raise ValueError("client_secret should not be set in pyproject.toml")
        return PygenSettings(**{**settings, **secret_toml})
    return None
