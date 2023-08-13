from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

try:
    from pydantic import BaseModel, FieldValidationInfo, field_validator

    is_pydantic_v2 = True
except ImportError:
    from pydantic import BaseModel, validator

    is_pydantic_v2 = False


class Argument(BaseModel):
    default: Optional[str] = None
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
    client_id: Argument = Argument(default=None, help="Azure Client ID for connecting to CDF")
    cdf_cluster: Argument = Argument(default=None, help="CDF Cluster to connect to")
    cdf_project: Argument = Argument(default=None, help="CDF Project to connect to")
    output_dir: Argument = Argument(default=None, help="Output directory for generated SDK")
    top_level_package: Argument = Argument(default="my_domain.client", help="Package name for the generated client.")
    client_name: Argument = Argument(default="MyClient", help="Client name for the generated client.")

    if is_pydantic_v2:

        @field_validator("*", mode="before")
        def parse_string(cls, value, info: FieldValidationInfo) -> Argument:
            field = cls.model_fields[info.field_name]
            return _parse_string(value, field)

    else:

        @validator("*", pre=True)  # type: ignore[type-var]
        def parse_string(cls, value, field) -> Argument:
            return _parse_string(value, field)


def _load_pyproject_toml(pyproject_toml_path: Path | None = None) -> dict[str, Any]:
    if pyproject_toml_path is None:
        pyproject_toml_path = Path.cwd() / "pyproject.toml"
    if not pyproject_toml_path.exists():
        return {}
    import toml

    return toml.loads(pyproject_toml_path.read_text())


def load_settings(pyproject_toml_path: Path | None = None) -> PygenSettings | None:
    pyproject_toml = _load_pyproject_toml(pyproject_toml_path)
    if "pygen" in pyproject_toml.get("tool", {}):
        return PygenSettings(**pyproject_toml["tool"]["pygen"])
    return None
