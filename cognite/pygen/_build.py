"""This module contains code for building a wheel distribution of a pygen generated SDK."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Optional

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen.config import PygenConfig

from ._generator import (
    DataModel,
    _create_folder_name,
    _default_client_name,
    _default_top_level_package,
    _extract_external_id,
    _get_data_model,
    generate_sdk,
)


def build_wheel(
    model_id: DataModel | Sequence[DataModel],
    client: Optional[CogniteClient] = None,
    *,
    top_level_package: Optional[str] = None,
    client_name: Optional[str] = None,
    default_instance_space: str | None = None,
    output_dir: Path = Path("dist"),
    format_code: bool = True,
    config: Optional[PygenConfig] = None,
) -> None:
    """
    Generates a wheel with Python SDK tailored to the given Data Model(s).

    Args:
        model_id: The ID(s) of the data model(s) used to create a tailored SDK. You can also pass in the data model(s)
            directly to avoid fetching them from CDF.
        client: The cognite client used for fetching the data model. This is required if you pass in
            data models ID(s) in the `model_id` argument and not a data model.
        top_level_package: The name of the top level package for the SDK. For example,
            if we have top_level_package=`apm` and the client_name=`APMClient`, then
            the importing the client will be `from apm import APMClient`. If nothing is passed,
            the package will be [external_id:snake] of the first data model given, while
            the client name will be [external_id:pascal_case]
        client_name: The name of the client class. For example, `APMClient`. See above for more details.
        default_instance_space: The default instance space to use for the generated SDK. If not provided,
            the space must be specified when creating, deleting, and retrieving nodes and edges.
        output_dir: The location to output the generated SDK wheel. Defaults to "dist".
        format_code: Whether to format the generated code using black. Defaults to True.
        config: The configuration used to control how to generate the SDK.
    """
    try:
        from build import ProjectBuilder  # type: ignore[import]
    except ImportError:
        raise ImportError(
            "'build' is required to build wheel. Install pygen with `pip install pygen[cli] or "
            "install build directly `pip install build`."
        ) from None

    data_model = _get_data_model(model_id, client, print)
    folder_name = _create_folder_name(
        data_model.as_id() if isinstance(data_model, dm.DataModel) else data_model.as_ids()
    )
    build_dir = Path(tempfile.gettempdir()) / "pygen_build" / folder_name
    if build_dir.exists():
        try:
            shutil.rmtree(build_dir)
        except Exception as e:
            print(f"Failed to clean temporary build directory {build_dir}: {e}")

    external_id = _extract_external_id(data_model)
    if top_level_package is None:
        top_level_package = _default_top_level_package(external_id)
    if client_name is None:
        client_name = _default_client_name(external_id)
    generate_sdk(
        data_model,
        client,
        top_level_package=top_level_package,
        client_name=client_name,
        default_instance_space=default_instance_space,
        output_dir=build_dir,
        overwrite=True,
        format_code=format_code,
        config=config,
    )

    generate_pyproject_toml(build_dir, top_level_package)

    output_dir.mkdir(exist_ok=True, parents=True)
    ProjectBuilder(build_dir).build(distribution="wheel", output_directory=str(output_dir))

    print(f"Generated SDK wheel at {output_dir}")


def generate_pyproject_toml(build_dir: Path, package_name: str) -> None:
    pyproject_toml = build_dir / "pyproject.toml"
    pyproject_toml.write_text(
        f"""[project]
name = "{package_name}"
version = "1.0.0"
dependencies = [
    "cognite-sdk>={cognite_sdk_version}",
    "pydantic>={PYDANTIC_VERSION}",
    "pandas>={pd.__version__}",
]"""
    )
