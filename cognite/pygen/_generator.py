from __future__ import annotations

import importlib
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Callable, Literal, Optional, cast, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.exceptions import CogniteAPIError

from cognite.pygen._core.generators import SDKGenerator

from ._settings import _load_pyproject_toml
from .exceptions import DataModelNotFound
from .utils.text import to_pascal, to_snake


def generate_sdk_notebook(
    client: CogniteClient,
    model_id: DataModelIdentifier | Sequence[DataModelIdentifier] | dm.DataModel[dm.View] | dm.DataModelList[dm.View],
    top_level_package: Optional[str] = None,
    client_name: Optional[str] = None,
    logger: Callable[[str], None] | None = None,
    overwrite: bool = True,
    format_code: bool = False,
) -> Any:
    """
    Generates a Python SDK from the given Data Model(s).

    The SDK is generated in a temporary directory and added to the sys.path. This is such that it becomes available
    to be imported in the current Python session.

    Args:
        client: The cognite client used for fetching the data model.
        model_id: The id(s) of the data model(s) to generate the SDK from. You can also pass in the data model(s)
                  directly to avoid fetching them from CDF.
        top_level_package: The name of the top level package for the SDK. Example "movie.client". If nothing is passed
                            the package will [external_id:snake].client of the first data model given.
        client_name: The name of the client class. Example "MovieClient". If nothing is passed the clien name will be
                     [external_id:pascal_case]Client of the first data model given.
        logger: A logger function that will be called with the progress of the generation.
        overwrite: Whether to overwrite the output directory if it already exists. Defaults to True.
        format_code: Whether to format the generated code using black. Defaults to False.

    Returns:
        The instantiated generated client class.
    """
    output_dir = Path(tempfile.gettempdir()) / "pygen"
    logger = logger or print
    identifier = _load_data_model_identifier(model_id)
    external_id = identifier[0].external_id.replace(" ", "_")
    if top_level_package is None:
        top_level_package = f"{to_snake(external_id)}.client"
    if client_name is None:
        client_name = f"{to_pascal(external_id)}Client"

    generate_sdk(
        client,
        model_id,
        top_level_package,
        client_name,
        output_dir,
        logger,
        pydantic_version="infer",
        overwrite=overwrite,
        format_code=format_code,
    )
    if str(output_dir) not in sys.path:
        sys.path.append(str(output_dir))
        logger(f"Added {output_dir} to sys.path to enable import")
    else:
        logger(f"{output_dir} already in sys.path")
    module = vars(importlib.import_module(top_level_package))
    logger(f"Imported {top_level_package}")
    return module[client_name](client)


def generate_sdk(
    client: CogniteClient,
    model_id: DataModelIdentifier | Sequence[DataModelIdentifier] | dm.DataModel[dm.View] | dm.DataModelList[dm.View],
    top_level_package: str,
    client_name: str,
    output_dir: Path,
    logger: Optional[Callable[[str], None]] = None,
    pydantic_version: Literal["v1", "v2", "infer"] = "infer",
    overwrite: bool = False,
    format_code: bool = True,
) -> None:
    """
    Generates a Python SDK from the given Data Model(s).

    Args:
        client: The cognite client used for fetching the data model.
        model_id: The id(s) of the data model(s) to generate the SDK from. You can also pass in the data model(s)
                  directly to avoid fetching them from CDF.
        top_level_package: The name of the top level package for the SDK. Example "movie.client"
        client_name: The name of the client class. Example "MovieClient"
        output_dir: The directory to write the SDK to.
        logger: A logger function to log progress. Defaults to print.
        pydantic_version: The version of pydantic to use. Defaults to "infer" which will use
                          the environment to detect the installed version of pydantic.
        overwrite: Whether to overwrite the output directory if it already exists. Defaults to False.
        format_code: Whether to format the generated code using black. Defaults to True.

    """
    logger = logger or print
    if isinstance(model_id, (dm.DataModel, dm.DataModelList)):
        data_model = model_id
    else:
        data_model = _load_data_model(client, model_id, logger)
        logger(f"Successfully retrieved data model(s) {model_id}")
    sdk_generator = SDKGenerator(top_level_package, client_name, data_model, pydantic_version, logger)
    sdk = sdk_generator.generate_sdk()
    logger(f"Writing SDK to {output_dir}")
    write_sdk_to_disk(sdk, output_dir, overwrite, format_code)
    logger("Done!")


@overload
def _load_data_model(
    client: CogniteClient, model_id: DataModelIdentifier, logger: Callable[[str], None]
) -> dm.DataModel:
    ...


@overload
def _load_data_model(
    client: CogniteClient, model_id: Sequence[DataModelIdentifier], logger: Callable[[str], None]
) -> dm.DataModelList:
    ...


def _load_data_model(
    client: CogniteClient, model_id: DataModelIdentifier | Sequence[DataModelIdentifier], logger: Callable[[str], None]
) -> dm.DataModel[dm.View] | dm.DataModelList[dm.View]:
    identifier = _load_data_model_identifier(model_id)
    try:
        data_models = client.data_modeling.data_models.retrieve(model_id, inline_views=True)
    except CogniteAPIError as e:
        logger(f"Error retrieving data model(s): {e}")
        raise e
    if len(data_models) == 1 == len(identifier):
        return data_models[0]
    elif len(data_models) == len(identifier):
        return data_models
    missing_ids = set(identifier) - set(data_models.as_ids())
    raise DataModelNotFound(list(missing_ids))


def _load_data_model_identifier(
    model_id: DataModelIdentifier | Sequence[DataModelIdentifier] | dm.DataModel[dm.View] | dm.DataModelList[dm.View],
) -> list[DataModelId]:
    model_id_only: DataModelIdentifier | Sequence[DataModelIdentifier]
    if isinstance(model_id, dm.DataModel):
        model_id_only = model_id.as_id()
    elif isinstance(model_id, dm.DataModelList):
        model_id_only = model_id.as_ids()
    else:
        model_id_only = model_id

    is_sequence = isinstance(model_id_only, Sequence) and not (
        isinstance(model_id_only, tuple) and isinstance(model_id_only[0], str)
    )
    model_ids: list[DataModelIdentifier] = (
        model_id_only if is_sequence else [model_id_only]  # type: ignore[list-item, assignment]
    )
    return [DataModelId.load(id_) for id_ in model_ids]


class CodeFormatter:
    def __init__(self, format_code: bool, logger: Callable[[str], None], default_line_length: int = 120) -> None:
        self._mode = None
        self._format_code = False

        if format_code:
            try:
                import black
            except ImportError:
                logger("black not installed. Skipping code formatting.")
            else:
                line_length = default_line_length
                target_version = f"py{sys.version_info[0]}{sys.version_info[1]}"
                pyproject_toml = _load_pyproject_toml()
                if pyproject_toml and "black" in pyproject_toml.get("tool", {}):
                    logger("Found black configuration in pyproject.toml")
                    black_config = pyproject_toml["tool"]["black"]
                    line_length = black_config.get("line-length", black_config.get("line_length", line_length))
                    target_versions = black_config.get(
                        "target-version", black_config.get("target_version", [target_version])
                    )
                    target_version = target_versions[0] if isinstance(target_versions, list) else target_versions

                self._mode = black.Mode(
                    target_versions={black.TargetVersion(int(target_version.removeprefix("py3")))},
                    line_length=line_length,
                )
                self._format_code = True
                logger(
                    f"Black code formatter enabled with the following settings: "
                    f"target_version: {target_version}, line_length: {line_length}"
                )

    def format_code(self, code: str) -> str:
        if self._format_code:
            import black

            try:
                code = black.format_file_contents(code, fast=False, mode=cast(black.Mode, self._mode))
            except black.NothingChanged:
                pass
            finally:
                # Make sure there's a newline after the content
                if code and code[-1] != "\n":
                    code += "\n"

        return code


def write_sdk_to_disk(
    sdk: dict[Path, str],
    output_dir: Path,
    overwrite: bool,
    format_code: bool = True,
) -> None:
    """Write a generated SDK to disk.

    Args:
        sdk (dict[Path, str]):
            The generated SDK.
        output_dir (Path):
            The output directory to write to.
        overwrite (bool):
            Whether to overwrite existing files.
        format_code (bool):
            Whether to format the generated code using black.
    """
    formatter = CodeFormatter(format_code, print)

    for file_path, file_content in sdk.items():
        path = output_dir / file_path
        if path.exists() and not overwrite:
            raise FileExistsError(f"File {path} already exists. Set overwrite=True to overwrite.")
        elif path.exists():
            path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        if format_code:
            file_content = formatter.format_code(file_content)
        path.write_text(file_content)
