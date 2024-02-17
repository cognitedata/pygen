from __future__ import annotations

import importlib
import re
import shutil
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union, cast, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.exceptions import CogniteAPIError
from typing_extensions import TypeAlias

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen._core.models import DataClass
from cognite.pygen._settings import _load_pyproject_toml
from cognite.pygen._version import __version__
from cognite.pygen.config import PygenConfig
from cognite.pygen.exceptions import DataModelNotFound
from cognite.pygen.utils.text import to_pascal, to_snake

DataModel: TypeAlias = Union[DataModelIdentifier, dm.DataModel[dm.View]]

_ILLEGAL_CHARACTERS_IN_FOLDER_NAME = '/\\?%*:|"<>!'


@overload
def generate_sdk(  # type: ignore[overload-overlap]
    model_id: DataModel | Sequence[DataModel],
    client: Optional[CogniteClient] = None,
    top_level_package: Optional[str] = None,
    client_name: Optional[str] = None,
    default_instance_space: str | None = None,
    output_dir: Optional[Path] = None,
    logger: Optional[Callable[[str], None]] = None,
    pydantic_version: Literal["v1", "v2", "infer"] = "infer",
    overwrite: bool = False,
    format_code: bool = True,
    config: Optional[PygenConfig] = None,
    return_sdk_files: Literal[False] = False,
) -> None: ...


@overload
def generate_sdk(
    model_id: DataModel | Sequence[DataModel],
    client: Optional[CogniteClient] = None,
    top_level_package: Optional[str] = None,
    client_name: Optional[str] = None,
    default_instance_space: str | None = None,
    output_dir: Optional[Path] = None,
    logger: Optional[Callable[[str], None]] = None,
    pydantic_version: Literal["v1", "v2", "infer"] = "infer",
    overwrite: bool = False,
    format_code: bool = True,
    config: Optional[PygenConfig] = None,
    return_sdk_files: Literal[True] = False,  # type: ignore[assignment]
) -> dict[Path, str]: ...


def generate_sdk(
    model_id: DataModel | Sequence[DataModel],
    client: Optional[CogniteClient] = None,
    top_level_package: Optional[str] = None,
    client_name: Optional[str] = None,
    default_instance_space: str | None = None,
    output_dir: Optional[Path] = None,
    logger: Optional[Callable[[str], None]] = None,
    pydantic_version: Literal["v1", "v2", "infer"] = "infer",
    overwrite: bool = False,
    format_code: bool = True,
    config: Optional[PygenConfig] = None,
    return_sdk_files: Literal[True, False] = False,
) -> None | dict[Path, str]:
    """
    Generates a Python SDK tailored to the given Data Model(s).

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
        default_instance_space: The default instance space to use for the generated SDK. Defaults to the
            instance space of the first data model given.
        output_dir: The location to output the generated SDK. Defaults to the current working directory.
        logger: A logger function to log progress. Defaults to print.
        pydantic_version: The version of pydantic to use. Defaults to "infer" which will use
            the environment to detect the installed version of pydantic.
        overwrite: Whether to overwrite the output directory if it already exists. Defaults to False.
        format_code: Whether to format the generated code using black. Defaults to True.
        config: The configuration used to control how to generate the SDK.
        return_sdk_files: Whether to return the generated SDK files as a dictionary. Defaults to False.
            This is useful for granular control of how to write the SDK to disk.
    """
    logger = logger or print
    data_model = _get_data_model(model_id, client, logger)

    external_id = _extract_external_id(data_model)

    if top_level_package is None:
        top_level_package = _default_top_level_package(external_id)
    if client_name is None:
        client_name = _default_client_name(external_id)

    sdk_generator = SDKGenerator(
        top_level_package,
        client_name,
        data_model,
        default_instance_space,
        pydantic_version,
        logger,
        config or PygenConfig(),
    )
    sdk = sdk_generator.generate_sdk()
    if return_sdk_files:
        return sdk
    output_dir = output_dir or Path.cwd()
    logger(f"Writing SDK to {output_dir}")
    write_sdk_to_disk(sdk, output_dir, overwrite, format_code)
    logger("Done!")
    return None


def generate_sdk_notebook(
    model_id: DataModel | Sequence[DataModel],
    client: Optional[CogniteClient] = None,
    top_level_package: Optional[str] = None,
    client_name: Optional[str] = None,
    default_instance_space: str | None = None,
    config: Optional[PygenConfig] = None,
    clean_pygen_temp_dir: bool = True,
) -> Any:
    """
    Generates a Python SDK tailored to the given Data Model(s) and imports it into the current Python session.

    This function is wrapper around generate_sdk. It is intended to be used in a Jupyter notebook.
    The differences are that it:

    * The SDK is generated in a temporary directory and added to the sys.path. This is such that it
      becomes available to be imported in the current Python session.
    * The signature is simplified.
    * An instantiated client of the generated SDK is returned.


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
        default_instance_space: The default instance space to use for the generated SDK. Defaults to the
            instance space of the first data model given.
        config: The configuration used to control how to generate the SDK.
        clean_pygen_temp_dir: Whether to clean the temporary directory used to store the generated SDK.
            Defaults to True.

    Returns:
        The instantiated generated client class.
    """
    data_model = _get_data_model(model_id, client, print)
    folder_name = _create_folder_name(
        data_model.as_id() if isinstance(data_model, dm.DataModel) else data_model.as_ids()
    )
    output_dir = Path(tempfile.gettempdir()) / "pygen" / folder_name
    if clean_pygen_temp_dir and output_dir.exists():
        try:
            shutil.rmtree(output_dir)
        except Exception as e:
            print(f"Failed to clean temporary directory {output_dir}: {e}")
        else:
            print(f"Cleaned temporary directory {output_dir}")

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
        output_dir=output_dir,
        overwrite=True,
        format_code=False,
        config=config,
    )
    if str(output_dir) not in sys.path:
        sys.path.append(str(output_dir))
        print(f"Added {output_dir} to sys.path to enable import")
    else:
        print(f"{output_dir} already in sys.path")
    module = vars(importlib.import_module(top_level_package))
    print(f"Imported {top_level_package}")
    print("You can now use the generated SDK in the current Python session.")
    if isinstance(data_model, dm.DataModel):
        view = data_model.views[0]
    elif isinstance(data_model, Sequence):
        view = data_model[0].views[0]
    else:
        view = None

    if view:
        print(
            "The data classes are available by importing, for example, "
            f"`from {top_level_package}.data_classes import {DataClass.to_base_name(view)}Write`"
        )
    return module[client_name](client)


def _default_top_level_package(external_id: str) -> str:
    return f"{to_snake(external_id)}"


def _default_client_name(external_id: str) -> str:
    return f"{to_pascal(external_id)}Client"


def _extract_external_id(data_model: dm.DataModel | dm.DataModelList) -> str:
    if isinstance(data_model, dm.DataModel):
        return data_model.external_id.replace(" ", "_")
    else:
        return data_model[0].external_id.replace(" ", "_")


def _create_folder_name(data_model: dm.DataModelId | list[dm.DataModelId]) -> str:
    """Create a folder name from a data model.
    >>> _create_folder_name(dm.DataModelId("space", "external_id", "version"))
    'space_external_id_version'
    >>> _create_folder_name(dm.DataModelId("space/", "\\external*id", "<version with spaces>"))
    'space_external_id_version_with_spaces_'
    """
    if isinstance(data_model, dm.DataModelId):
        name = f"{data_model.space}_{data_model.external_id}_{data_model.version}".replace(" ", "_")
    else:
        name = f"{data_model[0].space}_{data_model[0].external_id}_{data_model[0].version}".replace(" ", "_")

    legal_name = re.sub(f"[{re.escape(_ILLEGAL_CHARACTERS_IN_FOLDER_NAME)}]", "_", name)
    # Replace multiple underscores with a single underscore
    return re.sub("_{2,}", "_", legal_name)


def _get_data_model(model_id, client, logger) -> dm.DataModel | dm.DataModelList:
    if isinstance(model_id, dm.DataModel):
        return model_id
    elif isinstance(model_id, Sequence) and all(isinstance(model, dm.DataModel) for model in model_id):
        return dm.DataModelList(model_id)
    elif isinstance(model_id, (dm.DataModelId, tuple)) or (
        isinstance(model_id, Sequence) and all(isinstance(model, (dm.DataModelId, tuple)) for model in model_id)
    ):
        if client is None:
            raise ValueError("client must be provided when passing in DataModelId")

        data_model = _load_data_model(client, model_id, logger)
        logger(f"Successfully retrieved data model(s) {model_id}")
        return data_model

    raise TypeError(f"Invalid type for model_id: {type(model_id)}")


@overload
def _load_data_model(
    client: CogniteClient, model_id: DataModelIdentifier, logger: Callable[[str], None]
) -> dm.DataModel: ...


@overload
def _load_data_model(
    client: CogniteClient, model_id: Sequence[DataModelIdentifier], logger: Callable[[str], None]
) -> dm.DataModelList: ...


def _load_data_model(
    client: CogniteClient, model_id: DataModelIdentifier | Sequence[DataModelIdentifier], logger: Callable[[str], None]
) -> dm.DataModel[dm.View] | dm.DataModelList[dm.View]:
    identifier = _load_data_model_identifier(model_id)
    current_client_name = client.config.client_name
    # The client name is used for aggregated logging of Pygen Usage
    client.config.client_name = f"CognitePygen:{__version__}:GenerateSDK"
    try:
        data_models = client.data_modeling.data_models.retrieve(model_id, inline_views=True)
    except CogniteAPIError as e:
        logger(f"Error retrieving data model(s): {e}")
        raise e
    finally:
        client.config.client_name = current_client_name
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
