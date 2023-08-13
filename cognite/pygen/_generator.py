from __future__ import annotations

import importlib
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Literal, Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.exceptions import CogniteAPIError

from cognite.pygen._core.dms_to_python import SDKGenerator


def generate_sdk_notebook(
    client: CogniteClient,
    model_id: DataModelIdentifier | Sequence[DataModelIdentifier],
    top_level_package: str,
    client_name: str,
    logger: Callable[[str], None] | None = None,
    overwrite: bool = False,
) -> Any:
    """
    Generates a Python SDK from the given Data Model(s).

    The SDK is generated in a temporary directory and added to the sys.path. This is such that it becomes available
    to be imported in the current Python session.

     Parameters
     ----------
     client: CogniteClient
        The cognite client used for fetching the data model.
     model_id: DataModelIdentifier | Sequence[DataModelIdentifier]
        The id(s) of the data model(s) to generate the SDK from.
     top_level_package: str
        The name of the top level package for the SDK. Example "movie.client"
     client_name: str
        The name of the client class. Example "MovieClient"
     logger: Callable[[str], None]
        A logger function that will be called with the progress of the generation.
     overwrite: bool
        Whether to overwrite the output directory if it already exists. Defaults to False.

    Returns
    -------
        Any: The instantiated generated client class.
    """
    output_dir = Path(tempfile.gettempdir()) / "pygen"
    logger = logger or print  # noqa: T202
    generate_sdk(
        client,
        model_id,
        top_level_package,
        client_name,
        output_dir,
        logger,
        pydantic_version="infer",
        overwrite=overwrite,
    )
    sys.path.append(str(output_dir))
    logger(f"Added {output_dir} to sys.path to enable import")
    module = vars(importlib.import_module(top_level_package))
    logger(f"Imported {top_level_package}")
    return module[client_name](client)


def generate_sdk(
    client: CogniteClient,
    model_id: DataModelIdentifier | Sequence[DataModelIdentifier],
    top_level_package: str,
    client_name: str,
    output_dir: Path,
    logger: Callable[[str], None],
    pydantic_version: Literal["v1", "v2", "infer"] = "infer",
    overwrite: bool = False,
) -> None:
    """
    Generates a Python SDK from the given Data Model(s).

    Parameters
    ----------
    client: CogniteClient
        The cognite client used for fetching the data model.
    model_id: DataModelIdentifier | Sequence[DataModelIdentifier]
        The id(s) of the data model(s) to generate the SDK from.
    top_level_package : str
        The name of the top level package for the SDK. Example "movie.client"
    client_name: str
        The name of the client class. Example "MovieClient"
    output_dir: Path
        The directory to write the SDK to.
    logger: Callable[[str], None]
        A logger function to log progress.
    pydantic_version: Literal["v1", "v2", "infer"]
        The version of pydantic to use. Defaults to "infer" which will use the environment to detect the installed
        version of pydantic.
    overwrite: bool
        Whether to overwrite the output directory if it already exists. Defaults to False.

    """
    data_model = _load_data_model(client, model_id, logger)
    logger(f"Successfully retrieved data model(s) {model_id}")
    sdk_generator = SDKGenerator(top_level_package, client_name, data_model, pydantic_version, logger)
    sdk = sdk_generator.generate_sdk()
    logger(f"Writing SDK to {output_dir}")
    write_sdk_to_disk(sdk, output_dir, overwrite)
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
) -> dm.DataModel | dm.DataModelList:
    try:
        data_models = client.data_modeling.data_models.retrieve(model_id, inline_views=True)
        if len(data_models) == 1:
            data_model = data_models[0]
        else:
            return data_models
    except CogniteAPIError as e:
        logger(f"Error retrieving data model(s): {e}")
        raise e
    except IndexError as e:
        logger(f"Cannot find {model_id}")
        raise e
    return data_model


def write_sdk_to_disk(sdk: dict[Path, str], output_dir: Path, overwrite: bool):
    """Write a generated SDK to disk.

    Args:
        sdk: The generated SDK.
        output_dir: The output directory to write to.
        overwrite: Whether to overwrite existing files.
    """
    for file_path, file_content in sdk.items():
        path = output_dir / file_path
        if path.exists() and not overwrite:
            raise FileExistsError(f"File {path} already exists. Set overwrite=True to overwrite.")
        elif path.exists():
            path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file_content)
