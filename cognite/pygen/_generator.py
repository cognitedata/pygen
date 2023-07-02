import sys
import tempfile
from pathlib import Path
from typing import Callable

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.exceptions import CogniteAPIError

from cognite.pygen._core.dms_to_python import SDKGenerator


def generate_sdk_notebook(
    client: CogniteClient, model_id: DataModelIdentifier, top_level_package: str, client_name: str
) -> None:
    """
    Generates a Python SDK from the given Data Model.

    The SDK is generated in a temporary directory and added to the sys.path. This is such that it becomes available
    to be imported in the current Python session.

     Parameters
     ----------
     client: CogniteClient
        The cognite client used for fetching the data model.
     model_id: DataModelIdentifier
        The id of the data model to generate the SDK from.
     top_level_package: str
        The name of the top level package of the SDK. Example "movie.client"
     client_name: str
        The name of the client class. Example "MovieClient"
    """
    output_dir = Path(tempfile.gettempdir()) / "pygen"
    generate_sdk(client, model_id, top_level_package, client_name, output_dir, print)  # noqa: T202
    sys.path.append(str(output_dir))


def generate_sdk(
    client: CogniteClient,
    model_id: DataModelIdentifier,
    top_level_package: str,
    client_name: str,
    output_dir: Path,
    logger: Callable[[str], None],
):
    data_model = _load_data_model(client, model_id, logger)
    logger(f"Successfully retrieved data model {model_id}")
    sdk_generator = SDKGenerator(top_level_package, client_name, data_model, logger)
    sdk = sdk_generator.generate_sdk()
    logger(f"Writing SDK to {output_dir}")
    write_sdk_to_disk(sdk, output_dir)
    logger("Done!")


def _load_data_model(
    client: CogniteClient, model_id: DataModelIdentifier, logger: Callable[[str], None]
) -> dm.DataModel:
    try:
        data_models = client.data_modeling.data_models.retrieve(model_id, inline_views=True)
        data_model = data_models[0]
    except CogniteAPIError as e:
        logger(f"Error retrieving data model: {e}")
        raise e
    except IndexError as e:
        logger(f"Cannot find {model_id}")
        raise e
    return data_model


def write_sdk_to_disk(sdk: dict[Path, str], output_dir: Path):
    """Write a generated SDK to disk.

    Args:
        sdk: The generated SDK.
        output_dir: The output directory to write to.
    """
    for file_path, file_content in sdk.items():
        path = output_dir / file_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file_content)
