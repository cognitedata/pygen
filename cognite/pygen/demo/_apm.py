from __future__ import annotations

import pathlib
from typing import Any, Callable

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DataModel
from cognite.client.data_classes.data_modeling.ids import DataModelId, ViewId

from cognite.pygen import generate_sdk_notebook

from ._constants import DEFAULT_SPACE

_DATA_FOLDER = pathlib.Path(__file__).parent / "apm_data"


class APM:
    """
    Demo class for generating an APM model in Python.

    Args:
        space: The space to deploy the APM model to.
        model_external_id: The external ID of the APM model.
        model_version: The version of the APM model.
    """

    def __init__(self, space: str = DEFAULT_SPACE, model_external_id: str = "ApmModel", model_version: str = "1"):
        self._graphql = (_DATA_FOLDER / "model.graphql").read_text()
        self._data_model_id = DataModelId(space=space, external_id=model_external_id, version=model_version)
        self._echo: Callable[[str], None] = print

    def display(self):
        """
        Display the model in GraphQL format in a Jupyter notebook environment.
        """
        try:
            from IPython.display import Markdown, display

            display(Markdown(f"### {self._data_model_id}:\n ```\n{self._graphql}```"))
        except ImportError:
            print(self._graphql)

    def generate_sdk(self, client: CogniteClient, populate: bool = True) -> Any:
        """
        Deploy, populate (optional) and generate a Python SDK for the APM demo model.

        This method will do the following three steps

        1. Deploy the APM model to the CDF project the client is connected to.
        2. Generate a Python SDK for the APM model.
        3. Populate the APM model with mock data included in pygen.

        Args:
            client: Connected CogniteClient
            populate: Whether to populate the APM model with mock data included in pygen.

        Returns:
            An instantiated SDK client for the APM model.
        """
        self.deploy(client)
        if populate:
            self.populate(client)
        return generate_sdk_notebook(client, self._data_model_id)

    def deploy(self, client: CogniteClient) -> DataModel[ViewId]:
        """
        Deploy the APM model to the CDF project the client is connected to.

        Args:
            client: Connected CogniteClient

        Returns:
            The DMS representation of the deployed model.
        """
        retrieved = client.data_modeling.data_models.retrieve(self._data_model_id)
        if retrieved:
            self._echo(f"Data model {self._data_model_id} already exists, skipping deployment")
            return retrieved.latest_version()

        _ = client.data_modeling.graphql.apply_dml(self._data_model_id, self._graphql)
        self._echo(f"Deployed data model {self._data_model_id}")
        return client.data_modeling.data_models.retrieve(self._data_model_id).latest_version()

    def populate(self, client: CogniteClient):
        ...
