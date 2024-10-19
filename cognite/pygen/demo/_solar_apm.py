from __future__ import annotations

import pathlib
from collections.abc import Callable
from typing import Any, cast

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.data_classes.data_modeling import (
    DataModel,
    MappedProperty,
    SpaceApply,
    View,
)
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.exceptions import CogniteAPIError

from cognite.pygen import generate_sdk_notebook
from cognite.pygen.demo._constants import DEFAULT_DATA_SET, DEFAULT_SPACE
from cognite.pygen.utils.cdf import CSVLoader

_DATA_FOLDER = pathlib.Path(__file__).parent / "solar_apm_data"


class SolarFarmAPM:
    """
    Demo class for generating Solar Farm APM model in Python.

    Args:
        space: The space to deploy the APM model to.
        model_external_id: The external ID of the APM model.
        model_version: The version of the APM model.
        data_set_external_id: The external ID of the data set to use for CDF Resources such as Time Series and Files.
    """

    def __init__(
        self,
        space: str = DEFAULT_SPACE,
        model_external_id: str = "SolarFarmAPM",
        model_version: str = "1",
        data_set_external_id: str | None = DEFAULT_DATA_SET,
    ):
        self._graphql = (_DATA_FOLDER / "model.graphql").read_text()
        self._data_model_id = DataModelId(space=space, external_id=model_external_id, version=model_version)
        self._echo: Callable[[str], None] = print
        self._data_model: DataModel[View] | None = None
        self._data_set_external_id = data_set_external_id

    def display(self):
        """
        Display the model in GraphQL format in a Jupyter notebook environment.
        """
        try:
            from IPython.display import Markdown, display

            display(Markdown(f"### {self._data_model_id}:\n ```\n{self._graphql}```"))
        except ImportError:
            print(self._graphql)

    def create(self, client: CogniteClient, populate: bool = True) -> Any:
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
        self._data_model = self.deploy(client)
        self._echo("✅  Data Model Ready!")
        if populate:
            self.populate(client)
            self._echo("✅  Population Complete!")
        client = self.generate_sdk(client)
        self._echo("✅  SDK Generated!")
        return client

    def deploy(self, client: CogniteClient) -> DataModel[View]:
        """
        Deploy the APM model to the CDF project the client is connected to.

        Args:
            client: Connected CogniteClient

        Returns:
            The DMS representation of the deployed model.
        """
        space = client.data_modeling.spaces.retrieve(self._data_model_id.space)
        if not space:
            space_apply = SpaceApply(
                space=self._data_model_id.space,
                name=self._data_model_id.space,
                description="This space was created by pygen to host demo data models.",
            )
            space = client.data_modeling.spaces.apply(space_apply)
            self._echo(f"Created space {space.space}")

        retrieved = client.data_modeling.data_models.retrieve(self._data_model_id, inline_views=True)
        if retrieved:
            self._echo(f"Data model {self._data_model_id} already exists, skipping deployment")
            return retrieved.latest_version()

        _ = client.data_modeling.graphql.apply_dml(
            self._data_model_id,
            self._graphql,
            name=self._data_model_id.external_id,
            description="This data model was created by pygen for demo purposes.",
        )
        self._echo(f"Deployed data model {self._data_model_id}")
        return client.data_modeling.data_models.retrieve(self._data_model_id, inline_views=True).latest_version()

    def populate(self, client: CogniteClient):
        if self._data_model is None:
            raise ValueError("Cannot populate model before deploying it, please call deploy() first")
        loader = self._create_csv_loader(client)
        loader.populate(client)

    def _create_csv_loader(self, client: CogniteClient) -> CSVLoader:
        if self._data_model is None:
            raise ValueError("Cannot populate model before deploying it, please call deploy() first")
        data_set_id = self._data_set_id(client)
        loader = CSVLoader(
            _DATA_FOLDER,
            self._echo,
            data_set_id,
            self._data_model,
        )
        return loader

    def _data_set_id(self, client: CogniteClient) -> int | None:
        if self._data_set_external_id is None:
            return None
        dataset = client.data_sets.retrieve(external_id=self._data_set_external_id)
        if dataset:
            return dataset.id

        try:
            new_dataset = cast(
                DataSet,
                client.data_sets.create(
                    DataSet(
                        external_id=self._data_set_external_id,
                        name=self._data_set_external_id,
                        description="This data set was created by pygen for demo purposes.",
                    )
                ),
            )
        except CogniteAPIError as e:
            self._echo(f"Failed to create data set {self._data_set_external_id}: {e}")
            return None
        self._echo(f"Created data set {new_dataset.external_id}")
        return new_dataset.id

    def generate_sdk(self, client: CogniteClient) -> Any:
        """
        Generate a Python SDK for the APM demo model.

        !!! warning "Assumes APM Model Deployed"
            This method assumes the APM model has been deployed to the CDF project the client is connected to.

        Args:
            client: Connected CogniteClient

        Returns:
            An instantiated SDK client for the APM model.

        """
        if self._data_model is None:
            return generate_sdk_notebook(self._data_model_id, client)
        return generate_sdk_notebook(self._data_model, client)

    def clean(self, client: CogniteClient, delete_space: bool = True, auto_confirm: bool = False):
        """
        Clean the Solar Farm Model from the CDF project the client is connected to.

        This means removing the data model, views, and containers generated by pygen, as well as
        all CDF resources used in the population of the model.

        Args:
            client: Connected CogniteClient
            delete_space: Whether to try to delete the space the APM model was deployed to. This will only work if the
                          space does not contain any other data models, views or containers.
            auto_confirm: Whether to skip the confirmation prompt.

        """
        data_models = client.data_modeling.data_models.retrieve(self._data_model_id, inline_views=True)
        if not data_models:
            self._echo(f"Data model {self._data_model_id} does not exist, skipping clean")
            return
        data_model = data_models.latest_version()
        view_ids = list({view.as_id() for view in data_model.views})
        container_ids = list(
            {
                prop.container
                for view in data_model.views
                for prop in view.properties.values()
                if isinstance(prop, MappedProperty)
            }
        )
        self._data_model = data_model
        loader = self._create_csv_loader(client)
        if not auto_confirm:
            self._echo(f"About to delete data model {self._data_model_id}")
            self._echo(f"About to delete views {view_ids}")
            self._echo(f"About to delete containers {container_ids} along with all nodes and edges")
            if delete_space:
                self._echo(f"About to delete space {self._data_model_id.space}")
            answer = input("Are you sure you want to continue? [y/N] ")
            if not isinstance(answer, str) and hasattr(answer, "result"):
                # PyodideFuture
                self._echo(
                    "The parameter auto_confirm is not supported in a Pyodide environment,"
                    "please set auto_confirm=True"
                )
                answer = "n"

            if not answer.lower().startswith("y"):
                self._echo("Aborting")
                return

        loader.clean(client)
        client.data_modeling.data_models.delete(self._data_model_id)
        self._echo(f"Deleted data model {self._data_model_id}")
        client.data_modeling.views.delete(view_ids)
        self._echo(f"Deleted views {view_ids}")
        client.data_modeling.containers.delete(container_ids)
        self._echo(f"Deleted containers {container_ids}")
        if delete_space:
            client.data_modeling.spaces.delete(self._data_model_id.space)
            self._echo(f"Deleted space {self._data_model_id.space}")
