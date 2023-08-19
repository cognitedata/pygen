from __future__ import annotations

import csv
import pathlib
from collections import defaultdict
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime
from typing import Any, Callable, Optional, Protocol, Union, get_args, get_origin, get_type_hints

from cognite.client import CogniteClient
from cognite.client._api.files import FilesAPI
from cognite.client.data_classes import DataSet, FileMetadata, FileMetadataList, TimeSeries, TimeSeriesList
from cognite.client.data_classes._base import (
    CogniteResource,
    T_CogniteResource,
    T_CogniteResourceList,
)
from cognite.client.data_classes.data_modeling import (
    DataModel,
    DirectRelationReference,
    EdgeApply,
    EdgeApplyResultList,
    MappedProperty,
    NodeApply,
    NodeApplyResultList,
    NodeOrEdgeData,
    SingleHopConnectionDefinition,
    SpaceApply,
    View,
    data_types,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId, DataModelId, EdgeId, NodeId
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._text import to_snake_case

from cognite.pygen import generate_sdk_notebook
from cognite.pygen.demo._constants import DEFAULT_DATA_SET, DEFAULT_SPACE

_DATA_FOLDER = pathlib.Path(__file__).parent / "solar_apm_data"


class _CogniteCoreResourceAPI(Protocol[T_CogniteResourceList]):
    def retrieve_multiple(
        self,
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> T_CogniteResourceList:
        ...

    def create(self, items: T_CogniteResourceList) -> T_CogniteResourceList:
        ...

    def delete(
        self,
        id: Optional[Union[int, Sequence[int]]] = None,
        external_id: Optional[Union[str, Sequence[str]]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        ...


class _FileAPIAdapter(_CogniteCoreResourceAPI[FileMetadataList]):
    def __init__(self, files_api: FilesAPI):
        self._files_api = files_api

    def retrieve_multiple(
        self,
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadataList:
        return self._files_api.retrieve_multiple(
            ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
        )

    def create(self, items: FileMetadataList) -> FileMetadataList:
        created = []
        for item in items:
            created.append(self._files_api.create(item, overwrite=True)[0])
        return FileMetadataList(created)

    def delete(
        self,
        id: Optional[Union[int, Sequence[int]]] = None,
        external_id: Optional[Union[str, Sequence[str]]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        with suppress(CogniteNotFoundError):
            self._files_api.delete(id=id, external_id=external_id)


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
            self._data_model.space,
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
            new_dataset = client.data_sets.create(
                DataSet(
                    external_id=self._data_set_external_id,
                    name=self._data_set_external_id,
                    description="This data set was created by pygen for demo purposes.",
                )
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
        return generate_sdk_notebook(client, self._data_model_id, logger=self._echo, overwrite=True)

    def clean(self, client: CogniteClient, delete_space: bool = True, auto_confirm: bool = False):
        """
        Clean the APM model from the CDF project the client is connected to.

        This means removing the data model, views, and containers generated by pygen.

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
            if not input("Are you sure? [n/y] ").lower().startswith("y"):
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


class CSVLoader:
    def __init__(
        self,
        source_dir: pathlib.Path,
        echo: Optional[Callable[[str], None]] = None,
        data_set_id: int | None = None,
        space: str = DEFAULT_SPACE,
        data_model: DataModel[View] | None = None,
    ):
        self._source_dir = source_dir
        self._data_set_id = data_set_id
        self._echo: Callable[[str], None] = echo or print
        self._space = space
        self._data_model = data_model

    def populate(self, client: CogniteClient) -> None:
        """
        Populate CDF with data found in the source directory passed in the constructor.

        Args:
            client: Connected CogniteClient

        Returns:
        """
        self.populate_timeseries(client)
        self.populate_files(client)
        self.populate_nodes(client)
        self.populate_edges(client)

    def populate_timeseries(self, client: CogniteClient) -> TimeSeriesList:
        return self._populate_cdf_resource(client.time_series, TimeSeries, TimeSeriesList)

    def populate_files(self, client: CogniteClient) -> FileMetadataList:
        return self._populate_cdf_resource(_FileAPIAdapter(client.files), FileMetadata, FileMetadataList)

    def populate_nodes(self, client: CogniteClient) -> NodeApplyResultList:
        if not self._data_model:
            raise ValueError("Missing data model, please pass it to the constructor")
        all_nodes = []
        for view in self._data_model.views:
            properties: dict[str, MappedProperty] = {
                name: prop for name, prop in view.properties.items() if isinstance(prop, MappedProperty)
            }
            if not properties:
                continue
            raw_nodes = self._load_resource(NodeApply, file_name=view.external_id, properties=properties)
            # Need to split properties from general node data
            for raw_node in raw_nodes:
                external_id = self._pop_or_raise(raw_node, "externalId")
                data_properties: dict[ContainerId, dict] = defaultdict(dict)
                for name, value in raw_node.items():
                    if prop := properties.get(name):
                        data_properties[prop.container][name] = value
                node = NodeApply(
                    external_id=external_id,
                    space=self._space,
                    sources=[
                        NodeOrEdgeData(source=container, properties=properties)
                        for container, properties in data_properties.items()
                    ],
                )
                all_nodes.append(node)

        if not all_nodes:
            self._echo("No nodes to create")
            return NodeApplyResultList([])

        created = client.data_modeling.instances.apply(nodes=all_nodes)
        created_nodes = sum(1 for n in created.nodes if n.was_modified)
        if created_nodes == len(all_nodes):
            self._echo(f"Created {created_nodes} nodes")
        elif created_nodes == 0:
            self._echo(f"All {len(all_nodes)} nodes already exists")
        else:
            self._echo(f"Created {created_nodes} nodes, {len(all_nodes) - created_nodes} already exists.")
        return created.nodes

    def populate_edges(self, client: CogniteClient) -> EdgeApplyResultList:
        if not self._data_model:
            raise ValueError("Missing data model, please pass it to the constructor")
        all_edges = []
        for view in self._data_model.views:
            for name, prop in view.properties.items():
                if not isinstance(prop, SingleHopConnectionDefinition):
                    continue
                raw_edges = self._load_resource(EdgeApply, file_name=f"{view.external_id}.{name}")
                for raw_edge in raw_edges:
                    source_id = self._pop_or_raise(raw_edge, "sourceExternalId")
                    target_id = self._pop_or_raise(raw_edge, "targetExternalId")
                    edge = EdgeApply(
                        external_id=self._create_edge_external_id(source_id, target_id),
                        space=self._space,
                        type=prop.type,
                        start_node=DirectRelationReference(self._space, source_id),
                        end_node=DirectRelationReference(self._space, target_id),
                    )
                    all_edges.append(edge)
        if not all_edges:
            self._echo("No edges to create")
            return EdgeApplyResultList([])
        created = client.data_modeling.instances.apply(edges=all_edges)
        created_edges = sum(1 for e in created.edges if e.was_modified)
        if created_edges == len(all_edges):
            self._echo(f"Created {created_edges} edges")
        elif created_edges == 0:
            self._echo(f"All {len(all_edges)} edges already exists")
        else:
            self._echo(f"Created {created_edges} edges, {len(all_edges)-created_edges} edges already exists")
        return created.edges

    @staticmethod
    def _create_edge_external_id(source_id: str, target_id: str) -> str:
        return f"{source_id}.{target_id}"

    def clean(self, client: CogniteClient) -> None:
        """
        Delete all data created by this loader.

        Args:
            client: Connected CogniteClient

        Returns:
        """
        self.clean_timeseries(client)
        self.clean_files(client)
        self.clean_nodes(client)
        self.clean_edges(client)

    def clean_timeseries(self, client: CogniteClient) -> None:
        self._clean_cdf_resource(client.time_series, TimeSeries)

    def clean_files(self, client: CogniteClient) -> None:
        self._clean_cdf_resource(_FileAPIAdapter(client.files), FileMetadata)

    def clean_nodes(self, client: CogniteClient) -> None:
        if not self._data_model:
            raise ValueError("Missing data model, please pass it to the constructor")
        node_ids: list[NodeId] = []
        for view in self._data_model.views:
            raw_nodes = self._load_resource(NodeApply, file_name=view.external_id)
            node_ids.extend(
                [NodeId(self._space, external_id=self._pop_or_raise(node, "externalId")) for node in raw_nodes]
            )
        if not node_ids:
            self._echo("No nodes to delete")
            return
        result = client.data_modeling.instances.delete(nodes=node_ids)
        self._echo(f"Deleted {len(result.nodes)} nodes")

    def clean_edges(self, client: CogniteClient) -> None:
        if not self._data_model:
            raise ValueError("Missing data model, please pass it to the constructor")
        edge_ids = []
        # Each type of connection is stored in a node.
        node_ids = []
        for view in self._data_model.views:
            for name, prop in view.properties.items():
                if not isinstance(prop, SingleHopConnectionDefinition):
                    continue
                edge_ids.extend(
                    [
                        EdgeId(
                            self._space,
                            external_id=self._create_edge_external_id(
                                self._pop_or_raise(edge, "sourceExternalId"),
                                self._pop_or_raise(edge, "targetExternalId"),
                            ),
                        )
                        for edge in self._load_resource(EdgeApply, file_name=f"{view.external_id}.{name}")
                    ]
                )
                node_ids.append(NodeId(self._space, external_id=f"{view.external_id}.{name}"))

        if not edge_ids and not node_ids:
            self._echo("No edges to delete")
            return
        result = client.data_modeling.instances.delete(edges=edge_ids, nodes=node_ids)
        self._echo(f"Deleted {len(result.edges)} edges")
        self._echo(f"Deleted {len(result.nodes)} type nodes")

    @staticmethod
    def _pop_or_raise(d: dict, camel_case: str) -> Any:
        if camel_case in d:
            return d.pop(camel_case)
        elif (snake := to_snake_case(camel_case)) in d:
            return d.pop(snake)
        else:
            raise ValueError(f"Missing {camel_case} in {d}")

    @staticmethod
    def _read_csv(
        file_path: pathlib.Path,
        resource_cls: type[T_CogniteResource] | None = None,
        properties: dict[str, MappedProperty] | None = None,
    ) -> list[dict[str, Any]]:
        converter = CogniteResourceDataTypesConverter(resource_cls=resource_cls, properties=properties)
        with file_path.open() as file:
            reader = csv.reader(file)
            columns = next(reader)

            rows = []
            for raw_row in reader:
                row = converter.convert_dtypes(dict(zip(columns, raw_row)))
                rows.append(row)
        return rows

    def _load_resource(
        self,
        resource_cls: type[T_CogniteResource],
        file_name: str | None = None,
        properties: dict[str, MappedProperty] | None = None,
    ) -> list[dict[str, Any]]:
        resource_name = file_name or resource_cls.__name__
        if not (file_path := self._source_dir / f"{resource_name}.csv").exists():
            return []
        return self._read_csv(file_path, resource_cls=resource_cls, properties=properties)

    def _create_resource(
        self, api: _CogniteCoreResourceAPI[T_CogniteResourceList], resources: T_CogniteResourceList
    ) -> T_CogniteResourceList:
        if not resources:
            return resources
        resource_name = type(resources[0]).__name__
        new_ids = [t.external_id for t in resources]
        retrieved = api.retrieve_multiple(external_ids=new_ids, ignore_unknown_ids=True)
        existing = [ts.external_id for ts in retrieved if ts]
        if not (missing := set(new_ids) - set(existing)):
            self._echo(f"Skipping {resource_name} creation, all {len(existing)} {resource_name} already exist")
            return retrieved

        created = api.create(type(resources)([item for item in resources if item.external_id in missing]))
        self._echo(f"Created {len(created)} {resource_name}")
        return type(resources)(created + retrieved)

    def _populate_cdf_resource(
        self,
        api: _CogniteCoreResourceAPI[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        cls_list: type[T_CogniteResourceList],
    ) -> T_CogniteResourceList:
        raw_data = self._load_resource(resource_cls=resource_cls)
        for item in raw_data:
            if "dataSetId" not in item:
                item["dataSetId"] = self._data_set_id
        resources = cls_list._load(raw_data)
        if not resources:
            self._echo(f"Skipping {type(resource_cls.__name__)} population, no data found in {self._source_dir}")
            return resources
        return self._create_resource(api=api, resources=resources)

    def _clean_cdf_resource(
        self, api: _CogniteCoreResourceAPI[T_CogniteResourceList], resource_cls: type[T_CogniteResource]
    ):
        raw_data = self._load_resource(resource_cls)
        external_ids = [self._pop_or_raise(item, "externalId") for item in raw_data]
        api.delete(external_id=external_ids, ignore_unknown_ids=True)
        self._echo(f"Deleted {len(external_ids)} {resource_cls.__name__}")


class CogniteResourceDataTypesConverter:
    supported_timestamp_formats = ("%Y-%m-%dT%H:%M:%SZ", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S.%fZ")

    def __init__(
        self, resource_cls: type[CogniteResource] | None = None, properties: dict[str, MappedProperty] | None = None
    ):
        self._type_hint_by_name = self._load_type_hints(resource_cls)
        self._property_by_name = self._load_properties(properties)

    @staticmethod
    def _load_type_hints(resource_cls: type[CogniteResource] | None = None) -> dict[str, Any]:
        if resource_cls is None:
            return {}
        try:
            return get_type_hints(resource_cls.__init__, localns={"CogniteClient": CogniteClient})
        except TypeError:
            return {}

    @staticmethod
    def _load_properties(
        properties: dict[str, MappedProperty] | None = None
    ) -> dict[str, tuple[ContainerId, data_types.PropertyType]]:
        if properties is None:
            return {}
        return {name: (prop.container, prop.type) for name, prop in properties.items()}

    def convert_dtypes(self, row: dict[str, Any]) -> dict[str, Any]:
        if not self._type_hint_by_name and not self._property_by_name:
            return row
        for key, value in list(row.items()):
            snake_key = to_snake_case(key)
            if type_hint := self._type_hint_by_name.get(snake_key):
                row[key] = self._convert_type_hint(value, type_hint)
            elif (entry := self._property_by_name.get(key)) or (entry := self._property_by_name.get(snake_key)):
                container, property_type = entry
                try:
                    row[key] = self._convert_property(value, property_type, container)
                except ValueError as e:
                    if "cannot be empty" in str(e):
                        # Removing empty values
                        del row[key]
                    else:
                        raise e

        return row

    @staticmethod
    def _convert_type_hint(value: Any, outer_type_hint: Any) -> Any:
        if get_origin(outer_type_hint) is Union:
            type_hint, *_ = get_args(outer_type_hint)
        else:
            type_hint = outer_type_hint

        if type_hint is bool:
            return value.lower() == "true"
        elif type_hint is int:
            return int(value)
        elif type_hint is float:
            return float(value)
        elif type_hint is str:
            return str(value)
        return value

    @classmethod
    def _convert_property(cls, value: Any, property_type: data_types.PropertyType, container_id: ContainerId) -> Any:
        if isinstance(property_type, data_types.Boolean):
            return value.lower() == "true"
        elif isinstance(property_type, (data_types.Int32, data_types.Int64)):
            return int(value)
        elif isinstance(property_type, (data_types.Float32, data_types.Float64)):
            return float(value)
        elif isinstance(property_type, data_types.Text):
            return str(value)
        elif isinstance(property_type, data_types.Timestamp):
            # YYYY-MM-DDTHH:MM:SS[.millis][Z|time zone]
            for supported_format in cls.supported_timestamp_formats:
                try:
                    return datetime.strptime(value, supported_format).isoformat()
                except ValueError:
                    pass
            raise ValueError(f"Unsupported timestamp format: {value}")
        elif isinstance(property_type, data_types.DirectRelation):
            if not value:
                raise ValueError("Direct relation cannot be empty")
            return {"externalId": str(value), "space": container_id.space}
        elif isinstance(property_type, data_types.CDFExternalIdReference):
            if not value:
                raise ValueError("CDF external ID reference cannot be empty")
            return {"externalId": str(value)}

        return value


if __name__ == "__main__":
    # Quick and dirty way to do local ad-hoc testing

    from cognite.pygen import load_cognite_client_from_toml

    c = load_cognite_client_from_toml("../../../config.toml")
    apm = SolarFarmAPM()
    apm.create(c)
    apm.clean(c, auto_confirm=True)
