from __future__ import annotations

import csv
import pathlib
from collections import defaultdict
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any, Optional, Protocol, Union, get_args, get_origin, get_type_hints

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._api.files import FilesAPI
from cognite.client.data_classes import FileMetadata, FileMetadataList, TimeSeries, TimeSeriesList
from cognite.client.data_classes._base import CogniteResource, T_CogniteResource, T_CogniteResourceList
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DataModel,
    DataModelIdentifier,
    DirectRelationReference,
    EdgeApply,
    EdgeApplyResultList,
    EdgeId,
    MappedProperty,
    NodeApply,
    NodeApplyResultList,
    NodeId,
    NodeOrEdgeData,
    SingleHopConnectionDefinition,
    View,
    ViewList,
    data_types,
    filters,
)
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite.pygen.utils.text import to_snake


def load_cognite_client_from_toml(
    toml_file: Path | str = "config.toml", section: str | None = "cognite"
) -> CogniteClient:
    """
    This is a small helper function to load a CogniteClient from a toml file.

    The default name of the config file is "config.toml" and it should look like this:

    ```toml
    [cognite]
    project = "<cdf-project>"
    tenant_id = "<tenant-id>"
    cdf_cluster = "<cdf-cluster>"
    client_id = "<client-id>"
    client_secret = "<client-secret>"
    ```

    Args:
        toml_file: Path to toml file
        section: Name of the section in the toml file to use. If None, use the top level of the toml file.
                 Defaults to "cognite".

    Returns:
        A CogniteClient with configurations from the toml file.
    """
    import toml

    toml_content = toml.load(toml_file)
    if section is not None:
        toml_content = toml_content[section]

    login_flow = toml_content.pop("login_flow", None)
    if login_flow == "interactive":
        return CogniteClient.default_oauth_interactive(**toml_content)
    else:
        return CogniteClient.default_oauth_client_credentials(**toml_content)


class _CogniteCoreResourceAPI(Protocol[T_CogniteResourceList]):
    def retrieve_multiple(
        self,
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[SequenceNotStr[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> T_CogniteResourceList: ...

    def create(self, items: T_CogniteResourceList) -> T_CogniteResourceList: ...

    def delete(
        self,
        id: Optional[Union[int, Sequence[int]]] = None,
        external_id: Optional[Union[str, SequenceNotStr[str]]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None: ...


class _FileAPIAdapter(_CogniteCoreResourceAPI[FileMetadataList]):
    def __init__(self, files_api: FilesAPI):
        self._files_api = files_api

    def retrieve_multiple(
        self,
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[SequenceNotStr[str]] = None,
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
        external_id: Optional[Union[str, SequenceNotStr[str]]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        with suppress(CogniteNotFoundError):
            self._files_api.delete(id=id, external_id=external_id)


class CSVLoader:
    """
    Loads data from a CSV file into your CDF project.

    Args:
        source_dir: Path to directory containing CSV files.
        echo: Function to echo progress to. Defaults to print.
        data_set_id: ID of the data set to connect to CDF resources such as TimeSeries and Files . Defaults to None
                     meaning that no data set will be used.
        data_model: Data model to use. Defaults to None meaning that no data model will be used. This means you can
                    only load TimeSeries and Files.

    """

    def __init__(
        self,
        source_dir: pathlib.Path,
        echo: Optional[Callable[[str], None]] = None,
        data_set_id: int | None = None,
        data_model: DataModel[View] | None = None,
    ):
        self._source_dir = source_dir
        self._data_set_id = data_set_id
        self._echo: Callable[[str], None] = echo or print
        self._data_model = data_model
        self._space = data_model.space if data_model else ""

    def populate(self, client: CogniteClient) -> None:
        """
        Populate CDF with data found in the source directory passed in the constructor.

        This method will populate CDF with TimeSeries and Files. If a data model is passed in the constructor, it will
        also populate CDF with nodes and edges.

        Args:
            client: Connected CogniteClient

        """
        self.populate_timeseries(client)
        self.populate_files(client)
        if self._data_model:
            self.populate_nodes(client)
            self.populate_edges(client)

    def populate_timeseries(self, client: CogniteClient) -> TimeSeriesList:
        """
        Populate CDF with TimeSeries found in the source directory passed in the constructor. The TimeSeries
        is expected to be in a file called "TimeSeries.csv" in the source directory.

        Args:
            client: Connected CogniteClient

        Returns:
            A list of TimeSeries created in CDF.
        """
        return self._populate_cdf_resource(client.time_series, TimeSeries, TimeSeriesList)  # type: ignore[arg-type]

    def populate_files(self, client: CogniteClient) -> FileMetadataList:
        """
        Populate CDF with Files found in the source directory passed in the constructor. The Files
        is expected to be in a file called "FileMetadata.csv" in the source directory.

        Args:
            client: Connected CogniteClient

        Returns:
            A list of Files created in CDF.

        """
        return self._populate_cdf_resource(_FileAPIAdapter(client.files), FileMetadata, FileMetadataList)

    def populate_nodes(self, client: CogniteClient) -> NodeApplyResultList:
        """
        Populate CDF with nodes found in the source directory passed in the constructor. The nodes
        are expected to be in files matching the '[view.external_id].csv' for the data model passed in the
        constructor (e.g. 'MyView.csv' for a view with external_id 'MyView').

        Args:
            client: Connected CogniteClient

        Returns:
            A list of nodes created in CDF.
        """
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
        """
        Populate CDF with edges found in the source directory passed in the constructor. The edges
        are expected to be in files matching the '[view.external_id].[property_name].csv' for the data model passed in
        the constructor (e.g. 'MyView.MyProperty.csv' for a view with external_id 'MyView' and a property named
        'MyProperty').

        Args:
            client: Connected CogniteClient

        Returns:
            A list of edges created in CDF.
        """
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
        """
        self.clean_timeseries(client)
        self.clean_files(client)
        self.clean_nodes(client)
        self.clean_edges(client)

    def clean_timeseries(self, client: CogniteClient) -> None:
        """
        Delete all timeseries created by this loader.

        Args:
            client: Connected CogniteClient

        """
        self._clean_cdf_resource(client.time_series, TimeSeries)  # type: ignore[arg-type]

    def clean_files(self, client: CogniteClient) -> None:
        """
        Delete all files created by this loader.

        Args:
            client: Connected CogniteClient
        """
        self._clean_cdf_resource(_FileAPIAdapter(client.files), FileMetadata)

    def clean_nodes(self, client: CogniteClient) -> None:
        """
        Delete all nodes created by this loader.

        Args:
            client: Connected CogniteClient

        """
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
        """
        Delete all edges created by this loader.

        Args:
            client: Connected CogniteClient

        """
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
        elif (snake := to_snake(camel_case)) in d:
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
                row = converter.convert_dtypes(dict(zip(columns, raw_row, strict=False)))
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
    """
    This class is used to convert data types from string to the correct type for a CogniteResource.

    The class is initialized with a resource class and a dictionary of properties.
    The resource_class is used to infer the signature of the init method. The property dictionary is used
    for data modeling nodes and edges.

    The class is used by calling the convert_dtypes method with a dictionary of data.

    Args:
        resource_cls: The resource class to convert data types for.
        properties: A dictionary of properties for data modeling nodes and edges.

    Attributes:
        supported_timestamp_formats: Supported timestamp formats.

    """

    supported_timestamp_formats = ("%Y-%m-%dT%H:%M:%SZ", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S.%fZ")
    supported_date_formats = ("%Y-%m-%d", "%m/%d/%Y")

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
        properties: dict[str, MappedProperty] | None = None,
    ) -> dict[str, tuple[ContainerId, data_types.PropertyType]]:
        if properties is None:
            return {}
        return {name: (prop.container, prop.type) for name, prop in properties.items()}

    def convert_dtypes(self, entry: dict[str, Any]) -> dict[str, Any]:
        """
        Converts the data types of a dictionary according to the type hints and properties provided.

        !!! warning "Edge Case: mutation of input dictionary"
            In the case of that DirectRelation property, empty entries are removed. Thus, the input dictionary is
            modified in place. This is done as empty entries are not allowed in the CDF.

        Args:
            entry: The dictionary to convert.

        Returns:
            The converted dictionary.

        """
        if not self._type_hint_by_name and not self._property_by_name:
            return entry
        for key, value in list(entry.items()):
            snake_key = to_snake(key)
            if type_hint := self._type_hint_by_name.get(snake_key):
                entry[key] = self._convert_type_hint(value, type_hint)
            elif (pair := self._property_by_name.get(key)) or (pair := self._property_by_name.get(snake_key)):
                container, property_type = pair
                try:
                    entry[key] = self._convert_property(value, property_type, container)
                except ValueError as e:
                    if "cannot be empty" in str(e):
                        # Removing empty values
                        del entry[key]
                    else:
                        raise e

        return entry

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
        elif isinstance(property_type, data_types.Int32 | data_types.Int64):
            return int(value)
        elif isinstance(property_type, data_types.Float32 | data_types.Float64):
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
        elif isinstance(property_type, data_types.Date):
            for supported_format in cls.supported_date_formats:
                try:
                    return datetime.strptime(value, supported_format).date().isoformat()
                except ValueError:
                    pass
        elif isinstance(property_type, data_types.DirectRelation):
            if not value:
                raise ValueError("Direct relation cannot be empty")
            return {"externalId": str(value), "space": container_id.space}
        elif isinstance(property_type, data_types.CDFExternalIdReference):
            if not value:
                raise ValueError("CDF external ID reference cannot be empty")
            return {"externalId": str(value)}

        return value


def clean_model(client: CogniteClient, model_id: DataModelIdentifier, remove_space: bool = False) -> None:
    """
    Deletes the data model, the views and all the containers referenced by the views.

    Args:
        client: Connected CogniteClient
        model_id: ID of the data model to delete.
        remove_space: If True, the space will be deleted as well. Defaults to False.

    """
    model = client.data_modeling.data_models.retrieve(model_id, inline_views=True).latest_version()
    views = ViewList([view for view in model.views if not view.is_global])
    containers = list(
        {
            prop.container
            for view in views
            for prop in (view.properties or {}).values()
            if isinstance(prop, MappedProperty)
        }
    )

    if containers:
        deleted_containers = client.data_modeling.containers.delete(containers)
        print(f"Deleted {len(deleted_containers)} containers")
    if views:
        for _ in range(3):
            deleted_views = client.data_modeling.views.delete(views.as_ids())
            print(f"Deleted {len(deleted_views)} views")

            retrieved = client.data_modeling.views.retrieve(ids=views.as_ids())
            if not retrieved:
                break
            # Views are not always successfully deleted on the first try, so we have a retry logic.
            sleep(1)
    deleted_model = client.data_modeling.data_models.delete(model_id)
    print(f"Deleted {len(deleted_model)} data models")

    if remove_space:
        clean_space(client, model.space)


def clean_space(client: CogniteClient, space: str) -> None:
    """Deletes all data in a space.

    This means all nodes, edges, views, containers, and data models located in the space.

    Args:
        client: Connected CogniteClient
        space: The space to delete.

    """
    edges = client.data_modeling.instances.list("edge", limit=-1, filter=filters.Equals(["edge", "space"], space))
    if edges:
        instances = client.data_modeling.instances.delete(edges=edges.as_ids())
        print(f"Deleted {len(instances.edges)} edges")
    nodes = client.data_modeling.instances.list("node", limit=-1, filter=filters.Equals(["node", "space"], space))
    if nodes:
        instances = client.data_modeling.instances.delete(nodes=nodes.as_ids())
        print(f"Deleted {len(instances.nodes)} nodes")
    views = client.data_modeling.views.list(limit=-1, space=space)
    if views:
        deleted_views = client.data_modeling.views.delete(views.as_ids())
        print(f"Deleted {len(deleted_views)} views")
    containers = client.data_modeling.containers.list(limit=-1, space=space)
    if containers:
        deleted_containers = client.data_modeling.containers.delete(containers.as_ids())
        print(f"Deleted {len(deleted_containers)} containers")
    if data_models := client.data_modeling.data_models.list(limit=-1, space=space):
        deleted_data_models = client.data_modeling.data_models.delete(data_models.as_ids())
        print(f"Deleted {len(deleted_data_models)} data models")
    deleted_space = client.data_modeling.spaces.delete(space)
    print(f"Deleted space {deleted_space}")


def clean_model_interactive(client: CogniteClient, remove_space: bool = False) -> None:
    """
    Interactive version of clean_model.

    This will list all available spaces, and let the user select which one to delete from,
    and then list all available models in that space, and let the user select which one to delete.

    Args:
        client: Connected CogniteClient
        remove_space: If True, the space will be deleted as well. Defaults to False.

    """
    spaces = client.data_modeling.spaces.list(limit=-1)
    if not spaces:
        print("No spaces found")
        return
    index = _user_options(spaces.as_ids())
    selected_space = spaces[index]
    models = client.data_modeling.data_models.list(space=selected_space.space, limit=-1)
    if not models:
        print("No models found")
        return
    index = _user_options([model.as_id() for model in models])
    selected_model = models[index]
    clean_model(client, selected_model.as_id(), remove_space)


def _user_options(alternatives: list) -> int:
    for i, alternative in enumerate(alternatives, 1):
        print(f"{i}) {alternative}")
    print("\n q) Quit")
    while True:
        answer = input("Select option: ")
        if answer.casefold() == "q":
            raise KeyboardInterrupt
        try:
            return int(answer) - 1
        except ValueError:
            print("Invalid input, please try again")


def _unpack_filter(filter_: dm.Filter) -> list[tuple[list[type[dm.filters.CompoundFilter]], dm.Filter]]:
    """Unpacks a filter into a list of tuples of (compound filters, leaf filter)."""
    return __unpack_filter(filter_, [])


def __unpack_filter(
    filter_: dm.Filter, parents: list[type[dm.filters.CompoundFilter]]
) -> list[tuple[list[type[dm.filters.CompoundFilter]], dm.Filter]]:
    """Unpacks a filter into a list of tuples of (compound filters, leaf filter)."""
    if isinstance(filter_, dm.filters.CompoundFilter):
        output = []
        for child in filter_._filters:
            output.extend(__unpack_filter(child, [*parents, type(filter_)]))
        return output
    else:
        return [(parents, filter_)]


def _find_first_node_type(filter_: dm.filters.Filter | None) -> dm.DirectRelationReference | None:
    """Finds the node type if the filter has a node type equals filter.

    Args:
        filter_: The filter to search in.

    Returns:
        The node type if found, otherwise None.
    """
    if filter_ is None:
        return None

    filters_ = _unpack_filter(filter_)
    for parents, filter_ in filters_:
        if isinstance(filter_, dm.filters.Equals) and dm.filters.Not not in parents:
            dumped = filter_.dump()
            property_, value = dumped["equals"]["property"], dumped["equals"]["value"]
            if list(property_) == ["node", "type"] and "space" in value and "externalId" in value:
                return dm.DirectRelationReference(space=value["space"], external_id=value["externalId"])
    return None
