from __future__ import annotations

import itertools
import warnings
from collections import defaultdict
from collections.abc import Iterator, Sequence
from functools import total_ordering
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any, Callable, Literal, cast

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._version import __version__
from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.helper import get_pydantic_version

from . import validation
from .models import CDFExternalField, DataClass, EdgeDataClass, FilterMethod, MultiAPIClass, NodeDataClass, fields
from .models.api_casses import EdgeAPIClass, NodeAPIClass, QueryAPIClass, TimeSeriesAPIClass
from .validation import validate_api_classes_unique_names, validate_data_classes_unique_name


class SDKGenerator:
    """
    SDK generator for data model(s).

    Args:
        top_level_package: The name of the top level package for the SDK. Example "movie.client"
        client_name: The name of the client class. Example "MovieClient"
        data_model: The data model(s) used to generate an SDK.
        default_instance_space: The default instance space to use for the SDK. If None, the first space in the data
                                model will be used.
        pydantic_version: The version of pydantic to use. "infer" will use the version of pydantic installed in
                          the environment. "v1" will use pydantic v1, while "v2" will use pydantic v2.
        logger: A logger function to use for logging. If None, print will be done.
    """

    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        data_model: dm.DataModel | Sequence[dm.DataModel],
        default_instance_space: str | None = None,
        pydantic_version: Literal["v1", "v2", "infer"] = "infer",
        logger: Callable[[str], None] | None = None,
        config: PygenConfig = PygenConfig(),
    ):
        self.top_level_package = top_level_package
        self.client_name = client_name
        self._multi_api_classes: list[MultiAPIClass]
        if isinstance(data_model, dm.DataModel):
            data_model = [data_model]

        self._data_model = data_model

        if view_ids := [view for model in data_model for view in model.views if isinstance(view, dm.ViewId)]:
            raise ValueError(
                f"Data models ({', '.join(f'{model.space}, {model.external_id}' for model in data_model)}) "
                f"contains ViewIDs: {view_ids}. pygen requires Views to generate an SDK."
            )

        self.default_instance_space = default_instance_space or data_model[0].space

        self._multi_api_generator = MultiAPIGenerator(
            top_level_package,
            client_name,
            list(self._data_model),
            self.default_instance_space,
            pydantic_version,
            logger,
            config,
        )
        self._multi_api_classes = [
            MultiAPIClass.from_data_model(
                model,
                {
                    (view_id := view.as_id()): self._multi_api_generator.api_by_view_id[view_id].api_class
                    for view in model.views
                },
                config.naming.multi_api_class,
            )
            for model in sorted(data_model, key=lambda model: (model.space, model.external_id, model.version))
        ]

        validation.validate_multi_api_classes_unique_names(self._multi_api_classes)

    def generate_sdk(self) -> dict[Path, str]:
        """
        Generate the SDK.

        Returns:
            A Python SDK given as a dictionary of file paths and file contents, which can be written to disk.
        """
        client_dir = Path(self.top_level_package.replace(".", "/"))
        sdk = self._multi_api_generator.generate_apis(client_dir)
        sdk[client_dir / "_api_client.py"] = self._generate_api_client_file()
        return sdk

    def _generate_api_client_file(self) -> str:
        if len(self._multi_api_classes) == 1:
            api_client = self._multi_api_generator.env.get_template("_api_client_single_model.py.jinja")
        else:
            api_client = self._multi_api_generator.env.get_template("_api_client_multi_model.py.jinja")

        return (
            api_client.render(
                client_name=self.client_name,
                pygen_version=__version__,
                cognite_sdk_version=cognite_sdk_version,
                pydantic_version=PYDANTIC_VERSION,
                top_level_package=self.top_level_package,
                api_classes=sorted(self._multi_api_generator.unique_apis),
                data_model=self._data_model[0],
                api_by_view_id=self._multi_api_generator.api_by_view_id,
                multi_apis=self._multi_api_classes,
            )
            + "\n"
        )


def to_unique_parents_by_view_id(views: Sequence[dm.View]) -> dict[dm.ViewId, list[dm.ViewId]]:
    existing_views = {view.as_id() for view in views}
    parents_by_view_id: dict[dm.ViewId, set[dm.ViewId]] = {
        view.as_id(): {parent for parent in view.implements or [] if parent in existing_views} for view in views
    }
    ancestors_by_view_id: dict[dm.ViewId, set[dm.ViewId]] = defaultdict(set)
    for view_id in TopologicalSorter(parents_by_view_id).static_order():
        ancestors_by_view_id[view_id] = parents_by_view_id[view_id].union(
            *(ancestors_by_view_id[parent] for parent in parents_by_view_id[view_id])
        )

    unique_parents_by_view_id: dict[dm.ViewId, list[dm.ViewId]] = {}
    for view in views:
        ancestors = {
            grand_parent
            for parent in view.implements or []
            if parent in existing_views
            for grand_parent in ancestors_by_view_id[parent]
        }
        unique_parents_by_view_id[view.as_id()] = [
            parent for parent in view.implements or [] if parent in existing_views and parent not in ancestors
        ]
    return unique_parents_by_view_id


class MultiAPIGenerator:
    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        data_models: list[dm.DataModel[dm.View]],
        default_instance_space: str,
        pydantic_version: Literal["v1", "v2", "infer"] = "infer",
        logger: Callable[[str], None] | None = None,
        config: PygenConfig = PygenConfig(),
    ):
        self.env = Environment(loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape())
        self.top_level_package = top_level_package
        self.client_name = client_name
        self.default_instance_space = default_instance_space
        self._pydantic_version = pydantic_version
        self._logger = logger or print
        views = list(itertools.chain.from_iterable(model.views for model in data_models))
        self.api_by_view_id = self.create_api_by_view_id(
            list(views),
            default_instance_space,
            config,
            base_name_functions=[
                DataClass.to_base_name,
                DataClass.to_base_name_with_version,
                DataClass.to_base_name_with_space,
                DataClass.to_base_name_with_space_and_version,
            ],
        )

        data_class_by_view_id = {view_id: api.data_class for view_id, api in self.api_by_view_id.items()}
        query_class_by_view_id = {view_id: api.query_api for view_id, api in self.api_by_view_id.items()}
        interfaces = {parent for view in views for parent in view.implements or []}
        parents_by_view_id = to_unique_parents_by_view_id(views)
        for api in self.unique_apis:
            api.data_class.update_fields(api.view.properties, data_class_by_view_id, list(views), config)
            api.data_class.update_implements_interface_and_writable(
                [
                    parent_class
                    for parent in parents_by_view_id[api.view_id]
                    # If the interface is not in the data model, then, we cannot inherit from it.
                    if (parent_class := data_class_by_view_id.get(parent))
                ],
                api.view_id in interfaces,
            )

        # All data classes have been updated, before we can create edge APIs.
        for api in self.unique_apis:
            api.create_edge_apis(query_class_by_view_id)

        validate_api_classes_unique_names([api.api_class for api in self.unique_apis])
        validate_data_classes_unique_name([api.data_class for api in self.unique_apis])

        # Data Models require view external IDs to be unique within the data model.
        self._data_class_by_data_model_by_type = {
            model.as_id(): {view.external_id: data_class_by_view_id[view.as_id()] for view in model.views}
            for model in data_models
        }

    @property
    def unique_apis(self) -> Iterator[APIGenerator]:
        seen = set()
        for api in self.api_by_view_id.values():
            if api.view.as_id() not in seen:
                seen.add(api.view.as_id())
                yield api

    def __getitem__(self, view_id: dm.ViewId) -> APIGenerator:
        return self.api_by_view_id[view_id]

    @classmethod
    def create_api_by_view_id(
        cls,
        views: list[dm.View],
        default_instance_space: str,
        config: PygenConfig,
        base_name_functions: list[Callable[[dm.View], str]],
        selected_function: int = 0,
    ) -> dict[dm.ViewId, APIGenerator]:
        try:
            base_name_fun = base_name_functions[selected_function]
        except IndexError as e:
            raise ValueError("Failed to Generate SDK. Failed to find an unique data class name for each view.") from e

        view_by_id = {view.as_id(): view for view in views}
        api_by_view_id: dict[dm.ViewId, APIGenerator] = {}
        for base_name, view_group in itertools.groupby(
            sorted(((base_name_fun(view), view) for view in view_by_id.values()), key=lambda pair: pair[0]),
            key=lambda pair: pair[0],
        ):
            views_with_base_name = [view for _, view in view_group]
            if len(views_with_base_name) == 1:
                view = views_with_base_name[0]
                api_by_view_id[view.as_id()] = APIGenerator(view, default_instance_space, config, base_name)
                continue

            # The base name is not unique, so we need to try another base name function to separate the views.
            api_by_view_id.update(
                cls.create_api_by_view_id(
                    views_with_base_name, default_instance_space, config, base_name_functions, selected_function + 1
                )
            )

        return api_by_view_id

    @property
    def pydantic_version(self) -> Literal["v1", "v2"]:
        if self._pydantic_version == "infer":
            return get_pydantic_version()
        elif self._pydantic_version in ["v1", "v2"]:
            return self._pydantic_version
        else:
            raise ValueError(f"Unknown pydantic version {self._pydantic_version}")

    def generate_apis(self, client_dir: Path) -> dict[Path, str]:
        data_classes_dir = client_dir / "data_classes"
        api_dir = client_dir / "_api"

        sdk = {(api_dir / "__init__.py"): ""}
        for api in self.api_by_view_id.values():
            file_name = api.api_class.file_name
            sdk[data_classes_dir / f"_{file_name}.py"] = api.generate_data_class_file(self.pydantic_version == "v2")
            if isinstance(api.data_class, EdgeDataClass):
                continue
            sdk[api_dir / f"{file_name}.py"] = api.generate_api_file(self.top_level_package, self.client_name)

            sdk[api_dir / f"{api.query_api.file_name}.py"] = api.generate_api_query_file(
                self.top_level_package, self.client_name
            )

            for file_name, file_content in itertools.chain(
                api.generate_edge_api_files(self.top_level_package, self.client_name),
                api.generate_timeseries_api_files(self.top_level_package, self.client_name),
            ):
                sdk[api_dir / f"{file_name}.py"] = file_content

        sdk[client_dir / "__init__.py"] = self.generate_client_init_file()
        sdk[data_classes_dir / "__init__.py"] = self.generate_data_classes_init_file()
        sdk[api_dir / "_core.py"] = self.generate_api_core_file()

        sdk[data_classes_dir / "_core.py"] = self.generate_data_class_core_file()
        return sdk

    def generate_api_core_file(self) -> str:
        api_core = self.env.get_template("api_core.py.jinja")

        return (
            api_core.render(
                top_level_package=self.top_level_package,
                data_class_by_data_model_by_type=self._data_class_by_data_model_by_type,
                is_pydantic_v2=self.pydantic_version == "v2",
            )
            + "\n"
        )

    def generate_data_class_core_file(self) -> str:
        data_class_core = self.env.get_template("data_classes_core.py.jinja")
        return (
            data_class_core.render(
                is_pydantic_v2=self.pydantic_version == "v2", default_instance_space=self.default_instance_space
            )
            + "\n"
        )

    def generate_client_init_file(self) -> str:
        client_init = self.env.get_template("_client_init.py.jinja")
        return client_init.render(client_name=self.client_name, top_level_package=self.top_level_package) + "\n"

    def generate_data_classes_init_file(self) -> str:
        data_class_init = self.env.get_template("data_classes_init.py.jinja")

        dependencies_by_names: dict[tuple[str, str, str, bool], list[DataClass]] = defaultdict(list)
        for api in self.unique_apis:
            for dep in api.data_class.dependencies:
                dependencies_by_names[
                    (
                        api.data_class.read_name,
                        api.data_class.graphql_name,
                        api.data_class.write_name,
                        api.data_class.is_writable or api.data_class.is_interface,
                    )
                ].append(dep)

        return (
            data_class_init.render(
                classes=sorted([api.data_class for api in self.unique_apis]),
                is_pydantic_v2=self.pydantic_version == "v2",
                dependencies_by_names=dependencies_by_names,
                ft=fields,
                dm=dm,
            )
            + "\n"
        )


@total_ordering
class APIGenerator:
    def __init__(self, view: dm.View, default_instance_space: str, config: PygenConfig, base_name: str | None = None):
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape()
        )
        self.view = view
        self.base_name = base_name or DataClass.to_base_name(view)
        self.default_instance_space = default_instance_space
        self._config = config

        self.data_class = DataClass.from_view(view, self.base_name, config.naming.data_class)
        self.api_class = NodeAPIClass.from_view(
            view.as_id(), self.base_name, isinstance(self.data_class, EdgeDataClass), config.naming.api_class
        )
        self.query_api: QueryAPIClass = QueryAPIClass.create(self.data_class, self.base_name, config.naming.api_class)

        # These attributes require fields to be initialized
        self._list_method: FilterMethod | None = None
        self._timeseries_apis: list[TimeSeriesAPIClass] | None = None
        self._edge_apis: list[EdgeAPIClass] | None = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, APIGenerator):
            return NotImplemented
        return (self.base_name, self.view_id) == (other.base_name, other.view_id)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, APIGenerator):
            return NotImplemented
        return (self.base_name, self.view_id) < (other.base_name, other.view_id)

    def __repr__(self):
        return f"APIGenerator({self.base_name}, {self.view_id})"

    @property
    def view_id(self) -> dm.ViewId:
        return self.view.as_id()

    def _validate_initialized(self) -> None:
        if not self.data_class.fields:
            raise ValueError("APIGenerator has not been initialized.")

    @property
    def list_method(self) -> FilterMethod:
        if self._list_method is None:
            self._validate_initialized()
            self._list_method = FilterMethod.from_fields(
                self.data_class.fields, self._config.filtering, isinstance(self.data_class, EdgeDataClass)
            )
        return self._list_method

    @property
    def timeseries_apis(self) -> list[TimeSeriesAPIClass]:
        if self._timeseries_apis is None:
            self._validate_initialized()
            self._timeseries_apis = [
                TimeSeriesAPIClass.from_field(
                    cast(CDFExternalField, field), self.base_name, self._config.naming.api_class
                )
                for field in self.data_class.primitive_fields_of_type(dm.TimeSeriesReference)
            ]
        return self._timeseries_apis

    def create_edge_apis(self, query_api_by_view_id: dict[dm.ViewId, QueryAPIClass]) -> None:
        self._edge_apis = [
            EdgeAPIClass.from_fields(
                field,  # type: ignore[arg-type]
                self.data_class,
                self.base_name,
                query_api_by_view_id,
                self._config,
            )
            for field in itertools.chain(
                self.data_class.fields_of_type(fields.EdgeOneToMany),  # type: ignore[type-abstract]
                self.data_class.fields_of_type(fields.EdgeTypedOneToOne),
            )
        ]

    @property
    def edge_apis(self) -> list[EdgeAPIClass]:
        if self._edge_apis is None:
            raise ValueError("Please call create_edge_apis before accessing edge_apis.")
        return self._edge_apis

    def generate_data_class_file(self, is_pydantic_v2: bool) -> str:
        unique_start_classes = []
        unique_end_classes = []
        grouped_edge_classes = []
        if isinstance(self.data_class, NodeDataClass):
            type_data = self._env.get_template("data_class_node.py.jinja")
        elif isinstance(self.data_class, EdgeDataClass):
            type_data = self._env.get_template("data_class_edge.py.jinja")
            unique_start_classes = sorted(
                _unique_data_classes([edge.start_class for edge in self.data_class.end_node_field.edge_classes])
            )
            unique_end_classes = sorted(
                _unique_data_classes([edge.end_class for edge in self.data_class.end_node_field.edge_classes])
            )

            grouped_edge_classes = [
                (key, list(group))
                for key, group in itertools.groupby(
                    sorted(self.data_class.end_node_field.edge_classes), key=lambda c: c.end_class
                )
            ]
        else:
            raise ValueError(f"Unknown data class {type(self.data_class)}")

        def create_start_node_set(group: list[EdgeAPIClass]) -> str:
            return "{%s}" % ", ".join([g.start_class.write_name for g in group])

        if is_pydantic_v2 and self.data_class.has_any_field_model_prefix:
            names = ", ".join(field.name for field in self.data_class.fields if field.name.startswith("name"))
            warnings.warn(
                f"Field(s) {names} in view {self.view_id} has potential conflict with protected Pydantic "
                "namespace 'model_'",
                UserWarning,
                stacklevel=2,
            )

        return (
            type_data.render(
                data_class=self.data_class,
                list_method=self.list_method,
                is_pydantic_v2=is_pydantic_v2,
                # ft = field types
                ft=fields,
                dm=dm,
                sorted=sorted,
                unique_start_classes=unique_start_classes,
                unique_end_classes=unique_end_classes,
                grouped_edge_classes=grouped_edge_classes,
                create_start_node_set=create_start_node_set,
            )
            + "\n"
        )

    def generate_api_file(self, top_level_package: str, client_name: str) -> str:
        type_api = self._env.get_template("api_class_node.py.jinja")

        unique_edge_data_classes = _unique_data_classes([api.edge_class for api in self.edge_apis if api.edge_class])

        return (
            type_api.render(
                top_level_package=top_level_package,
                client_name=client_name,
                api_class=self.api_class,
                data_class=self.data_class,
                list_method=self.list_method,
                timeseries_apis=self.timeseries_apis,
                edge_apis=self.edge_apis,
                query_api=self.query_api,
                edge_data_classes=unique_edge_data_classes,
                # ft = field types
                ft=fields,
                dm=dm,
            )
            + "\n"
        )

    def generate_api_query_file(self, top_level_package: str, client_name: str) -> str:
        query_api = self._env.get_template("api_class_query.py.jinja")

        unique_edge_data_classes = _unique_data_classes([api.edge_class for api in self.edge_apis if api.edge_class])

        return (
            query_api.render(
                top_level_package=top_level_package,
                client_name=client_name,
                api_class=self.api_class,
                data_class=self.data_class,
                list_method=self.list_method,
                query_api=self.query_api,
                edge_apis=self.edge_apis,
                unique_edge_data_classes=unique_edge_data_classes,
                # ft = field types
                ft=fields,
                dm=dm,
                sorted=sorted,
            )
            + "\n"
        )

    def generate_edge_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        edge_class = self._env.get_template("api_class_edge.py.jinja")
        for edge_api in self.edge_apis:
            yield edge_api.file_name, (
                edge_class.render(
                    top_level_package=top_level_package,
                    client_name=client_name,
                    edge_api=edge_api,
                    api_class=self.api_class,
                    # ft = field types
                    ft=fields,
                    dm=dm,
                )
                + "\n"
            )

    def generate_timeseries_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        timeseries_api = self._env.get_template("api_class_timeseries.py.jinja")
        for timeseries in self.timeseries_apis:
            yield timeseries.file_name, (
                timeseries_api.render(
                    top_level_package=top_level_package,
                    client_name=client_name,
                    api_class=self.api_class,
                    data_class=self.data_class,
                    list_method=self.list_method,
                    timeseries_api=timeseries,
                    # ft = field types
                    ft=fields,
                    dm=dm,
                )
                + "\n"
            )


def _unique_data_classes(data_classes: Sequence[DataClass]) -> list[DataClass]:
    unique_data_classes: list[DataClass] = []
    seen = set()
    for data_class in data_classes:
        if data_class.read_name not in seen:
            seen.add(data_class.read_name)
            unique_data_classes.append(data_class)
    return unique_data_classes
