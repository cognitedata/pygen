from __future__ import annotations

import itertools
import json
from collections import defaultdict
from collections.abc import Iterator, Sequence
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
from .models import CDFExternalField, DataClass, FilterMethod, MultiAPIClass, NodeDataClass, fields
from .models.api_casses import EdgeAPIClass, NodeAPIClass, QueryAPIClass, TimeSeriesAPIClass


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
        self._data_model = data_model
        self.top_level_package = top_level_package
        self.client_name = client_name
        self._multi_api_classes: list[MultiAPIClass]
        if isinstance(data_model, dm.DataModel):
            data_model = [data_model]

        if view_ids := [view for model in data_model for view in model.views if isinstance(view, dm.ViewId)]:
            raise ValueError(
                f"Data models ({', '.join(f'{model.space}, {model.external_id}' for model in data_model)}) "
                f"contains ViewIDs: {view_ids}. pygen requires Views to generate an SDK."
            )

        self.default_instance_space = default_instance_space or data_model[0].space

        self._multi_api_generator = MultiAPIGenerator(
            top_level_package,
            client_name,
            [view for model in data_model for view in model.views],
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
            for model in data_model
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

        #
        # # In the template, we run zip(api_classes, views) and zip(multi_api_classes, view_sets)
        # # thus it is important that the order is the same for both.

        return (
            api_client.render(
                client_name=self.client_name,
                pygen_version=__version__,
                cognite_sdk_version=cognite_sdk_version,
                pydantic_version=PYDANTIC_VERSION,
                top_level_package=self.top_level_package,
            )
            + "\n"
        )


class MultiAPIGenerator:
    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        views: Sequence[dm.View],
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
        self.api_by_view_id = self.create_api_by_view_id(list(views), default_instance_space, config)
        data_class_by_view_id = {view_id: api.data_class for view_id, api in self.api_by_view_id.items()}

        for api in self.unique_apis:
            # if isinstance(api.data_class, EdgeWithPropertyDataClass):
            api.data_class.update_fields(api.view.properties, data_class_by_view_id, config)

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
        cls, views: list[dm.View], default_instance_space: str, config: PygenConfig
    ) -> dict[dm.ViewId, APIGenerator]:
        def dependent_base_names(prop: dm.ConnectionDefinition | dm.MappedProperty) -> set[str]:
            if isinstance(prop, dm.SingleHopConnectionDefinition):
                return {DataClass.to_base_name(view_by_id[prop.edge_source or prop.source])}
            elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and prop.source:
                return {DataClass.to_base_name(view_by_id[prop.source])}
            else:
                return set()

        view_by_id = {view.as_id(): view for view in views}
        api_by_view_id: dict[dm.ViewId, APIGenerator] = {}
        dependencies_by_base_name: dict[str, set[str]] = defaultdict(set)
        identical_base_names = set()
        views_by_base_name: dict[str, list[dm.View]] = {}
        for base_name, view_group in itertools.groupby(
            sorted(((DataClass.to_base_name(view), view) for view in views), key=lambda pair: pair[0]),
            key=lambda pair: pair[0],
        ):
            view_set = [pair[1] for pair in view_group]
            if len(view_set) == 1 or len({view.as_id() for view in view_set}) == 1:
                view = view_set[0]
                api_by_view_id[view.as_id()] = APIGenerator(view, default_instance_space, config, base_name)
                continue

            # We have multiple views with the same name, so we need to check if they can share API.
            properties_set = set()
            for view in view_set:
                independent_properties: dict[str, Any] = {}
                for prop_name, prop in view.properties.items():
                    dependency = dependent_base_names(prop)
                    if dependency:
                        dependencies_by_base_name[base_name].update(dependency)
                    else:
                        independent_properties[prop_name] = prop.dump()
                properties_set.add(json.dumps(independent_properties, sort_keys=True))

            if len(properties_set) == 1 and len(dependencies_by_base_name[base_name]) == 0:
                identical_base_names.add(base_name)
                view = max(view_set, key=lambda v: v.created_time)
                # All properties are the same, so we can share the API for these views.
                api = APIGenerator(view, default_instance_space, config, base_name)
                for view in view_set:
                    api_by_view_id[view.as_id()] = api
                continue
            elif len(properties_set) != 1:
                spaces = {view.space for view in view_set}
                # The properties are not the same, so we cannot share the API for these views.
                for view in view_set:
                    space_suffix = ""
                    if len(spaces) > 1:
                        space_suffix = f"_{view.space}"
                    api_by_view_id[view.as_id()] = APIGenerator(
                        view, default_instance_space, config, f"{base_name}{view.version}{space_suffix}"
                    )
                continue
            else:
                # The properties are the same, but there are dependencies, so we need to process all views before
                # we can determine if we can share the API.
                views_by_base_name[base_name] = view_set
                continue

        if views_by_base_name:
            # We have views with the same name, but with dependencies, so we need to process them again.
            while True:
                last_len = len(views_by_base_name)
                for base_name in list(views_by_base_name):
                    if all(d in identical_base_names for d in dependencies_by_base_name[base_name]):
                        view_set = views_by_base_name.pop(base_name)
                        view = max(view_set, key=lambda v: v.created_time)
                        api = APIGenerator(view, default_instance_space, config, base_name)
                        for view in view_set:
                            api_by_view_id[view.as_id()] = api
                        views_by_base_name.pop(base_name, None)
                        identical_base_names.add(base_name)
                if len(views_by_base_name) == last_len:
                    break
            # Todo Handle circular dependencies.
            for base_name, view_set in views_by_base_name.items():
                spaces = {view.space for view in view_set}
                for view in view_set:
                    space_suffix = ""
                    if len(spaces) > 1:
                        space_suffix = f"_{view.space}"
                    api_by_view_id[view.as_id()] = APIGenerator(
                        view, default_instance_space, config, f"{base_name}{view.version}{space_suffix}"
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
            # if isinstance(api.data_class, EdgeWithPropertyDataClass):
            sdk[api_dir / f"{file_name}.py"] = api.generate_api_file(self.top_level_package, self.client_name)
            raise NotImplementedError()
            # sdk[api_dir / f"{api.data_class.query_file_name}.py"] = api.generate_api_query_file(
            #     self.top_level_package, self.client_name
            # for file_name, file_content in itertools.chain(
            # ):

        sdk[client_dir / "__init__.py"] = self.generate_client_init_file()
        sdk[data_classes_dir / "__init__.py"] = self.generate_data_classes_init_file()
        sdk[api_dir / "_core.py"] = self.generate_api_core_file()

        sdk[data_classes_dir / "_core.py"] = self.generate_data_class_core_file()
        return sdk

    def generate_api_core_file(self) -> str:
        api_core = self.env.get_template("api_core.py.jinja")
        return api_core.render(top_level_package=self.top_level_package) + "\n"

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
        self.env.get_template("data_classes_init.py.jinja")
        raise NotImplementedError()
        #         api.data_class
        #         for api in self.unique_apis
        #         if api.data_class.has_edges or api.data_class.has_edge_with_property
        #     ),
        # for data_class in data_classes_with_dependencies:
        #
        # return (
        #     data_class_init.render(
        #         # Pydantic v1 needs read and write name separated, while v2 does not.
        #         # Pydantic v2 we just need a list of the names
        #         ),
        #         }[self.pydantic_version],
        #     + "\n"


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
        self.api_class = NodeAPIClass.from_view(view, self.base_name, config.naming.api_class)
        self.query_api = QueryAPIClass.from_view(self.base_name, config.naming.api_class)

        # These attributes require fields to be initialized
        self._list_method: FilterMethod | None = None
        self._timeseries_apis: list[TimeSeriesAPIClass] | None = None
        self._edge_apis: list[EdgeAPIClass] | None = None

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
            self._list_method = FilterMethod.from_fields(self.data_class.fields, self._config.filtering)
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

    @property
    def edge_apis(self) -> list[EdgeAPIClass]:
        if self._edge_apis is None:
            self._validate_initialized()
            self._edge_apis = [
                EdgeAPIClass.from_field(field, self.base_name, self._config.naming.api_class)
                for field in self.data_class.fields_of_type(fields.EdgeOneToMany)  # type: ignore[type-abstract]
            ]
        return self._edge_apis

    def generate_data_class_file(self, is_pydantic_v2: bool) -> str:
        if isinstance(self.data_class, NodeDataClass):
            type_data = self._env.get_template("data_class_node.py.jinja")
        else:
            raise ValueError(f"Unknown data class {type(self.data_class)}")

        return (
            type_data.render(
                data_class=self.data_class,
                list_method=self.list_method,
                is_pydantic_v2=is_pydantic_v2,
                # ft = field types
                ft=fields,
                dm=dm,
            )
            + "\n"
        )

    def generate_api_file(self, top_level_package: str, client_name: str) -> str:
        type_api = self._env.get_template("api_class_node.py.jinja")

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
                # ft = field types
                ft=fields,
                dm=dm,
            )
            + "\n"
        )

    def generate_api_query_file(self, top_level_package: str, client_name: str) -> str:
        query_api = self._env.get_template("api_class_query.py.jinja")

        return (
            query_api.render(
                top_level_package=top_level_package,
                client_name=client_name,
                api_class=self.api_class,
                data_class=self.data_class,
                list_method=self.list_method,
                sorted=sorted,
            )
            + "\n"
        )

    def generate_edge_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        self._env.get_template("api_class_edge.py.jinja")
        raise NotImplementedError()
        # for field in self.data_class.one_to_many_edges:
        #     # Todo: There should be no if-condition here
        #     if isinstance(field.data_class, NodeDataClass):
        #             field,
        #             self._config,
        #     yield field.edge_api_file_name, (
        #         edge_api.render(
        #         + "\n"

    def generate_timeseries_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        self._env.get_template("api_class_timeseries.py.jinja")
        raise NotImplementedError()
        # for timeseries in self.data_class.single_timeseries_fields:
        #     yield timeseries.edge_api_file_name, (
        #         timeseries_api.render(
        #         + "\n"
