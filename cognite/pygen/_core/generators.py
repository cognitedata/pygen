from __future__ import annotations

import itertools
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Callable, Literal, cast

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._version import __version__
from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.helper import get_pydantic_version

from . import validation
from .models import APIClass, DataClass, EdgeDataClass, MultiAPIClass, NodeDataClass


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
        self._multi_api_classes = sorted(
            (
                MultiAPIClass.from_data_model(
                    model, self._multi_api_generator.get(model.views), config.naming.multi_api_class
                )
                for model in data_model
            ),
            key=lambda a: a.name,
        )
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

        self.api_by_view: dict[dm.ViewId, APIGenerator] = {
            view.as_id(): APIGenerator(view, default_instance_space, config) for view in views
        }

        # Deal with duplicates.
        data_class_by_view_id: dict[dm.ViewId, DataClass] = {
            api.view_id: api.data_class for api in self.api_by_view.values()
        }
        for api, view in zip(self.api_by_view.values(), views):
            # if isinstance(api.data_class, EdgeWithPropertyDataClass):
            #     api.data_class.update_nodes(data_class_by_view_id, views, config.naming.field)
            api.data_class.update_fields(view.properties, data_class_by_view_id, config)

        validation.validate_data_classes_unique_name([api.data_class for api in self.api_by_view.values()])
        validation.validate_api_classes_unique_names([api.api_class for api in self.api_by_view.values()])

    def __getitem__(self, item: dm.ViewId) -> APIGenerator | None:
        return self.api_by_view.get(item)

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
        for api in self.apis:
            file_name = api.api_class.file_name
            sdk[data_classes_dir / f"_{file_name}.py"] = api.generate_data_class_file(self.pydantic_version == "v2")
            # if isinstance(api.data_class, EdgeWithPropertyDataClass):
            #     continue
            sdk[api_dir / f"{file_name}.py"] = api.generate_api_file(self.top_level_package, self.client_name)
            sdk[api_dir / f"{api.data_class.query_file_name}.py"] = api.generate_api_query_file(
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
        api_core = self.env.get_template("_core_api.py.jinja")
        return api_core.render(top_level_package=self.top_level_package) + "\n"

    def generate_data_class_core_file(self) -> str:
        data_class_core = self.env.get_template("_core_data_classes.py.jinja")
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

        data_classes_with_dependencies = sorted(
            (api.data_class for api in self.apis if api.data_class.has_edges or api.data_class.has_edge_with_property),
            key=lambda d: d.read_name,
        )
        dependencies_by_data_class_read_name: dict[str, list[DataClass]] = {}
        dependencies_by_data_class_write_name: dict[str, list[DataClass]] = {}
        for data_class in data_classes_with_dependencies:
            dependencies = sorted(data_class.dependencies, key=lambda d: d.read_name)
            dependencies_by_data_class_read_name[data_class.read_name] = dependencies
            dependencies_by_data_class_write_name[data_class.write_name] = dependencies

        return (
            data_class_init.render(
                classes=sorted((api.data_class for api in self.apis), key=lambda d: d.read_name),
                # Pydantic v1 needs read and write name separated, while v2 does not.
                dependencies_by_data_class_read_name=dependencies_by_data_class_read_name,
                dependencies_by_data_class_write_name=dependencies_by_data_class_write_name,
                # Pydantic v2 we just need a list of the names
                dependencies_by_data_class_name=sorted(
                    itertools.chain(dependencies_by_data_class_read_name, dependencies_by_data_class_write_name)
                ),
                top_level_package=self.top_level_package,
                import_file={
                    "v2": "data_classes_init_import.py.jinja",
                    "v1": "data_classes_init_import.py_pydanticv1.jinja",
                }[self.pydantic_version],
            )
            + "\n"
        )


class APIGenerator:
    def __init__(self, view: dm.View, default_instance_space: str, config: PygenConfig):
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape()
        )
        self.view_id = view.as_id()
        self.default_instance_space = default_instance_space
        self.data_class = DataClass.from_view(view, config.naming.data_class)
        self.api_class = APIClass.from_view(view, config.naming.api_class, self.data_class)
        self._config = config

    def generate_data_class_file(self, is_pydantic_v2: bool) -> str:
        if isinstance(self.data_class, NodeDataClass):
            type_data = self._env.get_template("data_class_node.py.jinja")
        # elif isinstance(self.data_class, EdgeWithPropertyDataClass):
        #     type_data = self._env.get_template("data_class_edge.py.jinja")
        else:
            raise ValueError(f"Unknown data class {type(self.data_class)}")

        return (
            type_data.render(
                data_class=self.data_class,
                list_method=self.data_class.list_method,
                instance_space=self.default_instance_space,
                is_pydantic_v2=is_pydantic_v2,
            )
            + "\n"
        )

    def generate_api_file(self, top_level_package: str, client_name: str) -> str:
        type_api = self._env.get_template("api_class.py.jinja")

        return (
            type_api.render(
                top_level_package=top_level_package,
                client_name=client_name,
                api_class=self.api_class,
                data_class=self.data_class,
                list_method=self.data_class.list_method,
                default_instance_space=self.default_instance_space,
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
                list_method=self.data_class.list_method,
                sorted=sorted,
            )
            + "\n"
        )

    def generate_edge_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        edge_api = self._env.get_template("api_class_edge.py.jinja")
        for field in self.data_class.one_to_many_edges:
            # Todo: There should be no if-condition here
            if isinstance(field.data_class, NodeDataClass):
                edge_class = EdgeDataClass.from_field_data_classes(
                    field,
                    cast(NodeDataClass, self.data_class),
                    self._config,
                )
                list_method = edge_class.list_method
                # elif isinstance(field.data_class, EdgeWithPropertyDataClass):
                #     edge_class = field.data_class
                list_method = field.data_class.list_method
            else:
                raise ValueError(f"Unknown data class {type(self.data_class)}")
            yield field.edge_api_file_name, (
                edge_api.render(
                    top_level_package=top_level_package,
                    client_name=client_name,
                    field=field,
                    api_class=self.api_class,
                    data_class=edge_class,
                    list_method=list_method,
                    default_instance_space=self.default_instance_space,
                )
                + "\n"
            )

    def generate_timeseries_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        timeseries_api = self._env.get_template("api_class_timeseries.py.jinja")
        for timeseries in self.data_class.single_timeseries_fields:
            yield timeseries.edge_api_file_name, (
                timeseries_api.render(
                    top_level_package=top_level_package,
                    client_name=client_name,
                    timeseries=timeseries,
                    api_class=self.api_class,
                    data_class=self.data_class,
                    list_method=self.data_class.list_method,
                )
                + "\n"
            )
