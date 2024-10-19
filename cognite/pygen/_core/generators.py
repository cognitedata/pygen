"""This module contains the SDK generator classes.

These are responsible for:
1. Generating the internal representation from the input views.
2. Use the internal representation with the Jinja templates to generate the SDK.
"""

from __future__ import annotations

import itertools
import warnings
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from functools import total_ordering
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any, Literal, cast

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from cognite.client.data_classes.data_modeling.data_types import Enum
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._version import __version__
from cognite.pygen.config import PygenConfig

from . import validation
from .models import CDFExternalField, DataClass, EdgeDataClass, FilterMethod, MultiAPIClass, NodeDataClass, fields
from .models.api_classes import APIClass, EdgeAPIClass, NodeAPIClass, QueryAPIClass, TimeSeriesAPIClass
from .validation import validate_api_classes_unique_names, validate_data_classes_unique_name


class SDKGenerator:
    """
    SDK generator for one or more data models.

    Args:
        top_level_package: The name of the top level package for the SDK. Example "movie.client"
        client_name: The name of the client class. Example "MovieClient"
        data_model: The data model(s) used to generate an SDK.
        default_instance_space: The default instance space to use for the SDK. If None, the first space in the data
                                model will be used.
        implements: Whether to use inheritance or composition for views that are implementing other views.
        logger: A logger function to use for logging. If None, print will be done.
    """

    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        data_model: dm.DataModel | Sequence[dm.DataModel],
        default_instance_space: str | None = None,
        implements: Literal["inheritance", "composition"] = "inheritance",
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

        self.default_instance_space = default_instance_space

        self._multi_api_generator = MultiAPIGenerator(
            top_level_package,
            client_name,
            list(self._data_model),
            self.default_instance_space,
            implements,
            logger,
            config,
        )
        self._multi_api_classes = [
            MultiAPIClass.from_data_model(
                model,
                {
                    (view_id := view.as_id()): self._multi_api_generator.api_by_type_by_view_id["node"][
                        view_id
                    ].api_class
                    for view in model.views
                    if view.as_id() in self._multi_api_generator.api_by_type_by_view_id["node"]
                },
                config.naming.multi_api_class,
            )
            for model in sorted(data_model, key=lambda model: (model.space, model.external_id, model.version))
        ]

        validation.validate_multi_api_classes_unique_names(self._multi_api_classes)

    @property
    def has_default_instance_space(self) -> bool:
        """Whether the SDK has a default instance space."""
        return self.default_instance_space is not None

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
                api_classes=sorted(self._multi_api_generator.apis),
                data_model=self._data_model[0],
                api_by_view_id=self._multi_api_generator.api_by_type_by_view_id["node"],
                multi_apis=self._multi_api_classes,
                has_default_instance_space=self.has_default_instance_space,
            )
            + "\n"
        )


def to_unique_parents_by_view_id(views: Sequence[dm.View]) -> dict[dm.ViewId, list[dm.ViewId]]:
    """Given a list of views, return a dictionary of unique parents for each view.

    Note that this is necessary due to the following situation (-> denotes implements):
     A -> B and C
     B -> C
    Then, A implements B and C, while B also implements C. This means that the unique parents for A
    is [B and C] and for B is [C].

    Args:
        views:

    Returns:

    """
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


def to_direct_children_by_view_id(views: Sequence[dm.View]) -> dict[dm.ViewId, list[dm.ViewId]]:
    """Given a list of views, return a dictionary of direct children for each view."""
    direct_children_by_view_id_: dict[dm.ViewId, set[dm.ViewId]] = defaultdict(set)
    for view in views:
        for parent in view.implements or []:
            direct_children_by_view_id_[parent].add(view.as_id())
    return {
        view_id: sorted(children, key=lambda v: v.external_id)
        for view_id, children in direct_children_by_view_id_.items()
    }


class MultiAPIGenerator:
    """This class is responsible for generating the API and Data Classes for multiple views."""

    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        data_models: list[dm.DataModel[dm.View]],
        default_instance_space: str | None,
        implements: Literal["inheritance", "composition"] = "inheritance",
        logger: Callable[[str], None] | None = None,
        config: PygenConfig = PygenConfig(),
    ):
        self.env = Environment(loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape())
        self.top_level_package = top_level_package
        self.client_name = client_name
        self.default_instance_space = default_instance_space
        self._implements = implements
        self._logger = logger or print
        seen_views: set[dm.ViewId] = set()
        unique_views: list[dm.View] = []
        # Used to verify that reverse direct relation's targets exist.
        direct_relations_by_view_id: dict[dm.ViewId, set[str]] = {}
        for view in itertools.chain.from_iterable(model.views for model in data_models):
            view_id = view.as_id()
            if view_id in seen_views:
                continue
            unique_views.append(view)
            seen_views.add(view_id)
            direct_relations_by_view_id[view_id] = {
                prop_name
                for prop_name, prop in view.properties.items()
                if isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation)
            }

        self.api_by_type_by_view_id = self.create_api_by_view_id_type(
            unique_views,
            default_instance_space is not None,
            config,
            base_name_functions=[
                DataClass.to_base_name,
                DataClass.to_base_name_with_version,
                DataClass.to_base_name_with_space,
                DataClass.to_base_name_with_space_and_version,
            ],
        )
        node_class_by_view_id: dict[dm.ViewId, NodeDataClass] = {
            view_id: api.data_class
            for view_id, api in self.api_by_type_by_view_id["node"].items()
            if isinstance(api.data_class, NodeDataClass)
        }
        edge_class_by_view_id: dict[dm.ViewId, EdgeDataClass] = {
            view_id: api.data_class
            for view_id, api in self.api_by_type_by_view_id["edge"].items()
            if isinstance(api.data_class, EdgeDataClass)
        }
        query_class_by_view_id = {
            view_id: api.query_api for view_id, api in self.api_by_type_by_view_id["node"].items()
        }
        parents = {parent for view in unique_views for parent in view.implements or []}
        parents_by_view_id = to_unique_parents_by_view_id(unique_views)
        direct_children_by_view_id = to_direct_children_by_view_id(unique_views)
        for api in self.apis:
            api.data_class.update_fields(
                api.view.properties,
                node_class_by_view_id,
                edge_class_by_view_id,
                unique_views,
                self.has_default_instance_space,
                direct_relations_by_view_id,
                config,
            )

            if implements == "inheritance":
                api.data_class.update_implements_interface_and_writable(
                    [
                        parent_api.data_class
                        for parent in parents_by_view_id[api.view_id]
                        # If the interface is not in the data model, then, we cannot inherit from it.
                        if (parent_api := self.api_by_type_by_view_id[api.used_for].get(parent))
                    ],
                    api.view_id in parents,
                )
            elif implements == "composition":
                api.data_class.initialization.add("parents")
            else:
                raise ValueError(f"Unknown implements value {implements}")

            api.data_class.update_direct_children(
                [
                    child_api.data_class
                    for child in direct_children_by_view_id.get(api.view_id, [])
                    if (child_api := self.api_by_type_by_view_id["node"].get(child))
                ],
            )

        # All data classes have been updated, before we can create edge APIs.
        for api in self.apis:
            api.create_edge_apis(query_class_by_view_id, self.api_by_type_by_view_id["node"])

        validate_api_classes_unique_names([api.api_class for api in self.apis])
        validate_data_classes_unique_name([api.data_class for api in self.apis])

        # Data Models require view external IDs to be unique within the data model.
        self._data_class_by_data_model_by_type = {
            model.as_id(): {
                view.external_id: (
                    node_class_by_view_id[view.as_id()]
                    if view.as_id() in node_class_by_view_id
                    else edge_class_by_view_id[view.as_id()]
                )
                for view in model.views
            }
            for model in data_models
        }

    @property
    def has_default_instance_space(self) -> bool:
        """Whether the SDK has a default instance space."""
        return self.default_instance_space is not None

    @property
    def apis(self) -> Iterator[APIGenerator]:
        """Iterate over the unique APIs."""
        for api_by_view_id in self.api_by_type_by_view_id.values():
            yield from api_by_view_id.values()

    @property
    def data_classes_topological_order(self) -> list[DataClass]:
        """Return the topological order of the data classes."""
        # Sorted by read name to ensure deterministic order
        dataclass_by_read_name = {}
        sorter: TopologicalSorter = TopologicalSorter()
        for data_class in sorted([api.data_class for api in self.apis], key=lambda d: d.read_name):
            sorter.add(data_class.read_name, *sorted(p.read_name for p in data_class.implements))
            dataclass_by_read_name[data_class.read_name] = data_class
        # TopologicalSorter is stable in the order the nodes were inserted, so we can rely on the sorted
        # data classes to be in the deterministic order
        return [dataclass_by_read_name[name] for name in sorter.static_order()]

    def __getitem__(self, view_id: dm.ViewId) -> APIGenerator:
        return self.api_by_type_by_view_id["node"][view_id]

    @classmethod
    def create_api_by_view_id_type(
        cls,
        views: list[dm.View],
        has_default_instance_space: bool,
        config: PygenConfig,
        base_name_functions: list[Callable[[dm.View], str]],
        selected_function: int = 0,
    ) -> dict[Literal["node", "edge"], dict[dm.ViewId, APIGenerator]]:
        """Create the API by view ID for the given views."""
        try:
            base_name_fun = base_name_functions[selected_function]
        except IndexError as e:
            raise ValueError("Failed to Generate SDK. Failed to find an unique data class name for each view.") from e

        view_by_id = {view.as_id(): view for view in views}
        api_by_view_id: dict[Literal["node", "edge"], dict[dm.ViewId, APIGenerator]] = {"node": {}, "edge": {}}
        base_name: str
        view: dm.View
        for base_name, view_group in itertools.groupby(
            sorted(((base_name_fun(view), view) for view in view_by_id.values()), key=lambda pair: pair[0]),
            key=lambda pair: pair[0],
        ):
            views_with_base_name = [view for _, view in view_group]
            if len(views_with_base_name) == 1:
                view = views_with_base_name[0]
                if view.used_for == "all":
                    api_by_view_id["node"][view.as_id()] = APIGenerator(
                        view, has_default_instance_space, config, "node", f"{base_name}Node"
                    )
                    api_by_view_id["edge"][view.as_id()] = APIGenerator(
                        view, has_default_instance_space, config, "edge", f"{base_name}Edge"
                    )
                elif view.used_for == "node" or view.used_for == "edge":
                    api_by_view_id[view.used_for][view.as_id()] = APIGenerator(
                        view, has_default_instance_space, config, view.used_for, base_name
                    )
                else:
                    warnings.warn("View used_for is not set. Skipping view", UserWarning, stacklevel=2)
                continue

            # The base name is not unique, so we need to try another base name function to separate the views.
            update = cls.create_api_by_view_id_type(
                views_with_base_name, has_default_instance_space, config, base_name_functions, selected_function + 1
            )
            api_by_view_id["node"].update(update["node"])
            api_by_view_id["edge"].update(update["edge"])

        return api_by_view_id

    def generate_apis(self, client_dir: Path) -> dict[Path, str]:
        """Generate the APIs for the SDK.

        Args:
            client_dir: The directory to generate the SDK in.

        Returns:
            A dictionary of file paths and file contents for the generated SDK.
        """
        data_classes_dir = client_dir / "data_classes"
        api_dir = client_dir / "_api"

        sdk: dict[Path, str] = {}
        for api in self.apis:
            file_name = api.api_class.file_name
            sdk[data_classes_dir / f"_{file_name}.py"] = api.generate_data_class_file()
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
        sdk[api_dir / "__init__.py"] = self.generate_api_init_file()

        sdk[data_classes_dir / "_core" / "base.py"] = self.generate_data_class_core_base_file()
        sdk[data_classes_dir / "_core" / "constants.py"] = self.generate_data_class_core_constants_file()
        sdk[data_classes_dir / "_core" / "helpers.py"] = self.generate_data_class_core_helpers_file()
        sdk[data_classes_dir / "_core" / "__init__.py"] = self.generate_data_class_core_init_file()
        sdk[data_classes_dir / "_core" / "query.py"] = self.generate_data_class_core_query_file()
        sdk[data_classes_dir / "_core" / "cdf_external.py"] = self.generate_data_class_core_cdf_external_file()
        return sdk

    def generate_api_core_file(self) -> str:
        """Generate the core API file for the SDK."""
        api_core = self.env.get_template("api_core.py.jinja")

        return (
            api_core.render(
                top_level_package=self.top_level_package,
                data_class_by_data_model_by_type=self._data_class_by_data_model_by_type,
                has_default_instance_space=self.has_default_instance_space,
            )
            + "\n"
        )

    def generate_api_init_file(self) -> str:
        """Generate the core API file for the SDK."""
        api_core = self.env.get_template("api_init.py.jinja")
        api_classes: list[APIClass] = []
        for api in self.apis:
            if api.used_for != "node":
                continue
            api_classes.append(api.api_class)
            api_classes.append(api.query_api)
            api_classes.extend(api.edge_apis or [])
            api_classes.extend(api.timeseries_apis or [])

        api_classes = sorted(api_classes, key=lambda api: api.name)

        return api_core.render(api_classes=api_classes).removeprefix("\n") + "\n"

    def generate_data_class_core_base_file(self) -> str:
        """Generate the core/base.py data classes file for the SDK."""
        data_class_core = self.env.get_template("data_classes_core_base.py.jinja")
        return data_class_core.render(has_default_instance_space=self.has_default_instance_space) + "\n"

    def generate_data_class_core_constants_file(self) -> str:
        """Generate the core/constants data classes file for the SDK."""
        data_class_core = self.env.get_template("data_classes_core_constants.py.jinja")
        return (
            data_class_core.render(
                default_instance_space=self.default_instance_space,
                has_default_instance_space=self.has_default_instance_space,
            )
            + "\n"
        )

    def generate_data_class_core_helpers_file(self) -> str:
        """Generate the core/helpers data classes file for the SDK."""
        data_class_core = self.env.get_template("data_classes_core_helpers.py.jinja")
        return data_class_core.render(has_default_instance_space=self.has_default_instance_space) + "\n"

    def generate_data_class_core_init_file(self) -> str:
        """Generate the core/__init__ data classes file for the SDK."""
        data_class_core = self.env.get_template("data_classes_core_init.py.jinja")
        return data_class_core.render() + "\n"

    def generate_data_class_core_query_file(self) -> str:
        """Generate the core data classes file for the SDK."""
        data_class_core = self.env.get_template("data_classes_core_query.py.jinja")
        return data_class_core.render(has_default_instance_space=self.has_default_instance_space) + "\n"

    def generate_data_class_core_cdf_external_file(self) -> str:
        """Generate the core data classes file for the SDK."""
        data_class_core = self.env.get_template("data_classes_core_cdf_external.py.jinja")
        return data_class_core.render() + "\n"

    def generate_client_init_file(self) -> str:
        """Generate the __init__.py file for the client.

        Returns:
            The generated __init__.py file as a string.
        """
        client_init = self.env.get_template("_client_init.py.jinja")
        return client_init.render(client_name=self.client_name, top_level_package=self.top_level_package) + "\n"

    def generate_data_classes_init_file(self) -> str:
        """Generate the __init__.py file for the data classes.

        Returns:
            The generated __init__.py file as a string.
        """
        data_class_init = self.env.get_template("data_classes_init.py.jinja")

        dependencies_by_names: dict[tuple[str, str, str, bool, bool], list[DataClass]] = defaultdict(list)
        for api in self.apis:
            dependencies = api.data_class.dependencies
            if dependencies:
                for dep in api.data_class.dependencies:
                    dependencies_by_names[
                        (
                            api.data_class.read_name,
                            api.data_class.graphql_name,
                            api.data_class.write_name,
                            api.data_class.is_writable or api.data_class.is_interface,
                            api.data_class.has_timeseries_fields(),
                        )
                    ].append(dep)
            elif has_timeseries_fields := api.data_class.has_timeseries_fields():
                dependencies_by_names[
                    (
                        api.data_class.read_name,
                        api.data_class.graphql_name,
                        api.data_class.write_name,
                        api.data_class.is_writable or api.data_class.is_interface,
                        has_timeseries_fields,
                    )
                ] = []

        return (
            data_class_init.render(
                classes=sorted([api.data_class for api in self.apis]),
                dependencies_by_names=dependencies_by_names,
                ft=fields,
                dm=dm,
            )
            + "\n"
        )

    def generate_typed_classes_file(
        self,
        include: set[dm.ViewId] | None = None,
        module_by_space: dict[str, str] | None = None,
        readonly_properties_by_view: dict[dm.ViewId, set[str]] | None = None,
    ) -> str:
        """Generate the typed classes file for the SDK.

        Args:
            include: The set of view IDs to include in the generated typed classes file.
                If None, all views will be included.
            module_by_space: A dictionary mapping space names to module names. This is used if part of the data model
                has been generated before and you want to reuse the generated classes. The keys are the space names
                and the values are the module names. For example, {"cdf_cdm": "cognite.client.data_classes.cdm.v1"},
                this will import all classes generated from views in the 'cdf_cdm' space from the
                'cognite.client.data_classes.cdm.v1' module.
            readonly_properties_by_view: A dictionary mapping view IDs to a set of readonly properties for that view.

        Returns:
            The generated typed classes file as a string.
        """
        typed_classes = self.env.get_template("typed_classes.py.jinja")
        available_dataclasses = self.data_classes_topological_order
        node_classes: list[NodeDataClass] = []
        edge_classes: list[EdgeDataClass] = []
        module_by_space = module_by_space or {}
        parent_classes_by_module: dict[str, list[str]] = defaultdict(list)
        for cls_ in available_dataclasses:
            if include is not None and cls_.view_id not in include:
                continue
            if module_ := module_by_space.get(cls_.view_id.space):
                parent_classes_by_module[module_].append(cls_.read_name)
                parent_classes_by_module[module_].append(f"{cls_.read_name}Apply")
                continue

            if isinstance(cls_, NodeDataClass):
                node_classes.append(cls_)
            elif isinstance(cls_, EdgeDataClass):
                edge_classes.append(cls_)
        for module in list(parent_classes_by_module.keys()):
            parent_classes_by_module[module] = sorted(parent_classes_by_module[module])

        datetime_import: str | None = None
        datetime_fields = {
            {dm.Timestamp: "datetime", dm.Date: "date"}[type(field.type_)]
            for cls_ in itertools.chain(node_classes, edge_classes)
            for field in cls_.fields_of_type(fields.BasePrimitiveField)
            if isinstance(field.type_, dm.Timestamp | dm.Date)
        }
        if datetime_fields:
            datetime_import = "from datetime import " + ", ".join(sorted(datetime_fields))
        has_literal_import = any(
            1
            for cls_ in itertools.chain(node_classes, edge_classes)
            for field in cls_.fields_of_type(fields.BasePrimitiveField)
            if isinstance(field.type_, Enum)
        )
        return (
            typed_classes.render(
                node_classes=node_classes,
                edge_classes=edge_classes,
                has_node_cls=bool(node_classes),
                has_edge_cls=bool(edge_classes),
                has_literal_import=has_literal_import,
                datetime_import=datetime_import,
                has_datetime_import=bool(datetime_import),
                len=len,
                parent_classes_by_module=parent_classes_by_module,
                readonly_properties_by_view=readonly_properties_by_view or {},
            )
            + "\n"
        )


@total_ordering
class APIGenerator:
    """This class is responsible for generating the API and Data Classes for a
    single view.

    Args:
        view: The view to generate the API and Data Classes for.
        has_default_instance_space: Whether the generated SDK has a default instance space.
        config: The configuration for the SDK generation
        base_name: The base name of the view. If None, the base name will be inferred from the view.

    Attributes:
        view: The view to generate the API and Data Classes for.
        data_class: The data class for the view.
        api_class: The API class for the view.
        query_api: The query API class for the view.

    """

    def __init__(
        self,
        view: dm.View,
        has_default_instance_space: bool,
        config: PygenConfig,
        used_for: Literal["node", "edge"],
        base_name: str | None = None,
    ):
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape()
        )
        self.view = view
        self.base_name = base_name or DataClass.to_base_name(view)
        self.has_default_instance_space = has_default_instance_space
        self._config = config
        self.used_for = used_for

        self.data_class = DataClass.from_view(view, self.base_name, used_for, config.naming.data_class)
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
        """The view ID of the view."""
        return self.view.as_id()

    def _validate_initialized(self) -> None:
        if missing := self.data_class.initialization - {"parents", "fields", "children"}:
            raise ValueError(f"APIGenerator has not been initialized. Missing {missing}")

    @property
    def list_method(self) -> FilterMethod:
        """The list method for the view."""
        if self._list_method is None:
            self._validate_initialized()
            self._list_method = FilterMethod.from_fields(
                self.data_class.fields,
                self._config.filtering,
                self.has_default_instance_space,
                isinstance(self.data_class, EdgeDataClass),
            )
        return self._list_method

    @property
    def timeseries_apis(self) -> list[TimeSeriesAPIClass]:
        """The timeseries APIs for the view."""
        if self._timeseries_apis is None:
            self._validate_initialized()
            self._timeseries_apis = [
                TimeSeriesAPIClass.from_field(
                    cast(CDFExternalField, field), self.base_name, self._config.naming.api_class
                )
                for field in self.data_class.primitive_fields_of_type(dm.TimeSeriesReference)
            ]
        return self._timeseries_apis

    def create_edge_apis(
        self,
        query_api_by_view_id: dict[dm.ViewId, QueryAPIClass],
        api_generator_by_view_id: dict[dm.ViewId, APIGenerator],
    ) -> None:
        """Create the edge APIs for the view."""
        self._edge_apis = [
            EdgeAPIClass.from_fields(
                field,  # type: ignore[arg-type]
                self.data_class,
                self.base_name,
                query_api_by_view_id,
                api_generator_by_view_id,
                self.has_default_instance_space,
                self._config,
            )
            for field in self.data_class.fields_of_type(fields.BaseConnectionField)  # type: ignore[type-abstract]
            if field.is_edge
        ]

    @property
    def edge_apis(self) -> list[EdgeAPIClass]:
        """The edge APIs for the view."""
        if self._edge_apis is None:
            raise ValueError("Please call create_edge_apis before accessing edge_apis.")
        return self._edge_apis

    @property
    def has_edge_api_dependencies(self) -> bool:
        """Whether the view has edge API dependencies."""
        return any(True for api in self.edge_apis if api.end_view_id != self.api_class.view_id)

    def generate_data_class_file(self) -> str:
        """Generate the data class file for the view.

        Returns:
            The generated data class file as a string.
        """
        unique_start_classes = []
        unique_end_classes = []
        grouped_edge_classes: dict[str, list[str]] = {}
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
            _grouped_edge_classes: dict[str, set[str]] = defaultdict(set)
            for edge_class in self.data_class.end_node_field.edge_classes:
                if "outwards" in edge_class.used_directions:
                    _grouped_edge_classes[edge_class.end_class.write_name].add(edge_class.start_class.write_name)
                elif "inwards" in edge_class.used_directions:
                    _grouped_edge_classes[edge_class.start_class.write_name].add(edge_class.end_class.write_name)
            for start_class, end_classes in sorted(_grouped_edge_classes.items(), key=lambda x: x[0]):
                grouped_edge_classes[start_class] = sorted(end_classes)
        else:
            raise ValueError(f"Unknown data class {type(self.data_class)}")

        if self.data_class.has_any_field_model_prefix:
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
                # ft = field types
                ft=fields,
                dm=dm,
                sorted=sorted,
                unique_start_classes=unique_start_classes,
                unique_end_classes=unique_end_classes,
                grouped_edge_classes=grouped_edge_classes,
                has_default_instance_space=self.has_default_instance_space,
            )
            + "\n"
        )

    def generate_api_file(self, top_level_package: str, client_name: str) -> str:
        """Generate the API file for the view.

        Args:
            top_level_package: The top level package for the SDK.
            client_name: The name of the client class.

        Returns:
            The generated API file as a string.
        """
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
                has_default_instance_space=self.has_default_instance_space,
                # ft = field types
                ft=fields,
                dm=dm,
            )
            + "\n"
        )

    def generate_api_query_file(self, top_level_package: str, client_name: str) -> str:
        """Generate the API query file for the view.

        This is the basis for the Python query functionality for the view.

        Args:
            top_level_package: The top level package for the SDK.
            client_name: The name of the client class.

        Returns:
            The generated API query file as a string.
        """
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
                has_edge_api_dependencies=self.has_edge_api_dependencies,
                unique_edge_data_classes=unique_edge_data_classes,
                # ft = field types
                ft=fields,
                dm=dm,
                sorted=sorted,
            )
            + "\n"
        )

    def generate_edge_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        """Generate the edge API files for the view.


        Args:
            top_level_package: The top level package for the SDK.
            client_name: The name of the client class.

        Returns:
            Iterator of tuples of file names and file contents for the edge APIs.
        """

        edge_class = self._env.get_template("api_class_edge.py.jinja")
        for edge_api in self.edge_apis:
            yield (
                edge_api.file_name,
                (
                    edge_class.render(
                        top_level_package=top_level_package,
                        client_name=client_name,
                        edge_api=edge_api,
                        api_class=self.api_class,
                        has_default_instance_space=self.has_default_instance_space,
                        # ft = field types
                        ft=fields,
                        dm=dm,
                    )
                    + "\n"
                ),
            )

    def generate_timeseries_api_files(self, top_level_package: str, client_name: str) -> Iterator[tuple[str, str]]:
        """Generate the timeseries API files for the view.

        Args:
            top_level_package: The top level package for the SDK.
            client_name: The name of the client class.

        Returns:
            Iterator of tuples of file names and file contents for the timeseries APIs.
        """
        timeseries_api = self._env.get_template("api_class_timeseries.py.jinja")
        for timeseries in self.timeseries_apis:
            yield (
                timeseries.file_name,
                (
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
                ),
            )


def _unique_data_classes(data_classes: Sequence[DataClass]) -> list[DataClass]:
    unique_data_classes: list[DataClass] = []
    seen = set()
    for data_class in data_classes:
        if data_class.read_name not in seen:
            seen.add(data_class.read_name)
            unique_data_classes.append(data_class)
    return unique_data_classes
