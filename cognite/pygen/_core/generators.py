from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Callable, Literal

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._version import __version__
from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.helper import get_pydantic_version

from . import validation
from .data_classes import APIClass, DataClass, ListMethod, MultiAPIClass, ViewSpaceExternalId
from .logic import get_unique_views


class SDKGenerator:
    """
    SDK generator for a data model.

    Args:
        top_level_package: The name of the top level package for the SDK. Example "movie.client"
        client_name: The name of the client class. Example "MovieClient"
        data_model: The data model(s) to generate a SDK for.
        pydantic_version: The version of pydantic to use. "infer" will use the version of pydantic installed in
                          the environment.
        logger: A logger function to use for logging. If None, print will be done.
    """

    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        data_model: dm.DataModel | Sequence[dm.DataModel],
        pydantic_version: Literal["v1", "v2", "infer"] = "infer",
        logger: Callable[[str], None] | None = None,
        config: PygenConfig = PygenConfig(),
    ):
        self._data_model = data_model
        self.top_level_package = top_level_package
        self.client_name = client_name
        self._multi_api_classes: list[MultiAPIClass]
        if isinstance(data_model, dm.DataModel):
            self._multi_api_generator = MultiAPIGenerator(
                top_level_package, client_name, data_model.views, pydantic_version, logger, config
            )
            self._multi_api_classes = []
        elif isinstance(data_model, Sequence):
            unique_views = get_unique_views(*[view for model in data_model for view in model.views])

            self._multi_api_generator = MultiAPIGenerator(
                top_level_package, client_name, unique_views, pydantic_version, logger, config
            )
            api_by_view_identifier = {api.view_identifier: api.api_class for api in self._multi_api_generator.sub_apis}

            self._multi_api_classes = sorted(
                (
                    MultiAPIClass.from_data_model(model, api_by_view_identifier, config.naming.multi_api_class)
                    for model in data_model
                ),
                key=lambda a: a.name,
            )
            validation.validate_multi_api_classes(self._multi_api_classes)
        else:
            raise ValueError("data_model must be a DataModel or a sequence of DataModels")

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
        api_client = self._multi_api_generator.env.get_template("_api_client.py.jinja")

        api_classes = sorted(
            (sub_api.api_class for sub_api in self._multi_api_generator.sub_apis),
            key=lambda api_class: api_class.name.lower(),
        )

        # In the template, we run zip(api_classes, views) and zip(multi_api_classes, view_sets)
        # thus it is important that the order is the same for both.
        views, view_sets = self._create_view_lists_in_order_of_api_classes(api_classes)

        return (
            api_client.render(
                client_name=self.client_name,
                pygen_version=__version__,
                cognite_sdk_version=cognite_sdk_version,
                pydantic_version=PYDANTIC_VERSION,
                api_classes=api_classes,
                is_single_model=isinstance(self._data_model, dm.DataModel),
                top_level_package=self.top_level_package,
                data_model=self._data_model,
                multi_api_classes=self._multi_api_classes,
                view_sets=view_sets,
                views=views,
                zip=zip,
            )
            + "\n"
        )

    def _create_view_lists_in_order_of_api_classes(
        self, api_classes: list[APIClass]
    ) -> tuple[list[dm.View], list[list[dm.View]]]:
        api_class_order: dict[ViewSpaceExternalId, int] = {
            api_class.view_id: i for i, api_class in enumerate(api_classes)
        }

        if isinstance(self._data_model, dm.DataModel):
            views = sorted(
                self._data_model.views,
                key=lambda v: api_class_order[ViewSpaceExternalId.from_(v)],
            )
            return views, []
        elif isinstance(self._data_model, Sequence):
            multi_api_class_order = {api_class.model_id: i for i, api_class in enumerate(self._multi_api_classes)}
            sorted_data_models = sorted(  # type: ignore[arg-type]
                self._data_model,
                key=lambda m: multi_api_class_order[m.as_id()],
            )
            view_sets: list[list[dm.View]] = []
            for data_model in sorted_data_models:
                views = sorted(data_model.views, key=lambda v: api_class_order[ViewSpaceExternalId.from_(v)])
                view_sets.append(views)
            return [], view_sets
        else:
            raise ValueError("data_model must be a DataModel or a sequence of DataModels")


class MultiAPIGenerator:
    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        views: Sequence[dm.View],
        pydantic_version: Literal["v1", "v2", "infer"] = "infer",
        logger: Callable[[str], None] | None = None,
        config: PygenConfig = PygenConfig(),
    ):
        self.env = Environment(loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape())
        self.top_level_package = top_level_package
        self.client_name = client_name
        self._pydantic_version = pydantic_version
        self._logger = logger or print

        self.sub_apis: list[APIGenerator] = [APIGenerator(view, config) for view in views]
        data_class_by_view_id: dict[ViewSpaceExternalId, DataClass] = {
            api.view_identifier: api.data_class for api in self.sub_apis
        }
        for api, view in zip(self.sub_apis, views):
            api.data_class.update_fields(view.properties, data_class_by_view_id, config.naming.field)

        validation.validate_data_classes([api.data_class for api in self.sub_apis])
        validation.validate_api_classes([api.api_class for api in self.sub_apis])

    def __getitem__(self, item: dm.View | dm.ViewId) -> APIGenerator | None:
        return next(
            (api for api in self.sub_apis if api.view_identifier == ViewSpaceExternalId.from_(item)),
            None,
        )

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
        for api in self.sub_apis:
            file_name = api.api_class.file_name
            sdk[data_classes_dir / f"_{file_name}.py"] = api.generate_data_class_file()
            sdk[api_dir / f"{file_name}.py"] = api.generate_api_file(self.top_level_package)

        sdk[client_dir / "__init__.py"] = self.generate_client_init_file()
        sdk[data_classes_dir / "__init__.py"] = self.generate_data_classes_init_file()
        sdk[api_dir / "_core.py"] = self.generate_api_core_file()

        sdk[data_classes_dir / "_core.py"] = self.generate_data_class_core_file()
        # if self.pydantic_version == "v2":
        return sdk

    def generate_api_core_file(self) -> str:
        api_core = self.env.get_template("_core_api.py.jinja")
        return api_core.render(top_level_package=self.top_level_package) + "\n"

    def generate_data_class_core_file(self) -> str:
        data_class_core = self.env.get_template("_core_data_classes.py.jinja")
        return data_class_core.render(is_pydantic_v2=self.pydantic_version == "v2") + "\n"

    def generate_client_init_file(self) -> str:
        client_init = self.env.get_template("_client_init.py.jinja")
        return client_init.render(client_name=self.client_name, top_level_package=self.top_level_package) + "\n"

    def generate_data_classes_init_file(self) -> str:
        data_class_init = self.env.get_template("data_classes_init.py.jinja")

        data_classes_with_dependencies = sorted(
            (api.data_class for api in self.sub_apis if api.data_class.has_edges), key=lambda d: d.read_name
        )
        dependencies_by_data_class_write_name = {
            data_class.write_name: sorted(data_class.dependencies, key=lambda d: d.read_name)
            for data_class in data_classes_with_dependencies
        }

        return (
            data_class_init.render(
                classes=sorted((api.data_class for api in self.sub_apis), key=lambda d: d.read_name),
                dependencies_by_data_class_write_name=dependencies_by_data_class_write_name,
                top_level_package=self.top_level_package,
                import_file={
                    "v2": "data_classes_init_import.py.jinja",
                    "v1": "data_classes_init_import.py_pydanticv1.jinja",
                }[self.pydantic_version],
            )
            + "\n"
        )


class APIGenerator:
    def __init__(self, view: dm.View, config: PygenConfig):
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"), autoescape=select_autoescape()
        )
        self.view_identifier = ViewSpaceExternalId.from_(view)
        self.data_class = DataClass.from_view(view, config.naming.data_class)
        self.api_class = APIClass.from_view(view, config.naming.api_class)

        # List method cannot be generated here, as we need all data class fields to bo updated first.
        self._config = config

    def generate_data_class_file(self) -> str:
        type_data = self._env.get_template("data_class.py.jinja")

        return type_data.render(data_class=self.data_class, space=self.view_identifier.space) + "\n"

    def generate_api_file(self, top_level_package: str) -> str:
        type_api = self._env.get_template("api_class.py.jinja")

        return (
            type_api.render(
                top_level_package=top_level_package,
                api_class=self.api_class,
                data_class=self.data_class,
                list_method=ListMethod.from_fields(self.data_class.fields, self._config.list_method),
            )
            + "\n"
        )
