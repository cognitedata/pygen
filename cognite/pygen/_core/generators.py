from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Callable, Literal
from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._core.data_classes import DataClass, APIClass, APIsClass
from cognite.pygen._core.logic import get_unique_views
from cognite.pygen._version import __version__
from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.helper import get_pydantic_version


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
        if isinstance(data_model, dm.DataModel):
            self._is_single_model = True
            self._apis = APIsGenerator(
                top_level_package, client_name, data_model.views, pydantic_version, logger, config
            )
            self._apis_classes = []
        elif isinstance(data_model, Sequence):
            self._is_single_model = False
            unique_views = get_unique_views(*[view for model in data_model for view in model.views])

            self._apis = APIsGenerator(top_level_package, client_name, unique_views, pydantic_version, logger, config)
            api_by_view_id = {api.view.as_id(): api.api_class for api in self._apis.apis}
            self._apis_classes = sorted(
                (APIsClass.from_data_model(model, api_by_view_id, config) for model in data_model), key=lambda a: a.name
            )
        else:
            raise ValueError("data_model must be a DataModel or a sequence of DataModels")

    def generate_sdk(self) -> dict[Path, str]:
        """
        Generate the SDK.

        Returns:
            A Python SDK given as a dictionary of file paths and file contents, which can be written to disk.
        """
        client_dir = Path(self.top_level_package.replace(".", "/"))
        sdk = self._apis.generate_apis(client_dir)
        sdk[client_dir / "_api_client.py"] = self._generate_api_client_file()
        return sdk

    def _generate_api_client_file(self) -> str:
        api_client = self._apis.env.get_template("_api_client.py.jinja")

        return (
            api_client.render(
                client_name=self.client_name,
                pygen_version=__version__,
                cognite_sdk_version=cognite_sdk_version,
                pydantic_version=PYDANTIC_VERSION,
                classes=sorted((api.api_class for api in self._apis.apis), key=lambda c: c.data_class),
                is_single_model=self._is_single_model,
                top_level_package=self.top_level_package,
                api_classes=self._apis_classes,
                data_model=self._data_model,
            )
            + "\n"
        )


class APIsGenerator:
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

        self.apis = [APIGenerator(view, config) for view in views]
        data_class_by_view_id = {api.view.as_id(): api.data_class for api in self.apis}
        for api in self.apis:
            api.data_class.update_fields(api.view.properties, data_class_by_view_id, config)

        # Validation
        # 1. Unique variable names for API and data classes.
        # 2. Unique file names for data classes.
        # 3. Unique file names for API classes.

        # self._dependencies_by_class = find_dependencies(self.apis)
        # self._static_dir = Path(__file__).parent / "static"

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
            try:
                sdk[data_classes_dir / f"_{file_name}.py"] = api.generate_data_class_file()
                sdk[api_dir / f"{file_name}.py"] = api.generate_api_file(self.top_level_package)
            except Exception as e:
                self._logger(f"Failed to generate SDK for view {api.view.name}: {e}")
                self._logger(f"Skipping view {api.view.name}")
                self._dependencies_by_class.pop(api.api_class, None)

        sdk[client_dir / "__init__.py"] = self.generate_client_init_file()
        sdk[data_classes_dir / "__init__.py"] = self.generate_data_classes_init_file()
        sdk[api_dir / "_core.py"] = self.generate_api_core_file()
        if self.pydantic_version == "v2":
            core_data_classes = "_core_data_classes.py"
        else:
            core_data_classes = "_core_data_classes_pydantic_v1.py"
        sdk[data_classes_dir / "_core.py"] = (self._static_dir / core_data_classes).read_text()
        return sdk

    def generate_api_core_file(self) -> str:
        api_core = self.env.get_template("_core_api.py.jinja")
        return api_core.render(top_level_package=self.top_level_package) + "\n"

    def generate_client_init_file(self) -> str:
        client_init = self.env.get_template("_client_init.py.jinja")
        return client_init.render(client_name=self.client_name, top_level_package=self.top_level_package) + "\n"

    def generate_data_classes_init_file(self) -> str:
        data_class_init = self.env.get_template("data_classes_init.py.jinja")
        return (
            data_class_init.render(
                classes=sorted((api.api_class for api in self.apis), key=lambda c: c.data_class),
                dependencies_by_class={
                    class_: sorted(dependencies, key=lambda c: c.data_class)
                    for class_, dependencies in sorted(
                        self._dependencies_by_class.items(), key=lambda x: x[0].data_class
                    )
                },
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

        self.view = view
        self.data_class = DataClass.from_view(view, config)
        self.api_class = APIClass.from_view(view, config)

    def generate_data_class_file(self) -> str:
        type_data = self._env.get_template("type_data.py.jinja")

        return type_data.render(data_class=self.data_class, view=self.view) + "\n"

    def generate_api_file(self, top_level_package: str) -> str:
        type_api = self._env.get_template("type_api.py.jinja")

        return type_api.render(
            top_level_package=top_level_package, api_class=self.api_class, data_class=self.data_class, view=self.view
        )


def find_dependencies(apis: list[APIGenerator]) -> dict[APIClass, set[APIClass]]:
    class_by_data_class_name = {api.api_class.data_class: api.api_class for api in apis}
    return {
        api.api_class: {class_by_data_class_name[d] for d in dependencies}
        for api in apis
        if (dependencies := api.fields.dependencies)
    }
