from dataclasses import dataclass
from pathlib import Path

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._generator.config import PygenSDKConfig
from cognite.pygen._pygen_model import APIClassFile, DataClass, DataClassFile, Field, PygenSDKModel

from .generator import Generator


class PythonGenerator(Generator):
    format = "python"

    def __init__(self, data_model: DataModelResponseWithViews, config: PygenSDKConfig | None = None) -> None:
        super().__init__(data_model, config)
        self._top_level = self._get_top_level()
        self._package_generator = PythonPackageGenerator(self.model, self._top_level, self.config.client_name)

    def _get_top_level(self) -> str:
        """Get the top-level import path for the instance_api module."""
        if self.config.pygen_as_dependency:
            return "cognite.pygen._python"
        return f"{self.config.top_level_package}"

    def create_data_class_code(self, data_class: DataClassFile) -> str:
        generator = PythonDataClassGenerator(data_class, top_level=self._top_level)
        parts: list[str] = [
            generator.create_import_statements(),
        ]
        if data_class.write:
            parts.append(generator.generate_write_class())
        parts.extend(
            [
                generator.generate_read_class(),
                generator.generate_read_list_class(),
                generator.generate_filter_class(),
            ]
        )
        return "\n\n".join(parts)

    def create_api_class_code(self, api_class: APIClassFile) -> str:
        generator = PythonAPIGenerator(api_class, top_level=self._top_level)
        parts: list[str] = [
            generator.create_import_statements(),
            generator.create_api_class_with_init(),
            generator.create_retrieve_method(),
            generator.create_aggregate_method(),
            generator.create_search_method(),
            generator.create_iterate_method(),
            generator.create_list_method(),
        ]
        return "\n\n".join(parts)

    def create_data_class_init_code(self, model: PygenSDKModel) -> str:
        """Generate the data_classes/__init__.py file."""
        return self._package_generator.create_data_class_init()

    def create_api_init_code(self, model: PygenSDKModel) -> str:
        """Generate the _api/__init__.py file."""
        return self._package_generator.create_api_init()

    def create_client_code(self, model: PygenSDKModel) -> str:
        """Generate the client file."""
        return self._package_generator.create_client()

    def create_package_init_code(self, model: PygenSDKModel) -> str:
        """Generate the top-level __init__.py file."""
        return self._package_generator.create_package_init()

    def add_instance_api(self) -> dict[Path, str]:
        raise NotImplementedError()


class PythonDataClassGenerator:
    def __init__(self, data_class: DataClassFile, top_level: str = "cognite.pygen._python") -> None:
        self.data_class = data_class
        self.top_level = top_level

    def create_import_statements(self) -> str:
        """Generate import statements for the data class file."""
        import_statements: list[str] = ["from typing import ClassVar, Literal", "", "from pydantic import Field", ""]
        has_direct_relation = any(self.data_class.list_fields(dtype="InstanceId"))
        if has_direct_relation:
            import_statements.append(
                f"from {self.top_level}.instance_api.models._references import InstanceId, ViewReference"
            )
        else:
            import_statements.append(f"from {self.top_level}.instance_api.models._references import ViewReference")
        if time_fields := set(field.dtype for field in self.data_class.list_fields(dtype={"DateTime", "Date"})):
            import_statements.append(
                f"from {self.top_level}.instance_api.models._types import {','.join(sorted(time_fields))}"
            )

        filter_imports: set[str] = {"    FilterContainer,"}
        for field in self.data_class.read.fields:
            if field.filter_name:
                filter_imports.add(f"    {field.filter_name},")
        import_statements.extend(
            [
                f"from {self.top_level}.instance_api.models.dtype_filters import (",
                *sorted(filter_imports),
                ")",
            ]
        )
        import_statements.append(f"from {self.top_level}.instance_api.models.instance import (")
        import_statements.append("    Instance,")
        if has_direct_relation:
            import_statements.append("    InstanceId,")
        import_statements.append("    InstanceList,")
        if self.data_class.write:
            import_statements.append("    InstanceWrite,")
        import_statements.append(")")
        return "\n".join(import_statements)

    def generate_read_class(self) -> str:
        """Generate the read class for the data class."""
        read = self.data_class.read
        view_id = self.data_class.view_id
        instance_type = self.data_class.instance_type
        write_method = ""
        if self.data_class.write:
            write = self.data_class.write
            write_method = f'''
    def as_write(self) -> {write.name}:
        """Convert to write representation."""
        return {write.name}.model_validate(self.model_dump(by_alias=True))'''

        return f'''class {read.name}(Instance):
    """Read class for {read.display_name} instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="{view_id.space}", external_id="{view_id.external_id}", version="{view_id.version}"
    )
    instance_type: Literal["{instance_type}"] = Field("{instance_type}", alias="instanceType")
    {self.create_fields(read)}
{write_method}
'''

    def generate_write_class(self) -> str:
        write = self.data_class.write
        if not write:
            raise ValueError("No write class defined for this data class file.")
        view_id = self.data_class.view_id
        instance_type = self.data_class.instance_type
        return f'''class {write.name}(InstanceWrite):
    """Write class for {write.display_name} instances."""
    _view_id: ClassVar[ViewReference] = ViewReference(
        space="{view_id.space}", external_id="{view_id.external_id}", version="{view_id.version}"
    )
    instance_type: Literal["{instance_type}"] = Field("{instance_type}", alias="instanceType")
    {self.create_fields(write)}
'''

    @staticmethod
    def create_fields(data_class: DataClass) -> str:
        field_lines = []
        for field in data_class.fields:
            field_line = f"{field.name}: {field.type_hint}"
            args: list[str] = []
            if field.default_value is not None:
                args.append(f"default={field.default_value}")
            if field.cdf_prop_id != field.name:
                args.append(f'alias="{field.cdf_prop_id}"')
            if args:
                field_line += f' = Field({", ".join(args)})'
            field_lines.append(field_line)
        return "\n    ".join(field_lines)

    def generate_read_list_class(self) -> str:
        """Generate the list class for the data class."""
        read = self.data_class.read
        list_cls = self.data_class.read_list
        return f'''class {list_cls.name}(InstanceList[{read.name}]):
    """List of {read.display_name} instances."""
    _INSTANCE: ClassVar[type[{read.name}]] = {read.name}
'''

    def generate_filter_class(self) -> str:
        """Generate the filter class for the data class."""
        filter_class = self.data_class.filter
        read = self.data_class.read
        instance_type = self.data_class.instance_type

        attributes, names = self._create_filter_attributes()
        attribute_str = f"\n{' '*8}".join(attributes)
        attribute_list = f"\n{' '*16}".join(names)
        return f"""class {filter_class.name}(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = {read.name}._view_id
        {attribute_str}
        super().__init__(
            data_type_filters=[
                {attribute_list}
            ],
            operator=operator,
            instance_type="{instance_type}",
        )
"""

    def _create_filter_attributes(self) -> tuple[list[str], list[str]]:
        """Create filter attributes for the filter class."""
        attributes: list[str] = []
        names: list[str] = []
        read = self.data_class.read
        for field in read.fields:
            if not field.filter_name:
                continue
            attributes.append(f'self.{field.name} = {field.filter_name}(view_id, "{field.cdf_prop_id}", operator)')
            names.append(f"self.{field.name},")
        return attributes, names


@dataclass
class FilterParam:
    """Represents a filter parameter for an API method."""

    name: str
    type_hint: str
    docstring: str
    filter_call: str  # e.g., "filter_.name.equals_or_in(name)"


# Mapping from filter name to parameter generation
# Returns: list of (param_name_suffix, type_hint, docstring, filter_method_call)
_FILTER_PARAM_TEMPLATES: dict[str, list[tuple[str, str, str, str]]] = {
    "TextFilter": [
        ("", "str | list[str] | None", "Filter by exact {name} or list of values.", ".equals_or_in({param})"),
        ("_prefix", "str | None", "Filter by {name} prefix.", ".prefix({param})"),
    ],
    "IntegerFilter": [
        ("", "int | None", "Minimum {name} (inclusive).", ".greater_than_or_equals({param})"),
        ("", "int | None", "Maximum {name} (inclusive).", ".less_than_or_equals({param})"),
    ],
    "FloatFilter": [
        ("", "float | None", "Minimum {name} (inclusive).", ".greater_than_or_equals({param})"),
        ("", "float | None", "Maximum {name} (inclusive).", ".less_than_or_equals({param})"),
    ],
    "BooleanFilter": [
        ("", "bool | None", "Filter by {name}.", ".equals({param})"),
    ],
    "DateFilter": [
        ("", "date | None", "Minimum {name} (inclusive).", ".greater_than_or_equals({param})"),
        ("", "date | None", "Maximum {name} (inclusive).", ".less_than_or_equals({param})"),
    ],
    "DateTimeFilter": [
        ("", "datetime | None", "Minimum {name} (inclusive).", ".greater_than_or_equals({param})"),
        ("", "datetime | None", "Maximum {name} (inclusive).", ".less_than_or_equals({param})"),
    ],
    "DirectRelationFilter": [
        (
            "",
            "str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None",
            "Filter by {name} relation.",
            ".equals_or_in({param})",
        ),
    ],
}


def _create_filter_params(field: Field) -> list[FilterParam]:
    """Create filter parameters for a field based on its filter type."""
    if not field.filter_name:
        return []

    templates = _FILTER_PARAM_TEMPLATES.get(field.filter_name, [])
    params: list[FilterParam] = []

    # Determine if this is a range filter type (uses min_/max_ prefixes)
    range_filter_types = {"IntegerFilter", "FloatFilter", "DateFilter", "DateTimeFilter"}
    is_range_filter = field.filter_name in range_filter_types

    for i, (suffix, type_hint, docstring_template, filter_call_template) in enumerate(templates):
        # For range filters (min/max), use min_/max_ prefix
        if is_range_filter and len(templates) == 2 and suffix == "":
            prefix = "min_" if i == 0 else "max_"
            param_name = f"{prefix}{field.name}"
        else:
            param_name = f"{field.name}{suffix}"

        params.append(
            FilterParam(
                name=param_name,
                type_hint=type_hint,
                docstring=docstring_template.format(name=field.name.replace("_", " ")),
                filter_call=f"filter_.{field.name}{filter_call_template.format(param=param_name)}",
            )
        )

    return params


class PythonAPIGenerator:
    def __init__(self, api_class: APIClassFile, top_level: str = "cognite.pygen._python") -> None:
        self.api_class = api_class
        self.data_class = api_class.data_class
        self.top_level = top_level
        self._filter_params: list[FilterParam] | None = None

    @property
    def filter_params(self) -> list[FilterParam]:
        """Get all filter parameters for the API methods."""
        if self._filter_params is None:
            self._filter_params = []
            for field in self.data_class.read.fields:
                self._filter_params.extend(_create_filter_params(field))
        return self._filter_params

    def create_import_statements(self) -> str:
        """Generate import statements for the API class file."""
        lines: list[str] = ["from collections.abc import Sequence"]
        if datetime_imports := {
            field.dtype.lower() for field in self.data_class.list_fields(dtype={"DateTime", "Date"})
        }:
            lines.append(f"from datetime import {', '.join(sorted(datetime_imports))}")
        lines.extend(
            [
                "from typing import Literal, overload",
                "",
                f"from {self.top_level}.instance_api._api import InstanceAPI",
                f"from {self.top_level}.instance_api.http_client import HTTPClient",
                f"from {self.top_level}.instance_api.models import (",
                "    Aggregation,",
                "    InstanceId,",
                "    PropertySort,",
                "    ViewReference,",
                ")",
                f"from {self.top_level}.instance_api.models.responses import (",
                "    AggregateResponse,",
                "    Page,",
                ")",
                "",
            ]
        )
        read_name = self.data_class.read.name
        filter_name = self.data_class.filter.name
        list_name = self.data_class.read_list.name
        lines.extend(
            ["from ._data_class import (", f"    {read_name},", f"    {filter_name},", f"    {list_name},", ")"]
        )

        return "\n".join(lines)

    def create_api_class_with_init(self) -> str:
        """Generate the API class definition with __init__ method."""
        api_name = self.api_class.name
        read_name = self.data_class.read.name
        list_name = self.data_class.read_list.name
        view_id = self.data_class.view_id
        instance_type = self.data_class.instance_type

        return f'''
def _create_property_ref(view_ref: ViewReference, property_name: str) -> list[str]:
    """Create a property reference for filtering."""
    return [view_ref.space, f"{{view_ref.external_id}}/{{view_ref.version}}", property_name]


class {api_name}(InstanceAPI[{read_name}, {list_name}]):
    """API for {read_name} instances with type-safe filter methods."""

    def __init__(self, http_client: HTTPClient) -> None:
        view_ref = ViewReference(
            space="{view_id.space}", external_id="{view_id.external_id}", version="{view_id.version}"
        )
        super().__init__(http_client, view_ref, "{instance_type}", {list_name})'''

    def create_retrieve_method(self) -> str:
        """Generate the retrieve method with overloads."""
        read_name = self.data_class.read.name
        list_name = self.data_class.read_list.name
        instance_id_type = "str | InstanceId | tuple[str, str]"

        return f'''
    @overload
    def retrieve(
        self,
        id: {instance_id_type},
        space: str | None = None,
    ) -> {read_name} | None: ...

    @overload
    def retrieve(
        self,
        id: list[{instance_id_type}],
        space: str | None = None,
    ) -> {list_name}: ...

    def retrieve(
        self,
        id: {instance_id_type} | list[{instance_id_type}],
        space: str | None = None,
    ) -> {read_name} | {list_name} | None:
        """Retrieve {read_name} instances by ID.

        Args:
            id: Instance identifier(s). Can be a string, InstanceId, tuple, or list of these.
            space: Default space to use when id is a string.

        Returns:
            For single id: The {read_name} if found, None otherwise.
            For list of ids: A {list_name} of found instances.
        """
        return self._retrieve(id, space)'''

    def _create_filter_arguments(self, include_pagination: bool = False, include_sort: bool = False) -> str:
        """Create filter argument list for method signatures."""
        args: list[str] = []
        for param in self.filter_params:
            args.append(f"{param.name}: {param.type_hint} = None,")

        # Add common filter params
        args.extend(
            [
                "external_id_prefix: str | None = None,",
                "space: str | list[str] | None = None,",
            ]
        )

        if include_pagination:
            args.extend(
                [
                    "cursor: str | None = None,",
                    "limit: int = 25,",
                ]
            )
        elif include_sort:
            args.extend(
                [
                    "sort_by: str | None = None,",
                    'sort_direction: Literal["ascending", "descending"] = "ascending",',
                    "limit: int | None = 25,",
                ]
            )

        return "\n        ".join(args)

    def _create_filter_argument_docstrings(self) -> str:
        """Create docstring entries for filter arguments."""
        docs: list[str] = []
        for param in self.filter_params:
            docs.append(f"            {param.name}: {param.docstring}")

        docs.extend(
            [
                "            external_id_prefix: Filter by external ID prefix.",
                "            space: Filter by space.",
            ]
        )
        return "\n".join(docs)

    def _create_filter_calls(self) -> str:
        """Create filter method calls for building the filter."""
        filter_name = self.data_class.filter.name
        calls: list[str] = [f'filter_ = {filter_name}("and")']

        for param in self.filter_params:
            calls.append(param.filter_call)

        calls.extend(
            [
                "filter_.external_id.prefix(external_id_prefix)",
                "filter_.space.equals_or_in(space)",
            ]
        )
        return "\n        ".join(calls)

    def create_aggregate_method(self) -> str:
        """Generate the aggregate method."""
        filter_args = self._create_filter_arguments()
        filter_docs = self._create_filter_argument_docstrings()
        filter_calls = self._create_filter_calls()

        return f'''
    def aggregate(
        self,
        aggregate: Aggregation | Sequence[Aggregation],
        group_by: str | Sequence[str] | None = None,
        {filter_args}
    ) -> AggregateResponse:
        """Aggregate instances.

        Args:
            aggregate: Aggregation(s) to perform.
            group_by: Property or properties to group by.
{filter_docs}

        Returns:
            AggregateResponse with aggregated values.
        """
        {filter_calls}
        return self._aggregate(aggregate=aggregate, group_by=group_by, filter=filter_.as_filter())'''

    def create_search_method(self) -> str:
        """Generate the search method."""
        list_name = self.data_class.read_list.name
        filter_args = self._create_filter_arguments()
        filter_docs = self._create_filter_argument_docstrings()
        filter_calls = self._create_filter_calls()

        return f'''
    def search(
        self,
        query: str | None = None,
        properties: str | Sequence[str] | None = None,
        {filter_args}
        limit: int = 25,
    ) -> {list_name}:
        """Search instances using full-text search.

        Args:
            query: The search query string.
            properties: Properties to search in. If None, searches all text properties.
{filter_docs}
            limit: Maximum number of results.

        Returns:
            A {list_name} with matching instances.
        """
        {filter_calls}
        return self._search(query=query, properties=properties, limit=limit, filter=filter_.as_filter()).items'''

    def create_iterate_method(self) -> str:
        """Generate the iterate method."""
        list_name = self.data_class.read_list.name
        filter_args = self._create_filter_arguments(include_pagination=True)
        filter_docs = self._create_filter_argument_docstrings()
        filter_calls = self._create_filter_calls()

        return f'''
    def iterate(
        self,
        {filter_args}
    ) -> Page[{list_name}]:
        """Iterate over instances with pagination.

        Args:
{filter_docs}
            cursor: Pagination cursor from a previous page.
            limit: Maximum number of results per page (1-1000).

        Returns:
            A Page containing items and optional next cursor.
        """
        {filter_calls}
        return self._iterate(cursor=cursor, limit=limit, filter=filter_.as_filter())'''

    def create_list_method(self) -> str:
        """Generate the list method."""
        list_name = self.data_class.read_list.name
        filter_args = self._create_filter_arguments(include_sort=True)
        filter_docs = self._create_filter_argument_docstrings()
        filter_calls = self._create_filter_calls()

        return f'''
    def list(
        self,
        {filter_args}
    ) -> {list_name}:
        """List instances with type-safe filtering.

        Args:
{filter_docs}
            sort_by: Property name to sort by.
            sort_direction: Sort direction.
            limit: Maximum number of results. None for no limit.

        Returns:
            A {list_name} of matching instances.
        """
        {filter_calls}
        sort = None
        if sort_by is not None:
            prop_ref = _create_property_ref(self._view_ref, sort_by)
            sort = PropertySort(property=prop_ref, direction=sort_direction)

        return self._list(limit=limit, filter=filter_.as_filter(), sort=sort)'''


class PythonPackageGenerator:
    """Generator for Python package structure files (__init__.py, _client.py)."""

    def __init__(self, model: PygenSDKModel, top_level: str, client_name: str) -> None:
        self.model = model
        self.top_level = top_level
        self.client_name = client_name

    def create_data_class_init(self) -> str:
        """Generate the data_classes/__init__.py file.

        Exports all data classes (read, write, list, filter) from each view.
        """
        lines: list[str] = [
            '"""Data classes for the generated SDK.',
            "",
            "This module exports all data classes including read, write, list, and filter classes.",
            '"""',
            "",
        ]

        # Collect all exports
        all_exports: list[str] = []

        for data_class_file in self.model.data_classes:
            # Module name is filename without .py extension
            module_name = data_class_file.filename.replace(".py", "")

            # Build list of classes to import from this module
            classes: list[str] = [
                data_class_file.read.name,
                data_class_file.read_list.name,
                data_class_file.filter.name,
            ]
            if data_class_file.write:
                classes.insert(0, data_class_file.write.name)

            # Sort the classes alphabetically
            classes = sorted(classes)
            all_exports.extend(classes)

            # Generate import statement
            classes_str = ",\n    ".join(classes)
            lines.append(f"from .{module_name} import (")
            lines.append(f"    {classes_str},")
            lines.append(")")

        # Generate __all__ (sorted)
        lines.append("")
        lines.append("__all__ = [")
        for export in sorted(all_exports):
            lines.append(f'    "{export}",')
        lines.append("]")

        return "\n".join(lines)

    def create_api_init(self) -> str:
        """Generate the _api/__init__.py file.

        Exports all API classes.
        """
        lines: list[str] = [
            '"""API classes for the generated SDK.',
            "",
            "This module exports all view-specific API classes.",
            '"""',
            "",
        ]

        # Collect all API class names and sort them
        api_classes_sorted = sorted(self.model.api_classes, key=lambda x: x.name)

        for api_class in api_classes_sorted:
            # Module name is filename without .py extension
            module_name = api_class.filename.replace(".py", "")
            lines.append(f"from .{module_name} import {api_class.name}")

        # Generate __all__ (already sorted by class name)
        lines.append("")
        lines.append("__all__ = [")
        for api_class in api_classes_sorted:
            lines.append(f'    "{api_class.name}",')
        lines.append("]")

        return "\n".join(lines)

    def create_client(self) -> str:
        """
        Generate the _client.py file.

        Creates the client class that composes all API classes.
        """
        # Import API classes (sorted)
        api_classes_sorted = sorted(self.model.api_classes, key=lambda x: x.name)
        if api_classes_sorted:
            api_class_imports = f"from ._api import {', '.join(api.name for api in api_classes_sorted)}"
            view_list = "\n".join(
                f"    - {api_class.client_attribute_name}: {api_class.name}" for api_class in api_classes_sorted
            )
            api_inits = "\n".join(
                f"        self.{api_class.client_attribute_name} = {api_class.name}(self._http_client)"
                for api_class in api_classes_sorted
            )
        else:
            api_class_imports = ""
            view_list = "    This client does not provide access to any views."
            api_inits = "        pass"

        return f'''"""Client for the generated SDK.

This module contains the {self.client_name} that composes view-specific APIs.
"""

from {self.top_level}.instance_api._client import InstanceClient
from {self.top_level}.instance_api.config import PygenClientConfig

{api_class_imports}


class {self.client_name}(InstanceClient):
    """Generated client for interacting with the data model.

    This client provides access to the following views:
{view_list}
    """

    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        super().__init__(config)

        # Initialize view-specific APIs
{api_inits}
'''

    def create_package_init(self) -> str:
        """Generate the top-level __init__.py file.

        Exports only the client class for convenient access.
        """
        return f'''"""Generated SDK package.

This package provides the {self.client_name} for interacting with the data model.
"""

from ._client import {self.client_name}

__all__ = ["{self.client_name}"]
'''
