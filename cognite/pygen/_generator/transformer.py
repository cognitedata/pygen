"""Transforms a data model into the PygenModel used for code generation."""

from typing import Literal, cast

from cognite.pygen._client.models import (
    DataModelResponseWithViews,
    ViewCorePropertyResponse,
    ViewResponse,
    ViewResponseProperty,
)
from cognite.pygen._generator.config import to_casing
from cognite.pygen._pygen_model import (
    APIClassFile,
    DataClass,
    DataClassFile,
    Field,
    FilterClass,
    ListDataClass,
    PygenSDKModel,
)
from cognite.pygen._utils.filesystem import sanitize

from ._types import OutputFormat
from .config import InternalPygenSDKConfig, NamingConfig
from .dtype_converter import DataTypeConverter, get_converter_by_format


def to_pygen_model(
    data_model: DataModelResponseWithViews, output_format: OutputFormat, config: InternalPygenSDKConfig
) -> PygenSDKModel:
    """Transforms a DataModelResponse into a PygenSDKModel for code generation.

    Args:
        data_model (DataModelResponse): The data model to transform.
        output_format (OutputFormat): The desired output format for the generated code.
        config (PygenSDKConfig): The SDK configuration.

    Returns:
        PygenSDKModel: The transformed PygenSDKModel.
    """
    if data_model.views is None:
        raise ValueError("Data model must have views to transform into PygenSDKModel.")

    model = PygenSDKModel(data_classes=[], api_classes=[])
    for view in data_model.views:
        if view.external_id in config.exclude_views:
            continue
        data_class = _create_data_class(view, config.naming, output_format)
        api_class = _create_api_class(data_class, view, config.naming, output_format)
        model.data_classes.append(data_class)
        model.api_classes.append(api_class)
    return model


def _create_data_class(view: ViewResponse, naming: NamingConfig, output_format: OutputFormat) -> DataClassFile:
    if view.used_for not in ("node", "edge"):
        raise NotImplementedError("Data classes for views used for 'all' are not supported yet.")
    else:
        used_for = cast(Literal["node", "edge"], view.used_for)

    write: DataClass | None = None
    if view.writable:
        write_converter = get_converter_by_format(output_format, context="write")
        write = DataClass(
            name=to_casing(f"{view.external_id}Write", naming.class_name),
            fields=[
                field_
                for prop_id, prop in view.properties.items()
                if (field_ := _create_field(prop_id, prop, naming, write_converter))
            ],
            display_name=view.name or view.external_id,
            description=view.description or "",
        )

    read_converter = get_converter_by_format(output_format, context="read")
    read = DataClass(
        name=to_casing(view.external_id, naming.class_name),
        fields=[
            field_
            for prop_id, prop in view.properties.items()
            if (field_ := _create_field(prop_id, prop, naming, read_converter))
        ],
        display_name=view.name or view.external_id,
        description=view.description or "",
    )

    read_list = ListDataClass(
        name=to_casing(f"{view.external_id}List", naming.class_name),
    )

    filter_class = FilterClass(
        name=to_casing(f"{view.external_id}Filter", naming.class_name),
    )

    return DataClassFile(
        filename=sanitize(f"{to_casing(view.external_id, naming.file_name)}.{_file_suffix(output_format)}"),
        instance_type=used_for,
        view_id=view.as_reference(),
        read=read,
        write=write,
        read_list=read_list,
        filter=filter_class,
    )


def _file_suffix(output_format: OutputFormat) -> str:
    if output_format == "python":
        return "py"
    elif output_format == "typescript":
        return "ts"
    else:
        raise NotImplementedError(f"Unsupported output format: {output_format}")


def _create_field(
    prop_id: str, prop: ViewResponseProperty, naming: NamingConfig, converter: DataTypeConverter
) -> Field | None:
    if not isinstance(prop, ViewCorePropertyResponse):
        return None
    return Field(
        cdf_prop_id=prop_id,
        name=to_casing(prop_id, naming.field_name),
        type_hint=converter.create_type_hint(prop),
        filter_name=converter.get_filter_name(prop),
        description=prop.description or "",
        default_value=converter.get_default_value(prop),
        dtype=converter.get_dtype(prop),
    )


def _create_api_class(
    data_class: DataClassFile, view: ViewResponse, naming: NamingConfig, output_format: OutputFormat
) -> APIClassFile:
    return APIClassFile(
        filename=sanitize(f"_{to_casing(view.external_id, naming.file_name)}_api.{_file_suffix(output_format)}"),
        name=to_casing(f"{view.external_id}API", naming.class_name),
        client_attribute_name=to_casing(f"{view.external_id}", naming.field_name),
        data_class=data_class,
    )
