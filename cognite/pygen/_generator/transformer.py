"""Transforms a data model into the PygenModel used for code generation."""

from typing import Literal, TypeAlias, cast

from pydantic import BaseModel
from pydantic.alias_generators import to_camel, to_pascal, to_snake

from cognite.pygen._client.models import (
    DataModelResponseWithViews,
    ViewCorePropertyResponse,
    ViewResponse,
    ViewResponseProperty,
)
from cognite.pygen._pygen_model import APIClassFile, DataClass, DataClassFile, Field, PygenSDKModel, ReadDataClass
from cognite.pygen._utils.filesystem import sanitize

from ._types import OutputFormat
from .config import NamingConfig, PygenSDKConfig
from .dtype_converter import DataTypeConverter, get_converter_by_format

Casing: TypeAlias = Literal["camelCase", "PascalCase", "snake_case"]


class StrictNamingConfig(BaseModel):
    class_name: Casing
    field_name: Casing


def to_pygen_model(
    data_model: DataModelResponseWithViews, output_format: OutputFormat, config: PygenSDKConfig | None = None
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
    config = config or PygenSDKConfig()
    naming = _create_naming(config.naming, output_format)

    model = PygenSDKModel(data_classes=[], api_classes=[])
    for view in data_model.views:
        if view.external_id in config.exclude_views:
            continue
        data_class = _create_data_class(view, naming, output_format)
        api_class = _create_api_class(data_class, view, naming, config)
        model.data_classes.append(data_class)
        model.api_classes.append(api_class)
    return model


def _create_naming(config: NamingConfig, output_format: OutputFormat) -> StrictNamingConfig:
    """Creates a naming strategy based on the configuration and output format.

    Args:
        config (NamingConfig): The naming configuration.
        output_format (OutputFormat): The desired output format for the generated code.
    Returns:
        Naming strategy object.
    """
    language = _get_naming_config(output_format)
    return StrictNamingConfig(
        class_name=language.class_name if config.class_name == "language_default" else config.class_name,
        field_name=language.field_name if config.field_name == "language_default" else config.field_name,
    )


def _get_naming_config(output_format: OutputFormat) -> StrictNamingConfig:
    if output_format == "python":
        return StrictNamingConfig(class_name="PascalCase", field_name="snake_case")
    elif output_format == "typescript":
        return StrictNamingConfig(class_name="PascalCase", field_name="camelCase")
    raise NotImplementedError(f"Naming config for output format {output_format} is not implemented.")


def _create_data_class(view: ViewResponse, naming: StrictNamingConfig, output_format: OutputFormat) -> DataClassFile:
    if view.used_for not in ("node", "edge"):
        raise NotImplementedError("Data classes for views used for 'all' are not supported yet.")
    else:
        used_for = cast(Literal["node", "edge"], view.used_for)

    write: DataClass | None = None
    if view.writable:
        write_converter = get_converter_by_format(output_format, context="write")
        write = DataClass(
            view_id=view.as_reference(),
            name=_to_casing(f"{view.external_id}Write", naming.class_name),
            fields=[
                field_
                for prop_id, prop in view.properties.items()
                if (field_ := _create_field(prop_id, prop, naming, write_converter))
            ],
            instance_type=used_for,
            display_name=view.name or view.external_id,
            description=view.description or "",
        )

    read_converter = get_converter_by_format(output_format, context="read")
    read = ReadDataClass(
        view_id=view.as_reference(),
        name=_to_casing(view.external_id, naming.class_name),
        fields=[
            field_
            for prop_id, prop in view.properties.items()
            if (field_ := _create_field(prop_id, prop, naming, read_converter))
        ],
        instance_type=used_for,
        display_name=view.name or view.external_id,
        description=view.description or "",
        write_class_name=write.name if write else None,
    )

    return DataClassFile(filename=sanitize(f"{view.external_id}.py"), read=read, write=write)


def _to_casing(name: str, casing: Casing) -> str:
    if casing == "camelCase":
        return to_camel(name)
    elif casing == "PascalCase":
        return to_pascal(name)
    elif casing == "snake_case":
        return to_snake(name)
    else:
        raise ValueError(f"Unsupported casing: {casing}")


def _create_field(
    prop_id: str, prop: ViewResponseProperty, naming: StrictNamingConfig, converter: DataTypeConverter
) -> Field | None:
    if not isinstance(prop, ViewCorePropertyResponse):
        return None
    return Field(
        cdf_prop_id=prop_id,
        name=_to_casing(prop_id, naming.field_name),
        type_hint=converter.create_type_hint(prop),
        filter_name=converter.get_filter_name(prop),
        description=prop.description or "",
    )


def _create_api_class(
    data_class: DataClassFile,
    view: ViewResponse,
    naming: StrictNamingConfig,
    config: PygenSDKConfig,
) -> APIClassFile:
    return APIClassFile(
        filename=sanitize(f"_{view.external_id}_api.py"),
    )
