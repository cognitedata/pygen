from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.utils.text import create_name

from .fields import CDFExternalField, EdgeOneToMany


@dataclass(frozen=True)
class APIClass:
    parent_attribute: str
    name: str
    file_name: str


@dataclass(frozen=True)
class NodeAPIClass(APIClass):
    view_id: dm.ViewId

    @classmethod
    def from_view(cls, view: dm.View, base_name: str, api_class: pygen_config.APIClassNaming) -> NodeAPIClass:
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        return cls(
            parent_attribute=create_name(base_name, api_class.client_attribute),
            name=f"{class_name}API",
            file_name=file_name,
            view_id=view.as_id(),
        )


class QueryAPIClass(APIClass):
    @classmethod
    def from_view(cls, base_name: str, api_class: pygen_config.APIClassNaming) -> QueryAPIClass:
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        parent_attribute = create_name(base_name, api_class.client_attribute)
        return cls(
            parent_attribute=f"{parent_attribute}_query",  # Not used.
            name=f"{class_name}QueryAPI",
            file_name=f"{file_name}_query",
        )


class EdgeAPIClass(APIClass):
    @classmethod
    def from_field(cls, field: EdgeOneToMany, base_name: str, api_class: pygen_config.APIClassNaming) -> EdgeAPIClass:
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        parent_attribute = create_name(base_name, api_class.client_attribute)
        return cls(
            parent_attribute=f"{parent_attribute}_edge",
            name=f"{class_name}EdgeAPI",
            file_name=f"{file_name}_edge",
        )


class TimeSeriesAPIClass(APIClass):
    @classmethod
    def from_field(
        cls, field: CDFExternalField, base_name: str, api_class: pygen_config.APIClassNaming
    ) -> TimeSeriesAPIClass:
        base_name = f"{base_name}_{field.name}"
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        parent_attribute = create_name(field.name, api_class.client_attribute)
        return cls(
            parent_attribute=f"{parent_attribute}",
            name=f"{class_name}API",
            file_name=f"{file_name}",
        )


@dataclass(frozen=True)
class MultiAPIClass:
    """
    This represents a set of APIs which are generated from a single data model.

    The motivation for having this class is the case when you want to create one SDK for multiple data models.
    """

    sub_apis: list[APIClass]
    client_attribute: str
    name: str
    model: dm.DataModel[dm.View]

    @property
    def model_id(self) -> dm.DataModelId:
        return self.model.as_id()

    @classmethod
    def from_data_model(
        cls,
        data_model: dm.DataModel[dm.View],
        api_class_by_view_id: dict[dm.ViewId, APIClass],
        multi_api_class: pygen_config.MultiAPIClassNaming,
    ) -> MultiAPIClass:
        sub_apis = sorted([api_class_by_view_id[view.as_id()] for view in data_model.views], key=lambda api: api.name)

        data_model_name = data_model.name or data_model.external_id

        return cls(
            sub_apis=sub_apis,
            client_attribute=create_name(data_model_name, multi_api_class.client_attribute),
            name=f"{create_name(data_model_name, multi_api_class.name)}APIs",
            model=data_model,
        )
