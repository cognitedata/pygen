from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.utils.text import create_name

if TYPE_CHECKING:
    from . import DataClass


@dataclass(frozen=True)
class APIClass:
    client_attribute: str
    name: str
    file_name: str
    view_id: dm.ViewId
    data_class: DataClass

    @classmethod
    def from_view(
        cls, view: dm.View, view_name: str, api_class: pygen_config.APIClassNaming, data_class: DataClass
    ) -> APIClass:
        file_name = create_name(view_name, api_class.file_name)
        class_name = create_name(view_name, api_class.name)
        return cls(
            client_attribute=create_name(view_name, api_class.client_attribute),
            name=f"{class_name}API",
            file_name=file_name,
            view_id=view.as_id(),
            data_class=data_class,
        )


@dataclass(frozen=True)
class EdgeAPIClass:
    edge_api_file_name: str
    edge_api_class: str
    edge_api_attribute: str


@dataclass(frozen=True)
class QueryAPIClass:
    file_name: str
    class_name: str


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