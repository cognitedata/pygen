from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.utils.text import create_name

from .data_classes import DataClass, EdgeDataClass
from .fields import CDFExternalField, EdgeOneToMany, EdgeOneToManyEdges


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


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class EdgeAPIClass(APIClass):
    start_class: DataClass
    end_class: DataClass
    edge_class: DataClass | None
    field_name: str
    type: dm.DirectRelationReference

    @property
    def has_edge_class(self) -> bool:
        return self.edge_class is not None

    @classmethod
    def from_field(
        cls, field: EdgeOneToMany, data_class: DataClass, base_name: str, api_class: pygen_config.APIClassNaming
    ) -> EdgeAPIClass:
        base_name = f"{base_name}_{field.name}"
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        parent_attribute = create_name(field.name, api_class.client_attribute)
        if isinstance(field, EdgeOneToManyEdges):
            edge_class = field.data_class
            if not isinstance(edge_class, EdgeDataClass):
                raise ValueError("Expected EdgeOneToManyEdges")
            end_class = next(
                (c.end_class for c in edge_class.end_node_field.edge_classes if c.edge_type == field.edge_type), None
            )
            if end_class is None:
                raise ValueError("Could not find end class")
        else:
            raise NotImplementedError()
            # Todo create a dm.Edge class
            edge_class = object()
            end_class = field.data_class

        return cls(
            parent_attribute=f"{parent_attribute}_edge",
            name=f"{class_name}API",
            file_name=file_name,
            edge_class=edge_class,
            field_name=field.name,
            type=field.edge_type,
            start_class=data_class,
            end_class=end_class,
        )


@dataclass(frozen=True)
class TimeSeriesAPIClass(APIClass):
    query_class: str
    prop_name: str
    variable: str

    @classmethod
    def from_field(
        cls, field: CDFExternalField, base_name: str, api_class: pygen_config.APIClassNaming
    ) -> TimeSeriesAPIClass:
        base_name = f"{base_name}_{field.name}"
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        parent_attribute = create_name(field.name, api_class.client_attribute)
        variable = create_name(field.name, api_class.variable)

        return cls(
            parent_attribute=f"{parent_attribute}",
            name=f"{class_name}API",
            file_name=f"{file_name}",
            query_class=f"{class_name}Query",
            prop_name=field.prop_name,
            variable=variable,
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
