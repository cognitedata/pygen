from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.utils.text import create_name

from .data_classes import DataClass, EdgeDataClass
from .fields import CDFExternalField, EdgeOneToMany, EdgeOneToManyEdges
from .filter_method import FilterMethod, FilterParameter


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
    filter_method: FilterMethod
    doc_name: str

    def filter_parameters(self, include_nodes: bool = True) -> Iterator[FilterParameter]:
        if include_nodes:
            yield FilterParameter(
                name=f"from_{self.start_class.variable}",
                type_="str | list[str] | dm.NodeId | list[dm.NodeId]",
                description=f"ID of the source { self.start_class.doc_name}.",
                default=None,
            )
            yield FilterParameter(
                name=f"from_{self.start_class.variable}_space",
                type_="str",
                description=f"Location of the {self.start_class.doc_list_name}.",
                default="DEFAULT_INSTANCE_SPACE",
                is_nullable=False,
            )
            yield FilterParameter(
                name=f"to_{self.end_class.variable}",
                type_="str | list[str] | dm.NodeId | list[dm.NodeId]",
                description=f"ID of the target { self.end_class.doc_name}.",
                default=None,
            )
            yield FilterParameter(
                name=f"to_{self.end_class.variable}_space",
                type_="str",
                description=f"Location of the {self.end_class.doc_list_name}.",
                default="DEFAULT_INSTANCE_SPACE",
                is_nullable=False,
            )
        yield from self.filter_method.parameters

    def has_edge_class(self) -> bool:
        return self.edge_class is not None

    @classmethod
    def from_field(
        cls, field: EdgeOneToMany, data_class: DataClass, base_name: str, pygen_config: pygen_config.PygenConfig
    ) -> EdgeAPIClass:
        api_class = pygen_config.naming.api_class
        base_name = f"{base_name}_{field.name}"
        file_name = create_name(base_name, api_class.file_name)
        class_name = create_name(base_name, api_class.name)
        parent_attribute = create_name(field.name, api_class.client_attribute)

        edge_class: DataClass | None
        end_class: DataClass
        if isinstance(field, EdgeOneToManyEdges):
            edge_class = field.data_class
            if not isinstance(edge_class, EdgeDataClass):
                raise ValueError("Expected EdgeOneToManyEdges")
            try:
                end_class = next(
                    c.end_class for c in edge_class.end_node_field.edge_classes if c.edge_type == field.edge_type
                )
            except StopIteration:
                raise ValueError("Could not find end class") from None
            filter_method = FilterMethod.from_fields(edge_class.fields, pygen_config.filtering, is_edge_class=True)
        else:
            edge_class = None
            end_class = field.data_class
            filter_method = FilterMethod.from_fields([], pygen_config.filtering)

        return cls(
            parent_attribute=f"{parent_attribute}_edge",
            name=f"{class_name}API",
            file_name=file_name,
            edge_class=edge_class,
            field_name=field.name,
            type=field.edge_type,
            start_class=data_class,
            end_class=end_class,
            filter_method=filter_method,
            doc_name=create_name(field.name, api_class.doc_name),
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
