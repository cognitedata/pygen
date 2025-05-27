from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainRelation,
    DomainRelationWrite,
    DomainRelationList,
    DomainRelationWriteList,
    GraphQLCore,
    ResourcesWrite,
    DomainModelList,
    T_DomainList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_read_args,
    as_write_args,
    as_pygen_node_id,
    is_tuple_id,
    EdgeQueryCore,
    NodeQueryCore,
    QueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
    FloatFilter,
    IntFilter,
    TimestampFilter,
)
from cognite_core.data_classes._cognite_annotation import CogniteAnnotation, CogniteAnnotationWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystem,
        CogniteSourceSystemGraphQL,
        CogniteSourceSystemWrite,
    )


__all__ = [
    "CogniteDiagramAnnotation",
    "CogniteDiagramAnnotationWrite",
    "CogniteDiagramAnnotationList",
    "CogniteDiagramAnnotationWriteList",
    "CogniteDiagramAnnotationFields",
    "CogniteDiagramAnnotationTextFields",
]


CogniteDiagramAnnotationTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "end_node_text",
    "name",
    "source_context",
    "source_created_user",
    "source_id",
    "source_updated_user",
    "start_node_text",
    "tags",
]
CogniteDiagramAnnotationFields = Literal[
    "external_id",
    "aliases",
    "confidence",
    "description",
    "end_node_page_number",
    "end_node_text",
    "end_node_x_max",
    "end_node_x_min",
    "end_node_y_max",
    "end_node_y_min",
    "name",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "start_node_page_number",
    "start_node_text",
    "start_node_x_max",
    "start_node_x_min",
    "start_node_y_max",
    "start_node_y_min",
    "status",
    "tags",
]
_COGNITEDIAGRAMANNOTATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "confidence": "confidence",
    "description": "description",
    "end_node_page_number": "endNodePageNumber",
    "end_node_text": "endNodeText",
    "end_node_x_max": "endNodeXMax",
    "end_node_x_min": "endNodeXMin",
    "end_node_y_max": "endNodeYMax",
    "end_node_y_min": "endNodeYMin",
    "name": "name",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "start_node_page_number": "startNodePageNumber",
    "start_node_text": "startNodeText",
    "start_node_x_max": "startNodeXMax",
    "start_node_x_min": "startNodeXMin",
    "start_node_y_max": "startNodeYMax",
    "start_node_y_min": "startNodeYMin",
    "status": "status",
    "tags": "tags",
}


class CogniteDiagramAnnotationGraphQL(GraphQLCore):
    """This represents the reading version of Cognite diagram annotation, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite diagram annotation.
        data_record: The data record of the Cognite diagram annotation node.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        confidence: The confidence that the annotation is a good match
        description: Description of the instance
        end_node_page_number: The number of the page on which this annotation is located in the endNode File if an
            endNode is present. The first page has number 1
        end_node_text: The text extracted from within the bounding box on the endNode. Only applicable if an endNode is
            defined
        end_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more
            than endNodeXMin. Only applicable if an endNode is defined
        end_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less
            than endNodeXMax. Only applicable if an endNode is defined
        end_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more
            than endNodeYMin. Only applicable if an endNode is defined
        end_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less
            than endNodeYMax. Only applicable if an endNode is defined
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        start_node_page_number: The number of the page on which this annotation is located in `startNode` File. The
            first page has number 1
        start_node_text: The text extracted from within the bounding box on the startNode
        start_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more
            than startNodeXMin
        start_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less
            than startNodeXMax
        start_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more
            than startNodeYMin
        start_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly
            less than startNodeYMax
        status: The status of the annotation
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1")
    end_node: Union[dm.NodeId, None] = Field(None, alias="endNode")
    aliases: Optional[list[str]] = None
    confidence: Optional[float] = None
    description: Optional[str] = None
    end_node_page_number: Optional[int] = Field(None, alias="endNodePageNumber")
    end_node_text: Optional[str] = Field(None, alias="endNodeText")
    end_node_x_max: Optional[float] = Field(None, alias="endNodeXMax")
    end_node_x_min: Optional[float] = Field(None, alias="endNodeXMin")
    end_node_y_max: Optional[float] = Field(None, alias="endNodeYMax")
    end_node_y_min: Optional[float] = Field(None, alias="endNodeYMin")
    name: Optional[str] = None
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    start_node_page_number: Optional[int] = Field(None, alias="startNodePageNumber")
    start_node_text: Optional[str] = Field(None, alias="startNodeText")
    start_node_x_max: Optional[float] = Field(None, alias="startNodeXMax")
    start_node_x_min: Optional[float] = Field(None, alias="startNodeXMin")
    start_node_y_max: Optional[float] = Field(None, alias="startNodeYMax")
    start_node_y_min: Optional[float] = Field(None, alias="startNodeYMin")
    status: Optional[Literal["Approved", "Rejected", "Suggested"]] = None
    tags: Optional[list[str]] = None

    def as_read(self) -> CogniteDiagramAnnotation:
        """Convert this GraphQL format of Cognite diagram annotation to the reading format."""
        return CogniteDiagramAnnotation.model_validate(as_read_args(self))

    def as_write(self) -> CogniteDiagramAnnotationWrite:
        """Convert this GraphQL format of Cognite diagram annotation to the writing format."""
        return CogniteDiagramAnnotationWrite.model_validate(as_write_args(self))


class CogniteDiagramAnnotation(CogniteAnnotation):
    """This represents the reading version of Cognite diagram annotation.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite diagram annotation.
        data_record: The data record of the Cognite diagram annotation edge.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        confidence: The confidence that the annotation is a good match
        description: Description of the instance
        end_node_page_number: The number of the page on which this annotation is located in the endNode File if an
            endNode is present. The first page has number 1
        end_node_text: The text extracted from within the bounding box on the endNode. Only applicable if an endNode is
            defined
        end_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more
            than endNodeXMin. Only applicable if an endNode is defined
        end_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less
            than endNodeXMax. Only applicable if an endNode is defined
        end_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more
            than endNodeYMin. Only applicable if an endNode is defined
        end_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less
            than endNodeYMax. Only applicable if an endNode is defined
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        start_node_page_number: The number of the page on which this annotation is located in `startNode` File. The
            first page has number 1
        start_node_text: The text extracted from within the bounding box on the startNode
        start_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more
            than startNodeXMin
        start_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less
            than startNodeXMax
        start_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more
            than startNodeYMin
        start_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly
            less than startNodeYMax
        status: The status of the annotation
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node_page_number: Optional[int] = Field(None, alias="endNodePageNumber")
    end_node_text: Optional[str] = Field(None, alias="endNodeText")
    end_node_x_max: Optional[float] = Field(None, alias="endNodeXMax")
    end_node_x_min: Optional[float] = Field(None, alias="endNodeXMin")
    end_node_y_max: Optional[float] = Field(None, alias="endNodeYMax")
    end_node_y_min: Optional[float] = Field(None, alias="endNodeYMin")
    start_node_page_number: Optional[int] = Field(None, alias="startNodePageNumber")
    start_node_text: Optional[str] = Field(None, alias="startNodeText")
    start_node_x_max: Optional[float] = Field(None, alias="startNodeXMax")
    start_node_x_min: Optional[float] = Field(None, alias="startNodeXMin")
    start_node_y_max: Optional[float] = Field(None, alias="startNodeYMax")
    start_node_y_min: Optional[float] = Field(None, alias="startNodeYMin")

    def as_write(self) -> CogniteDiagramAnnotationWrite:
        """Convert this read version of Cognite diagram annotation to the writing version."""
        return CogniteDiagramAnnotationWrite.model_validate(as_write_args(self))


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {}


def _validate_end_node(start_node: DomainModelWrite, end_node: Union[str, dm.NodeId]) -> None:
    if isinstance(end_node, str | dm.NodeId):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )


class CogniteDiagramAnnotationWrite(CogniteAnnotationWrite):
    """This represents the writing version of Cognite diagram annotation.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite diagram annotation.
        data_record: The data record of the Cognite diagram annotation edge.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        confidence: The confidence that the annotation is a good match
        description: Description of the instance
        end_node_page_number: The number of the page on which this annotation is located in the endNode File if an
            endNode is present. The first page has number 1
        end_node_text: The text extracted from within the bounding box on the endNode. Only applicable if an endNode is
            defined
        end_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more
            than endNodeXMin. Only applicable if an endNode is defined
        end_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less
            than endNodeXMax. Only applicable if an endNode is defined
        end_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more
            than endNodeYMin. Only applicable if an endNode is defined
        end_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less
            than endNodeYMax. Only applicable if an endNode is defined
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        start_node_page_number: The number of the page on which this annotation is located in `startNode` File. The
            first page has number 1
        start_node_text: The text extracted from within the bounding box on the startNode
        start_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more
            than startNodeXMin
        start_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less
            than startNodeXMax
        start_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more
            than startNodeYMin
        start_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly
            less than startNodeYMax
        status: The status of the annotation
        tags: Text based labels for generic use, limited to 1000
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "confidence",
        "description",
        "end_node_page_number",
        "end_node_text",
        "end_node_x_max",
        "end_node_x_min",
        "end_node_y_max",
        "end_node_y_min",
        "name",
        "source",
        "source_context",
        "source_created_time",
        "source_created_user",
        "source_id",
        "source_updated_time",
        "source_updated_user",
        "start_node_page_number",
        "start_node_text",
        "start_node_x_max",
        "start_node_x_min",
        "start_node_y_max",
        "start_node_y_min",
        "status",
        "tags",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("source",)
    _validate_end_node = _validate_end_node

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1")
    end_node_page_number: Optional[int] = Field(None, alias="endNodePageNumber")
    end_node_text: Optional[str] = Field(None, alias="endNodeText")
    end_node_x_max: Optional[float] = Field(None, alias="endNodeXMax")
    end_node_x_min: Optional[float] = Field(None, alias="endNodeXMin")
    end_node_y_max: Optional[float] = Field(None, alias="endNodeYMax")
    end_node_y_min: Optional[float] = Field(None, alias="endNodeYMin")
    start_node_page_number: Optional[int] = Field(None, alias="startNodePageNumber")
    start_node_text: Optional[str] = Field(None, alias="startNodeText")
    start_node_x_max: Optional[float] = Field(None, alias="startNodeXMax")
    start_node_x_min: Optional[float] = Field(None, alias="startNodeXMin")
    start_node_y_max: Optional[float] = Field(None, alias="startNodeYMax")
    start_node_y_min: Optional[float] = Field(None, alias="startNodeYMin")


class CogniteDiagramAnnotationList(DomainRelationList[CogniteDiagramAnnotation]):
    """List of Cognite diagram annotations in the reading version."""

    _INSTANCE = CogniteDiagramAnnotation

    def as_write(self) -> CogniteDiagramAnnotationWriteList:
        """Convert this read version of Cognite diagram annotation list to the writing version."""
        return CogniteDiagramAnnotationWriteList([edge.as_write() for edge in self])


class CogniteDiagramAnnotationWriteList(DomainRelationWriteList[CogniteDiagramAnnotationWrite]):
    """List of Cognite diagram annotations in the writing version."""

    _INSTANCE = CogniteDiagramAnnotationWrite


def _create_cognite_diagram_annotation_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    min_confidence: float | None = None,
    max_confidence: float | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    min_end_node_page_number: int | None = None,
    max_end_node_page_number: int | None = None,
    end_node_text: str | list[str] | None = None,
    end_node_text_prefix: str | None = None,
    min_end_node_x_max: float | None = None,
    max_end_node_x_max: float | None = None,
    min_end_node_x_min: float | None = None,
    max_end_node_x_min: float | None = None,
    min_end_node_y_max: float | None = None,
    max_end_node_y_max: float | None = None,
    min_end_node_y_min: float | None = None,
    max_end_node_y_min: float | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    source: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    source_context: str | list[str] | None = None,
    source_context_prefix: str | None = None,
    min_source_created_time: datetime.datetime | None = None,
    max_source_created_time: datetime.datetime | None = None,
    source_created_user: str | list[str] | None = None,
    source_created_user_prefix: str | None = None,
    source_id: str | list[str] | None = None,
    source_id_prefix: str | None = None,
    min_source_updated_time: datetime.datetime | None = None,
    max_source_updated_time: datetime.datetime | None = None,
    source_updated_user: str | list[str] | None = None,
    source_updated_user_prefix: str | None = None,
    min_start_node_page_number: int | None = None,
    max_start_node_page_number: int | None = None,
    start_node_text: str | list[str] | None = None,
    start_node_text_prefix: str | None = None,
    min_start_node_x_max: float | None = None,
    max_start_node_x_max: float | None = None,
    min_start_node_x_min: float | None = None,
    max_start_node_x_min: float | None = None,
    min_start_node_y_max: float | None = None,
    max_start_node_y_max: float | None = None,
    min_start_node_y_min: float | None = None,
    max_start_node_y_min: float | None = None,
    status: (
        Literal["Approved", "Rejected", "Suggested"] | list[Literal["Approved", "Rejected", "Suggested"]] | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
    if min_confidence is not None or max_confidence is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("confidence"), gte=min_confidence, lte=max_confidence))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if min_end_node_page_number is not None or max_end_node_page_number is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endNodePageNumber"), gte=min_end_node_page_number, lte=max_end_node_page_number
            )
        )
    if isinstance(end_node_text, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("endNodeText"), value=end_node_text))
    if end_node_text and isinstance(end_node_text, list):
        filters.append(dm.filters.In(view_id.as_property_ref("endNodeText"), values=end_node_text))
    if end_node_text_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("endNodeText"), value=end_node_text_prefix))
    if min_end_node_x_max is not None or max_end_node_x_max is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("endNodeXMax"), gte=min_end_node_x_max, lte=max_end_node_x_max)
        )
    if min_end_node_x_min is not None or max_end_node_x_min is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("endNodeXMin"), gte=min_end_node_x_min, lte=max_end_node_x_min)
        )
    if min_end_node_y_max is not None or max_end_node_y_max is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("endNodeYMax"), gte=min_end_node_y_max, lte=max_end_node_y_max)
        )
    if min_end_node_y_min is not None or max_end_node_y_min is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("endNodeYMin"), gte=min_end_node_y_min, lte=max_end_node_y_min)
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(source):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=as_instance_dict_id(source)))
    if source and isinstance(source, Sequence) and not isinstance(source, str) and not is_tuple_id(source):
        filters.append(
            dm.filters.In(view_id.as_property_ref("source"), values=[as_instance_dict_id(item) for item in source])
        )
    if isinstance(source_context, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceContext"), value=source_context))
    if source_context and isinstance(source_context, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceContext"), values=source_context))
    if source_context_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceContext"), value=source_context_prefix))
    if min_source_created_time is not None or max_source_created_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("sourceCreatedTime"),
                gte=min_source_created_time.isoformat(timespec="milliseconds") if min_source_created_time else None,
                lte=max_source_created_time.isoformat(timespec="milliseconds") if max_source_created_time else None,
            )
        )
    if isinstance(source_created_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceCreatedUser"), value=source_created_user))
    if source_created_user and isinstance(source_created_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceCreatedUser"), values=source_created_user))
    if source_created_user_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("sourceCreatedUser"), value=source_created_user_prefix)
        )
    if isinstance(source_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceId"), value=source_id))
    if source_id and isinstance(source_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceId"), values=source_id))
    if source_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceId"), value=source_id_prefix))
    if min_source_updated_time is not None or max_source_updated_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("sourceUpdatedTime"),
                gte=min_source_updated_time.isoformat(timespec="milliseconds") if min_source_updated_time else None,
                lte=max_source_updated_time.isoformat(timespec="milliseconds") if max_source_updated_time else None,
            )
        )
    if isinstance(source_updated_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceUpdatedUser"), value=source_updated_user))
    if source_updated_user and isinstance(source_updated_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceUpdatedUser"), values=source_updated_user))
    if source_updated_user_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("sourceUpdatedUser"), value=source_updated_user_prefix)
        )
    if min_start_node_page_number is not None or max_start_node_page_number is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startNodePageNumber"),
                gte=min_start_node_page_number,
                lte=max_start_node_page_number,
            )
        )
    if isinstance(start_node_text, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("startNodeText"), value=start_node_text))
    if start_node_text and isinstance(start_node_text, list):
        filters.append(dm.filters.In(view_id.as_property_ref("startNodeText"), values=start_node_text))
    if start_node_text_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("startNodeText"), value=start_node_text_prefix))
    if min_start_node_x_max is not None or max_start_node_x_max is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startNodeXMax"), gte=min_start_node_x_max, lte=max_start_node_x_max
            )
        )
    if min_start_node_x_min is not None or max_start_node_x_min is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startNodeXMin"), gte=min_start_node_x_min, lte=max_start_node_x_min
            )
        )
    if min_start_node_y_max is not None or max_start_node_y_max is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startNodeYMax"), gte=min_start_node_y_max, lte=max_start_node_y_max
            )
        )
    if min_start_node_y_min is not None or max_start_node_y_min is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startNodeYMin"), gte=min_start_node_y_min, lte=max_start_node_y_min
            )
        )
    if isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class _CogniteDiagramAnnotationQuery(EdgeQueryCore[T_DomainList, CogniteDiagramAnnotationList]):
    _view_id = CogniteDiagramAnnotation._view_id
    _result_cls = CogniteDiagramAnnotation
    _result_list_cls_end = CogniteDiagramAnnotationList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
    ):
        from ._cognite_source_system import _CogniteSourceSystemQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            None,
            connection_name,
            connection_property,
        )
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
                connection_property=ViewPropertyId(self._view_id, "end_node"),
            )

        if _CogniteSourceSystemQuery not in created_types:
            self.source = _CogniteSourceSystemQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,  # type: ignore[type-var]
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("source"),
                    direction="outwards",
                ),
                connection_name="source",
                connection_property=ViewPropertyId(self._view_id, "source"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.confidence = FloatFilter(self, self._view_id.as_property_ref("confidence"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.end_node_page_number = IntFilter(self, self._view_id.as_property_ref("endNodePageNumber"))
        self.end_node_text = StringFilter(self, self._view_id.as_property_ref("endNodeText"))
        self.end_node_x_max = FloatFilter(self, self._view_id.as_property_ref("endNodeXMax"))
        self.end_node_x_min = FloatFilter(self, self._view_id.as_property_ref("endNodeXMin"))
        self.end_node_y_max = FloatFilter(self, self._view_id.as_property_ref("endNodeYMax"))
        self.end_node_y_min = FloatFilter(self, self._view_id.as_property_ref("endNodeYMin"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source_filter = DirectRelationFilter(self, self._view_id.as_property_ref("source"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self.start_node_page_number = IntFilter(self, self._view_id.as_property_ref("startNodePageNumber"))
        self.start_node_text = StringFilter(self, self._view_id.as_property_ref("startNodeText"))
        self.start_node_x_max = FloatFilter(self, self._view_id.as_property_ref("startNodeXMax"))
        self.start_node_x_min = FloatFilter(self, self._view_id.as_property_ref("startNodeXMin"))
        self.start_node_y_max = FloatFilter(self, self._view_id.as_property_ref("startNodeYMax"))
        self.start_node_y_min = FloatFilter(self, self._view_id.as_property_ref("startNodeYMin"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.confidence,
                self.description,
                self.end_node_page_number,
                self.end_node_text,
                self.end_node_x_max,
                self.end_node_x_min,
                self.end_node_y_max,
                self.end_node_y_min,
                self.name,
                self.source_filter,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
                self.start_node_page_number,
                self.start_node_text,
                self.start_node_x_max,
                self.start_node_x_min,
                self.start_node_y_max,
                self.start_node_y_min,
            ]
        )
