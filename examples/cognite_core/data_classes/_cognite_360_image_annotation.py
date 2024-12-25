from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field

from cognite_core.data_classes._cognite_3_d_object import Cognite3DObjectWrite
from cognite_core.data_classes._cognite_360_image import Cognite360ImageGraphQL, Cognite360ImageWrite
from cognite_core.data_classes._cognite_annotation import CogniteAnnotation, CogniteAnnotationWrite
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainRelationList,
    DomainRelationWrite,
    DomainRelationWriteList,
    EdgeQueryCore,
    FloatFilter,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainList,
    TimestampFilter,
    as_instance_dict_id,
    is_tuple_id,
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_360_image import (
        Cognite360ImageGraphQL,
        Cognite360ImageWrite,
    )
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystemGraphQL,
    )


__all__ = [
    "Cognite360ImageAnnotation",
    "Cognite360ImageAnnotationWrite",
    "Cognite360ImageAnnotationApply",
    "Cognite360ImageAnnotationList",
    "Cognite360ImageAnnotationWriteList",
    "Cognite360ImageAnnotationApplyList",
    "Cognite360ImageAnnotationFields",
    "Cognite360ImageAnnotationTextFields",
]


Cognite360ImageAnnotationTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "format_version",
    "name",
    "source_context",
    "source_created_user",
    "source_id",
    "source_updated_user",
    "tags",
]
Cognite360ImageAnnotationFields = Literal[
    "external_id",
    "aliases",
    "confidence",
    "description",
    "format_version",
    "name",
    "polygon",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "status",
    "tags",
]
_COGNITE360IMAGEANNOTATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "confidence": "confidence",
    "description": "description",
    "format_version": "formatVersion",
    "name": "name",
    "polygon": "polygon",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "status": "status",
    "tags": "tags",
}


class Cognite360ImageAnnotationGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 360 image annotation, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image annotation.
        data_record: The data record of the Cognite 360 image annotation node.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        confidence: The confidence that the annotation is a good match
        description: Description of the instance
        format_version: Specifies the storage representation for the polygon
        name: Name of the instance
        polygon: List of floats representing the polygon. Format depends on formatVersion
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        status: The status of the annotation
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageAnnotation", "v1")
    end_node: Union[Cognite360ImageGraphQL, None] = None
    aliases: Optional[list[str]] = None
    confidence: Optional[float] = None
    description: Optional[str] = None
    format_version: Optional[str] = Field(None, alias="formatVersion")
    name: Optional[str] = None
    polygon: Optional[list[float]] = None
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    status: Optional[Literal["Approved", "Rejected", "Suggested"]] = None
    tags: Optional[list[str]] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite360ImageAnnotation:
        """Convert this GraphQL format of Cognite 360 image annotation to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite360ImageAnnotation(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            end_node=self.end_node.as_read() if isinstance(self.end_node, GraphQLCore) else self.end_node,
            aliases=self.aliases,
            confidence=self.confidence,
            description=self.description,
            format_version=self.format_version,
            name=self.name,
            polygon=self.polygon,
            source=self.source.as_read() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            status=self.status,
            tags=self.tags,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageAnnotationWrite:
        """Convert this GraphQL format of Cognite 360 image annotation to the writing format."""
        return Cognite360ImageAnnotationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            end_node=self.end_node.as_write() if isinstance(self.end_node, DomainModel) else self.end_node,
            aliases=self.aliases,
            confidence=self.confidence,
            description=self.description,
            format_version=self.format_version,
            name=self.name,
            polygon=self.polygon,
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            status=self.status,
            tags=self.tags,
        )


class Cognite360ImageAnnotation(CogniteAnnotation):
    """This represents the reading version of Cognite 360 image annotation.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image annotation.
        data_record: The data record of the Cognite 360 image annotation edge.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        confidence: The confidence that the annotation is a good match
        description: Description of the instance
        format_version: Specifies the storage representation for the polygon
        name: Name of the instance
        polygon: List of floats representing the polygon. Format depends on formatVersion
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        status: The status of the annotation
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageAnnotation", "v1")
    space: str = DEFAULT_INSTANCE_SPACE
    format_version: Optional[str] = Field(None, alias="formatVersion")
    polygon: Optional[list[float]] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageAnnotationWrite:
        """Convert this read version of Cognite 360 image annotation to the writing version."""
        return Cognite360ImageAnnotationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            end_node=self.end_node.as_write() if isinstance(self.end_node, DomainModel) else self.end_node,
            aliases=self.aliases,
            confidence=self.confidence,
            description=self.description,
            format_version=self.format_version,
            name=self.name,
            polygon=self.polygon,
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            status=self.status,
            tags=self.tags,
        )

    def as_apply(self) -> Cognite360ImageAnnotationWrite:
        """Convert this read version of Cognite 360 image annotation to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Cognite360ImageAnnotationWrite(CogniteAnnotationWrite):
    """This represents the writing version of Cognite 360 image annotation.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image annotation.
        data_record: The data record of the Cognite 360 image annotation edge.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        confidence: The confidence that the annotation is a good match
        description: Description of the instance
        format_version: Specifies the storage representation for the polygon
        name: Name of the instance
        polygon: List of floats representing the polygon. Format depends on formatVersion
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        status: The status of the annotation
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageAnnotation", "v1")
    space: str = DEFAULT_INSTANCE_SPACE
    format_version: Optional[str] = Field(None, alias="formatVersion")
    polygon: Optional[list[float]] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.external_id and (self.space, self.external_id) in cache:
            return resources

        _validate_end_node(start_node, self.end_node)

        if isinstance(self.end_node, DomainModelWrite):
            end_node = self.end_node.as_direct_reference()
        elif isinstance(self.end_node, str):
            end_node = dm.DirectRelationReference(self.space, self.end_node)
        elif isinstance(self.end_node, dm.NodeId):
            end_node = dm.DirectRelationReference(self.end_node.space, self.end_node.external_id)
        else:
            raise ValueError(f"Invalid type for equipment_module: {type(self.end_node)}")

        external_id = self.external_id or DomainRelationWrite.external_id_factory(start_node, self.end_node, edge_type)

        properties: dict[str, Any] = {}

        if self.aliases is not None or write_none:
            properties["aliases"] = self.aliases

        if self.confidence is not None or write_none:
            properties["confidence"] = self.confidence

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.format_version is not None or write_none:
            properties["formatVersion"] = self.format_version

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.polygon is not None or write_none:
            properties["polygon"] = self.polygon

        if self.source is not None:
            properties["source"] = {
                "space": self.space if isinstance(self.source, str) else self.source.space,
                "externalId": self.source if isinstance(self.source, str) else self.source.external_id,
            }

        if self.source_context is not None or write_none:
            properties["sourceContext"] = self.source_context

        if self.source_created_time is not None or write_none:
            properties["sourceCreatedTime"] = (
                self.source_created_time.isoformat(timespec="milliseconds") if self.source_created_time else None
            )

        if self.source_created_user is not None or write_none:
            properties["sourceCreatedUser"] = self.source_created_user

        if self.source_id is not None or write_none:
            properties["sourceId"] = self.source_id

        if self.source_updated_time is not None or write_none:
            properties["sourceUpdatedTime"] = (
                self.source_updated_time.isoformat(timespec="milliseconds") if self.source_updated_time else None
            )

        if self.source_updated_user is not None or write_none:
            properties["sourceUpdatedUser"] = self.source_updated_user

        if self.status is not None or write_none:
            properties["status"] = self.status

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if properties:
            this_edge = dm.EdgeApply(
                space=self.space,
                external_id=external_id,
                type=edge_type,
                start_node=start_node.as_direct_reference(),
                end_node=end_node,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.edges.append(this_edge)
            cache.add((self.space, external_id))

        if isinstance(self.end_node, DomainModelWrite):
            other_resources = self.end_node._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class Cognite360ImageAnnotationApply(Cognite360ImageAnnotationWrite):
    def __new__(cls, *args, **kwargs) -> Cognite360ImageAnnotationApply:
        warnings.warn(
            "Cognite360ImageAnnotationApply is deprecated and will be removed in v1.0. Use Cognite360ImageAnnotationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite360ImageAnnotation.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite360ImageAnnotationList(DomainRelationList[Cognite360ImageAnnotation]):
    """List of Cognite 360 image annotations in the reading version."""

    _INSTANCE = Cognite360ImageAnnotation

    def as_write(self) -> Cognite360ImageAnnotationWriteList:
        """Convert this read version of Cognite 360 image annotation list to the writing version."""
        return Cognite360ImageAnnotationWriteList([edge.as_write() for edge in self])

    def as_apply(self) -> Cognite360ImageAnnotationWriteList:
        """Convert these read versions of Cognite 360 image annotation list to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Cognite360ImageAnnotationWriteList(DomainRelationWriteList[Cognite360ImageAnnotationWrite]):
    """List of Cognite 360 image annotations in the writing version."""

    _INSTANCE = Cognite360ImageAnnotationWrite


class Cognite360ImageAnnotationApplyList(Cognite360ImageAnnotationWriteList): ...


def _create_cognite_360_image_annotation_filter(
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
    format_version: str | list[str] | None = None,
    format_version_prefix: str | None = None,
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
    if isinstance(format_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("formatVersion"), value=format_version))
    if format_version and isinstance(format_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("formatVersion"), values=format_version))
    if format_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("formatVersion"), value=format_version_prefix))
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {
    Cognite360ImageWrite: {Cognite3DObjectWrite},
}


def _validate_end_node(start_node: DomainModelWrite, end_node: Union[Cognite360ImageWrite, str, dm.NodeId]) -> None:
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


class _Cognite360ImageAnnotationQuery(EdgeQueryCore[T_DomainList, Cognite360ImageAnnotationList]):
    _view_id = Cognite360ImageAnnotation._view_id
    _result_cls = Cognite360ImageAnnotation
    _result_list_cls_end = Cognite360ImageAnnotationList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):
        from ._cognite_source_system import _CogniteSourceSystemQuery

        super().__init__(created_types, creation_path, client, result_list_cls, expression, None, connection_name)
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
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
                "source",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.confidence = FloatFilter(self, self._view_id.as_property_ref("confidence"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.format_version = StringFilter(self, self._view_id.as_property_ref("formatVersion"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.confidence,
                self.description,
                self.format_version,
                self.name,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
            ]
        )
