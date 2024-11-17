from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    BooleanFilter,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._cognite_3_d_revision import Cognite3DRevision, Cognite3DRevisionWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_360_image_model import (
        Cognite360ImageModel,
        Cognite360ImageModelList,
        Cognite360ImageModelGraphQL,
        Cognite360ImageModelWrite,
        Cognite360ImageModelWriteList,
    )


__all__ = [
    "Cognite360ImageCollection",
    "Cognite360ImageCollectionWrite",
    "Cognite360ImageCollectionApply",
    "Cognite360ImageCollectionList",
    "Cognite360ImageCollectionWriteList",
    "Cognite360ImageCollectionApplyList",
    "Cognite360ImageCollectionFields",
    "Cognite360ImageCollectionTextFields",
    "Cognite360ImageCollectionGraphQL",
]


Cognite360ImageCollectionTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
Cognite360ImageCollectionFields = Literal[
    "external_id", "aliases", "description", "name", "published", "status", "tags", "type_"
]

_COGNITE360IMAGECOLLECTION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "published": "published",
    "status": "status",
    "tags": "tags",
    "type_": "type",
}


class Cognite360ImageCollectionGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of Cognite 360 image collection, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image collection.
        data_record: The data record of the Cognite 360 image collection node.
        aliases: Alternative names for the node
        description: Description of the instance
        model_3d: The model 3d field.
        name: Name of the instance
        published: The published field.
        status: The status field.
        tags: Text based labels for generic use, limited to 1000
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageCollection", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    model_3d: Optional[Cognite360ImageModelGraphQL] = Field(default=None, repr=False, alias="model3D")
    name: Optional[str] = None
    published: Optional[bool] = None
    status: Optional[Literal["Done", "Failed", "Processing", "Queued"]] = None
    tags: Optional[list[str]] = None
    type_: Optional[Literal["CAD", "Image360", "PointCloud"]] = Field(None, alias="type")

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator("model_3d", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite360ImageCollection:
        """Convert this GraphQL format of Cognite 360 image collection to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite360ImageCollection(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            description=self.description,
            model_3d=self.model_3d.as_read() if isinstance(self.model_3d, GraphQLCore) else self.model_3d,
            name=self.name,
            published=self.published,
            status=self.status,
            tags=self.tags,
            type_=self.type_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageCollectionWrite:
        """Convert this GraphQL format of Cognite 360 image collection to the writing format."""
        return Cognite360ImageCollectionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            model_3d=self.model_3d.as_write() if isinstance(self.model_3d, GraphQLCore) else self.model_3d,
            name=self.name,
            published=self.published,
            status=self.status,
            tags=self.tags,
            type_=self.type_,
        )


class Cognite360ImageCollection(CogniteDescribableNode, Cognite3DRevision, protected_namespaces=()):
    """This represents the reading version of Cognite 360 image collection.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image collection.
        data_record: The data record of the Cognite 360 image collection node.
        aliases: Alternative names for the node
        description: Description of the instance
        model_3d: The model 3d field.
        name: Name of the instance
        published: The published field.
        status: The status field.
        tags: Text based labels for generic use, limited to 1000
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageCollection", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None

    def as_write(self) -> Cognite360ImageCollectionWrite:
        """Convert this read version of Cognite 360 image collection to the writing version."""
        return Cognite360ImageCollectionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            description=self.description,
            model_3d=self.model_3d.as_write() if isinstance(self.model_3d, DomainModel) else self.model_3d,
            name=self.name,
            published=self.published,
            status=self.status,
            tags=self.tags,
            type_=self.type_,
        )

    def as_apply(self) -> Cognite360ImageCollectionWrite:
        """Convert this read version of Cognite 360 image collection to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Cognite360ImageCollection],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_360_image_model import Cognite360ImageModel

        for instance in instances.values():
            if (
                isinstance(instance.model_3d, (dm.NodeId, str))
                and (model_3d := nodes_by_id.get(instance.model_3d))
                and isinstance(model_3d, Cognite360ImageModel)
            ):
                instance.model_3d = model_3d


class Cognite360ImageCollectionWrite(CogniteDescribableNodeWrite, Cognite3DRevisionWrite, protected_namespaces=()):
    """This represents the writing version of Cognite 360 image collection.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image collection.
        data_record: The data record of the Cognite 360 image collection node.
        aliases: Alternative names for the node
        description: Description of the instance
        model_3d: The model 3d field.
        name: Name of the instance
        published: The published field.
        status: The status field.
        tags: Text based labels for generic use, limited to 1000
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageCollection", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.aliases is not None or write_none:
            properties["aliases"] = self.aliases

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.model_3d is not None:
            properties["model3D"] = {
                "space": self.space if isinstance(self.model_3d, str) else self.model_3d.space,
                "externalId": self.model_3d if isinstance(self.model_3d, str) else self.model_3d.external_id,
            }

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.published is not None or write_none:
            properties["published"] = self.published

        if self.status is not None or write_none:
            properties["status"] = self.status

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if self.type_ is not None or write_none:
            properties["type"] = self.type_

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.model_3d, DomainModelWrite):
            other_resources = self.model_3d._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class Cognite360ImageCollectionApply(Cognite360ImageCollectionWrite):
    def __new__(cls, *args, **kwargs) -> Cognite360ImageCollectionApply:
        warnings.warn(
            "Cognite360ImageCollectionApply is deprecated and will be removed in v1.0. Use Cognite360ImageCollectionWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite360ImageCollection.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite360ImageCollectionList(DomainModelList[Cognite360ImageCollection]):
    """List of Cognite 360 image collections in the read version."""

    _INSTANCE = Cognite360ImageCollection

    def as_write(self) -> Cognite360ImageCollectionWriteList:
        """Convert these read versions of Cognite 360 image collection to the writing versions."""
        return Cognite360ImageCollectionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Cognite360ImageCollectionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def model_3d(self) -> Cognite360ImageModelList:
        from ._cognite_360_image_model import Cognite360ImageModel, Cognite360ImageModelList

        return Cognite360ImageModelList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, Cognite360ImageModel)]
        )


class Cognite360ImageCollectionWriteList(DomainModelWriteList[Cognite360ImageCollectionWrite]):
    """List of Cognite 360 image collections in the writing version."""

    _INSTANCE = Cognite360ImageCollectionWrite

    @property
    def model_3d(self) -> Cognite360ImageModelWriteList:
        from ._cognite_360_image_model import Cognite360ImageModelWrite, Cognite360ImageModelWriteList

        return Cognite360ImageModelWriteList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, Cognite360ImageModelWrite)]
        )


class Cognite360ImageCollectionApplyList(Cognite360ImageCollectionWriteList): ...


def _create_cognite_360_image_collection_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    model_3d: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    published: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(model_3d, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(model_3d):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model3D"), value=as_instance_dict_id(model_3d)))
    if model_3d and isinstance(model_3d, Sequence) and not isinstance(model_3d, str) and not is_tuple_id(model_3d):
        filters.append(
            dm.filters.In(view_id.as_property_ref("model3D"), values=[as_instance_dict_id(item) for item in model_3d])
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(published, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("published"), value=published))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Cognite360ImageCollectionQuery(NodeQueryCore[T_DomainModelList, Cognite360ImageCollectionList]):
    _view_id = Cognite360ImageCollection._view_id
    _result_cls = Cognite360ImageCollection
    _result_list_cls_end = Cognite360ImageCollectionList

    def __init__(
        self,
        created_triples: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._cognite_360_image_model import _Cognite360ImageModelQuery

        super().__init__(
            created_triples,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _Cognite360ImageModelQuery not in created_triples:
            self.model_3d = _Cognite360ImageModelQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("model3D"),
                    direction="outwards",
                ),
                connection_name="model_3d",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.published = BooleanFilter(self, self._view_id.as_property_ref("published"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
                self.published,
            ]
        )

    def list_cognite_360_image_collection(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite360ImageCollectionList:
        return self._list(limit=limit)


class Cognite360ImageCollectionQuery(_Cognite360ImageCollectionQuery[Cognite360ImageCollectionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite360ImageCollectionList)
