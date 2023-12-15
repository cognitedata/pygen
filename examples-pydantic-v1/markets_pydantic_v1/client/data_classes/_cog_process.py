from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._bid import Bid, BidApply
    from ._date_transformation_pair import DateTransformationPair, DateTransformationPairApply
    from ._value_transformation import ValueTransformation, ValueTransformationApply


__all__ = [
    "CogProcess",
    "CogProcessApply",
    "CogProcessList",
    "CogProcessApplyList",
    "CogProcessFields",
    "CogProcessTextFields",
]


CogProcessTextFields = Literal["name"]
CogProcessFields = Literal["name"]

_COGPROCESS_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class CogProcess(DomainModel):
    """This represents the reading version of cog proces.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog proces.
        bid: The bid field.
        date_transformations: The date transformation field.
        name: The name field.
        transformation: The transformation field.
        created_time: The created time of the cog proces node.
        last_updated_time: The last updated time of the cog proces node.
        deleted_time: If present, the deleted time of the cog proces node.
        version: The version of the cog proces node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bid: Union[Bid, str, dm.NodeId, None] = Field(None, repr=False)
    date_transformations: Union[DateTransformationPair, str, dm.NodeId, None] = Field(
        None, repr=False, alias="dateTransformations"
    )
    name: Optional[str] = None
    transformation: Union[ValueTransformation, str, dm.NodeId, None] = Field(None, repr=False)

    def as_apply(self) -> CogProcessApply:
        """Convert this read version of cog proces to the writing version."""
        return CogProcessApply(
            space=self.space,
            external_id=self.external_id,
            bid=self.bid.as_apply() if isinstance(self.bid, DomainModel) else self.bid,
            date_transformations=self.date_transformations.as_apply()
            if isinstance(self.date_transformations, DomainModel)
            else self.date_transformations,
            name=self.name,
            transformation=self.transformation.as_apply()
            if isinstance(self.transformation, DomainModel)
            else self.transformation,
        )


class CogProcessApply(DomainModelApply):
    """This represents the writing version of cog proces.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog proces.
        bid: The bid field.
        date_transformations: The date transformation field.
        name: The name field.
        transformation: The transformation field.
        existing_version: Fail the ingestion request if the cog proces version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bid: Union[BidApply, str, dm.NodeId, None] = Field(None, repr=False)
    date_transformations: Union[DateTransformationPairApply, str, dm.NodeId, None] = Field(
        None, repr=False, alias="dateTransformations"
    )
    name: Optional[str] = None
    transformation: Union[ValueTransformationApply, str, dm.NodeId, None] = Field(None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "CogProcess", "b5df5d19e08fd0"
        )

        properties = {}
        if self.bid is not None:
            properties["bid"] = {
                "space": self.space if isinstance(self.bid, str) else self.bid.space,
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.date_transformations is not None:
            properties["dateTransformations"] = {
                "space": self.space if isinstance(self.date_transformations, str) else self.date_transformations.space,
                "externalId": self.date_transformations
                if isinstance(self.date_transformations, str)
                else self.date_transformations.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if self.transformation is not None:
            properties["transformation"] = {
                "space": self.space if isinstance(self.transformation, str) else self.transformation.space,
                "externalId": self.transformation
                if isinstance(self.transformation, str)
                else self.transformation.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.bid, DomainModelApply):
            other_resources = self.bid._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.date_transformations, DomainModelApply):
            other_resources = self.date_transformations._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.transformation, DomainModelApply):
            other_resources = self.transformation._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class CogProcessList(DomainModelList[CogProcess]):
    """List of cog process in the read version."""

    _INSTANCE = CogProcess

    def as_apply(self) -> CogProcessApplyList:
        """Convert these read versions of cog proces to the writing versions."""
        return CogProcessApplyList([node.as_apply() for node in self.data])


class CogProcessApplyList(DomainModelApplyList[CogProcessApply]):
    """List of cog process in the writing version."""

    _INSTANCE = CogProcessApply


def _create_cog_proces_filter(
    view_id: dm.ViewId,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if bid and isinstance(bid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": "market", "externalId": bid}))
    if bid and isinstance(bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": bid[0], "externalId": bid[1]}))
    if bid and isinstance(bid, list) and isinstance(bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": "market", "externalId": item} for item in bid]
            )
        )
    if bid and isinstance(bid, list) and isinstance(bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": item[0], "externalId": item[1]} for item in bid]
            )
        )
    if date_transformations and isinstance(date_transformations, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("dateTransformations"),
                value={"space": "market", "externalId": date_transformations},
            )
        )
    if date_transformations and isinstance(date_transformations, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("dateTransformations"),
                value={"space": date_transformations[0], "externalId": date_transformations[1]},
            )
        )
    if date_transformations and isinstance(date_transformations, list) and isinstance(date_transformations[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("dateTransformations"),
                values=[{"space": "market", "externalId": item} for item in date_transformations],
            )
        )
    if date_transformations and isinstance(date_transformations, list) and isinstance(date_transformations[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("dateTransformations"),
                values=[{"space": item[0], "externalId": item[1]} for item in date_transformations],
            )
        )
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if transformation and isinstance(transformation, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("transformation"), value={"space": "market", "externalId": transformation}
            )
        )
    if transformation and isinstance(transformation, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("transformation"),
                value={"space": transformation[0], "externalId": transformation[1]},
            )
        )
    if transformation and isinstance(transformation, list) and isinstance(transformation[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("transformation"),
                values=[{"space": "market", "externalId": item} for item in transformation],
            )
        )
    if transformation and isinstance(transformation, list) and isinstance(transformation[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("transformation"),
                values=[{"space": item[0], "externalId": item[1]} for item in transformation],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
