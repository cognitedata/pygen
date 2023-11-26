from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union  # noqa: F401

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._date_transformation import DateTransformation, DateTransformationApply


__all__ = [
    "DateTransformationPair",
    "DateTransformationPairApply",
    "DateTransformationPairList",
    "DateTransformationPairApplyList",
]


class DateTransformationPair(DomainModel):
    """This represents the reading version of date transformation pair.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date transformation pair.
        end: The end field.
        start: The start field.
        created_time: The created time of the date transformation pair node.
        last_updated_time: The last updated time of the date transformation pair node.
        deleted_time: If present, the deleted time of the date transformation pair node.
        version: The version of the date transformation pair node.
    """

    space: str = "market"
    end: Union[list[DateTransformation], list[str], None] = Field(default=None, repr=False)
    start: Union[list[DateTransformation], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> DateTransformationPairApply:
        """Convert this read version of date transformation pair to the writing version."""
        return DateTransformationPairApply(
            space=self.space,
            external_id=self.external_id,
            end=[end.as_apply() if isinstance(end, DomainModel) else end for end in self.end or []],
            start=[start.as_apply() if isinstance(start, DomainModel) else start for start in self.start or []],
        )


class DateTransformationPairApply(DomainModelApply):
    """This represents the writing version of date transformation pair.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date transformation pair.
        end: The end field.
        start: The start field.
        existing_version: Fail the ingestion request if the date transformation pair version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    end: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)
    start: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "DateTransformationPair", "bde9fd4428c26e"
        )

        edge_type = dm.DirectRelationReference("market", "DateTransformationPair.end")
        for end in self.end or []:
            other_resources = DomainRelationApply._from_edge_to_resources(
                cache, self, end, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("market", "DateTransformationPair.start")
        for start in self.start or []:
            other_resources = DomainRelationApply._from_edge_to_resources(
                cache, self, start, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        return resources


class DateTransformationPairList(DomainModelList[DateTransformationPair]):
    """List of date transformation pairs in the read version."""

    _INSTANCE = DateTransformationPair

    def as_apply(self) -> DateTransformationPairApplyList:
        """Convert these read versions of date transformation pair to the writing versions."""
        return DateTransformationPairApplyList([node.as_apply() for node in self.data])


class DateTransformationPairApplyList(DomainModelApplyList[DateTransformationPairApply]):
    """List of date transformation pairs in the writing version."""

    _INSTANCE = DateTransformationPairApply


def _create_date_transformation_pair_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
