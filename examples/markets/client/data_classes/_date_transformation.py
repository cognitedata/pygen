from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "DateTransformation",
    "DateTransformationApply",
    "DateTransformationList",
    "DateTransformationApplyList",
    "DateTransformationFields",
    "DateTransformationTextFields",
]


DateTransformationTextFields = Literal["method"]
DateTransformationFields = Literal["arguments", "method"]

_DATETRANSFORMATION_PROPERTIES_BY_FIELD = {
    "arguments": "arguments",
    "method": "method",
}


class DateTransformation(DomainModel):
    """This represents the reading version of date transformation.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date transformation.
        arguments: The argument field.
        method: The method field.
        created_time: The created time of the date transformation node.
        last_updated_time: The last updated time of the date transformation node.
        deleted_time: If present, the deleted time of the date transformation node.
        version: The version of the date transformation node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def as_apply(self) -> DateTransformationApply:
        """Convert this read version of date transformation to the writing version."""
        return DateTransformationApply(
            space=self.space,
            external_id=self.external_id,
            arguments=self.arguments,
            method=self.method,
        )


class DateTransformationApply(DomainModelApply):
    """This represents the writing version of date transformation.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date transformation.
        arguments: The argument field.
        method: The method field.
        existing_version: Fail the ingestion request if the date transformation version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "DateTransformation", "482866112eb911"
        )

        properties = {}
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if self.method is not None:
            properties["method"] = self.method

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

        return resources


class DateTransformationList(DomainModelList[DateTransformation]):
    """List of date transformations in the read version."""

    _INSTANCE = DateTransformation

    def as_apply(self) -> DateTransformationApplyList:
        """Convert these read versions of date transformation to the writing versions."""
        return DateTransformationApplyList([node.as_apply() for node in self.data])


class DateTransformationApplyList(DomainModelApplyList[DateTransformationApply]):
    """List of date transformations in the writing version."""

    _INSTANCE = DateTransformationApply


def _create_date_transformation_filter(
    view_id: dm.ViewId,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
