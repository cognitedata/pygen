from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "ValueTransformation",
    "ValueTransformationApply",
    "ValueTransformationList",
    "ValueTransformationApplyList",
    "ValueTransformationFields",
    "ValueTransformationTextFields",
]


ValueTransformationTextFields = Literal["method"]
ValueTransformationFields = Literal["arguments", "method"]

_VALUETRANSFORMATION_PROPERTIES_BY_FIELD = {
    "arguments": "arguments",
    "method": "method",
}


class ValueTransformation(DomainModel):
    """This represent a read version of value transformation.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the value transformation.
        arguments: The argument field.
        method: The method field.
        created_time: The created time of the value transformation node.
        last_updated_time: The last updated time of the value transformation node.
        deleted_time: If present, the deleted time of the value transformation node.
        version: The version of the value transformation node.
    """

    space: str = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def as_apply(self) -> ValueTransformationApply:
        """Convert this read version of value transformation to a write version."""
        return ValueTransformationApply(
            space=self.space,
            external_id=self.external_id,
            arguments=self.arguments,
            method=self.method,
        )


class ValueTransformationApply(DomainModelApply):
    """This represent a write version of value transformation.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the value transformation.
        arguments: The argument field.
        method: The method field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if self.method is not None:
            properties["method"] = self.method
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "ValueTransformation", "147ebcf1583165"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ValueTransformationList(TypeList[ValueTransformation]):
    """List of value transformations in read version."""

    _NODE = ValueTransformation

    def as_apply(self) -> ValueTransformationApplyList:
        """Convert this read version of value transformation to a write version."""
        return ValueTransformationApplyList([node.as_apply() for node in self.data])


class ValueTransformationApplyList(TypeApplyList[ValueTransformationApply]):
    """List of value transformations in write version."""

    _NODE = ValueTransformationApply
