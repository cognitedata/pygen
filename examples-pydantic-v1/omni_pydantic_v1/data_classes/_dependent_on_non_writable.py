from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._implementation_1_non_writeable import Implementation1NonWriteable


__all__ = [
    "DependentOnNonWritable",
    "DependentOnNonWritableApply",
    "DependentOnNonWritableList",
    "DependentOnNonWritableApplyList",
    "DependentOnNonWritableFields",
    "DependentOnNonWritableTextFields",
]


DependentOnNonWritableTextFields = Literal["a_value"]
DependentOnNonWritableFields = Literal["a_value"]

_DEPENDENTONNONWRITABLE_PROPERTIES_BY_FIELD = {
    "a_value": "aValue",
}


class DependentOnNonWritable(DomainModel):
    """This represents the reading version of dependent on non writable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        a_value: The a value field.
        to_non_writable: The to non writable field.
        created_time: The created time of the dependent on non writable node.
        last_updated_time: The last updated time of the dependent on non writable node.
        deleted_time: If present, the deleted time of the dependent on non writable node.
        version: The version of the dependent on non writable node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "pygen-models", "DependentOnNonWritable"
    )
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Union[list[Implementation1NonWriteable], list[str], None] = Field(
        default=None, repr=False, alias="toNonWritable"
    )

    def as_apply(self) -> DependentOnNonWritableApply:
        """Convert this read version of dependent on non writable to the writing version."""
        return DependentOnNonWritableApply(
            space=self.space,
            external_id=self.external_id,
            a_value=self.a_value,
            to_non_writable=[
                to_non_writable.as_apply() if isinstance(to_non_writable, DomainModel) else to_non_writable
                for to_non_writable in self.to_non_writable or []
            ],
        )


class DependentOnNonWritableApply(DomainModelApply):
    """This represents the writing version of dependent on non writable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        a_value: The a value field.
        to_non_writable: The to non writable field.
        existing_version: Fail the ingestion request if the dependent on non writable version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "pygen-models", "DependentOnNonWritable"
    )
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Union[list[str], None] = Field(default=None, repr=False, alias="toNonWritable")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            DependentOnNonWritable, dm.ViewId("pygen-models", "DependentOnNonWritable", "1")
        )

        properties = {}

        if self.a_value is not None:
            properties["aValue"] = self.a_value

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("pygen-models", "toNonWritable")
        for to_non_writable in self.to_non_writable or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=to_non_writable,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        return resources


class DependentOnNonWritableList(DomainModelList[DependentOnNonWritable]):
    """List of dependent on non writables in the read version."""

    _INSTANCE = DependentOnNonWritable

    def as_apply(self) -> DependentOnNonWritableApplyList:
        """Convert these read versions of dependent on non writable to the writing versions."""
        return DependentOnNonWritableApplyList([node.as_apply() for node in self.data])


class DependentOnNonWritableApplyList(DomainModelApplyList[DependentOnNonWritableApply]):
    """List of dependent on non writables in the writing version."""

    _INSTANCE = DependentOnNonWritableApply


def _create_dependent_on_non_writable_filter(
    view_id: dm.ViewId,
    a_value: str | list[str] | None = None,
    a_value_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if a_value is not None and isinstance(a_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aValue"), value=a_value))
    if a_value and isinstance(a_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aValue"), values=a_value))
    if a_value_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aValue"), value=a_value_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
