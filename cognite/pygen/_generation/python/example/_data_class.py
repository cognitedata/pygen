"""Data classes for the example SDK.

This module contains the data classes for the example data model with:
- ProductNode: Node view with various property types and a direct relation to CategoryNode
- CategoryNode: Node view with a reverse direct relation to ProductNode
- RelatesTo: Edge view for relating nodes
"""

from datetime import date, datetime
from typing import ClassVar, Literal

from pydantic import Field

from cognite.pygen._generation.python.instance_api.models._references import NodeReference, ViewReference
from cognite.pygen._generation.python.instance_api.models._types import Date, DateTime
from cognite.pygen._generation.python.instance_api.models.dtype_filters import (
    BooleanFilter,
    DateFilter,
    FilterContainer,
    FloatFilter,
    IntegerFilter,
    TextFilter,
)
from cognite.pygen._generation.python.instance_api.models.instance import (
    Instance,
    InstanceId,
    InstanceList,
    InstanceWrite,
)


class ProductNodeWrite(InstanceWrite):
    """Write class for ProductNode instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="pygen_example", external_id="ProductNodeWrite", version="v1"
    )
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    name: str
    description: str | None = None
    tags: list[str] | None = None
    price: float
    prices: list[float] | None = None
    quantity: int
    quantities: list[int] | None = None
    active: bool | None = None
    created_date: Date = Field(alias="createdDate")
    updated_timestamp: DateTime | None = Field(None, alias="updatedTimestamp")
    category: InstanceId | tuple[str, str] | None = None


class ProductNode(Instance):
    """Read class for ProductNode instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="ProductNode", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    name: str
    description: str | None = None
    tags: list[str] | None = None
    price: float
    prices: list[float] | None = None
    quantity: int
    quantities: list[int] | None = None
    active: bool | None = None
    created_date: date = Field(alias="createdDate")
    updated_timestamp: datetime | None = Field(None, alias="updatedTimestamp")
    category: InstanceId | None = None

    def as_write(self) -> ProductNodeWrite:
        """Convert to write representation."""
        return ProductNodeWrite.model_validate(self.model_dump(by_alias=True))


class ProductNodeList(InstanceList[ProductNode]):
    """List of ProductNode instances."""

    _INSTANCE: ClassVar[type[ProductNode]] = ProductNode


class ProductNodeFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = ProductNode._view_id
        self.name = TextFilter(view_id, "name", operator)
        self.description = TextFilter(view_id, "description", operator)
        self.price = FloatFilter(view_id, "price", operator)
        self.quantity = IntegerFilter(view_id, "quantity", operator)
        self.active = BooleanFilter(view_id, "active", operator)
        self.created_date = DateFilter(view_id, "createdDate", operator)
        super().__init__(
            data_type_filters=[
                self.name,
                self.description,
                self.price,
                self.quantity,
                self.active,
                self.created_date,
            ],
            operator=operator,
        )


class CategoryNodeWrite(InstanceWrite):
    """Write class for CategoryNode instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="pygen_example", external_id="CategoryNodeWrite", version="v1"
    )
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    category_name: str = Field(alias="categoryName")


class CategoryNode(Instance):
    """Read class for CategoryNode instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="CategoryNode", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    category_name: str = Field(alias="categoryName")
    products: list[InstanceId] | None = None

    def as_write(self) -> CategoryNodeWrite:
        """Convert to write representation."""
        return CategoryNodeWrite(
            space=self.space,
            external_id=self.external_id,
            category_name=self.category_name,
        )


class CategoryNodeList(InstanceList[CategoryNode]):
    """List of CategoryNode instances."""

    _INSTANCE: ClassVar[type[CategoryNode]] = CategoryNode


class RelatesToWrite(InstanceWrite):
    """Write class for RelatesTo edge instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="RelatesToWrite", version="v1")
    instance_type: Literal["edge"] = Field("edge", alias="instanceType")
    start_node: InstanceId | tuple[str, str] = Field(alias="startNode")
    end_node: InstanceId | tuple[str, str] = Field(alias="endNode")
    relation_type: str = Field(alias="relationType")
    strength: float | None = None
    created_at: DateTime = Field(alias="createdAt")


class RelatesTo(Instance):
    """Read class for RelatesTo edge instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="RelatesTo", version="v1")
    instance_type: Literal["edge"] = Field("edge", alias="instanceType")
    start_node: NodeReference = Field(alias="startNode")
    end_node: NodeReference = Field(alias="endNode")
    relation_type: str = Field(alias="relationType")
    strength: float | None = None
    created_at: datetime = Field(alias="createdAt")

    def as_write(self) -> RelatesToWrite:
        """Convert to write representation."""
        return RelatesToWrite(
            space=self.space,
            external_id=self.external_id,
            start_node=self.start_node,
            end_node=self.end_node,
            relation_type=self.relation_type,
            strength=self.strength,
            created_at=self.created_at,
        )


class RelatesToList(InstanceList[RelatesTo]):
    """List of RelatesTo edge instances."""

    _INSTANCE: ClassVar[type[RelatesTo]] = RelatesTo
