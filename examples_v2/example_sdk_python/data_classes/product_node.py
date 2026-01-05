from typing import ClassVar, Literal

from pydantic import Field

from cognite.pygen._python.instance_api.models._references import ViewReference
from cognite.pygen._python.instance_api.models._types import Date, DateTime
from cognite.pygen._python.instance_api.models.dtype_filters import (
    BooleanFilter,
    DateFilter,
    DateTimeFilter,
    DirectRelationFilter,
    FilterContainer,
    FloatFilter,
    IntegerFilter,
    TextFilter,
)
from cognite.pygen._python.instance_api.models.instance import (
    Instance,
    InstanceId,
    InstanceList,
    InstanceWrite,
)


class ProductNodeWrite(InstanceWrite):
    """Write class for Product Node instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="ProductNode", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    name: str
    description: str
    tags: list[str]
    price: float
    prices: list[float]
    quantity: int
    quantities: list[int]
    active: bool
    created_date: Date
    updated_timestamp: DateTime
    category: InstanceId


class ProductNode(Instance):
    """Read class for Product Node instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="ProductNode", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    name: str
    description: str
    tags: list[str]
    price: float
    prices: list[float]
    quantity: int
    quantities: list[int]
    active: bool
    created_date: Date
    updated_timestamp: DateTime
    category: InstanceId

    def as_write(self) -> ProductNodeWrite:
        """Convert to write representation."""
        return ProductNodeWrite.model_validate(self.model_dump(by_alias=True))


class ProductNodeList(InstanceList[ProductNode]):
    """List of Product Node instances."""

    _INSTANCE: ClassVar[type[ProductNode]] = ProductNode


class ProductNodeFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = ProductNode._view_id
        self.name = TextFilter(view_id, "name", operator)
        self.description = TextFilter(view_id, "description", operator)
        self.price = FloatFilter(view_id, "price", operator)
        self.quantity = IntegerFilter(view_id, "quantity", operator)
        self.active = BooleanFilter(view_id, "active", operator)
        self.created_date = DateFilter(view_id, "created_date", operator)
        self.updated_timestamp = DateTimeFilter(view_id, "updated_timestamp", operator)
        self.category = DirectRelationFilter(view_id, "category", operator)
        super().__init__(
            data_type_filters=[
                self.name,
                self.description,
                self.price,
                self.quantity,
                self.active,
                self.created_date,
                self.updated_timestamp,
                self.category,
            ],
            operator=operator,
            instance_type="node",
        )
