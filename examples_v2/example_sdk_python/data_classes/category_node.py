from typing import ClassVar, Literal

from cognite.pygen._python.instance_api.models._references import ViewReference
from cognite.pygen._python.instance_api.models.dtype_filters import (
    FilterContainer,
    TextFilter,
)
from cognite.pygen._python.instance_api.models.instance import (
    Instance,
    InstanceList,
    InstanceWrite,
)


class CategoryNodeWrite(InstanceWrite):
    """Write class for Category Node instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="CategoryNode", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    category_name: str


class CategoryNode(Instance):
    """Read class for Category Node instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="CategoryNode", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    category_name: str

    def as_write(self) -> CategoryNodeWrite:
        """Convert to write representation."""
        return CategoryNodeWrite.model_validate(self.model_dump(by_alias=True))


class CategoryNodeList(InstanceList[CategoryNode]):
    """List of Category Node instances."""

    _INSTANCE: ClassVar[type[CategoryNode]] = CategoryNode


class CategoryNodeFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = CategoryNode._view_id
        self.category_name = TextFilter(view_id, "category_name", operator)
        super().__init__(
            data_type_filters=[
                self.category_name,
            ],
            operator=operator,
            instance_type="node",
        )
