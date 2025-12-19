from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite_core.config import global_config
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite


__all__ = [
    "CogniteFileCategory",
    "CogniteFileCategoryWrite",
    "CogniteFileCategoryList",
    "CogniteFileCategoryWriteList",
    "CogniteFileCategoryFields",
    "CogniteFileCategoryTextFields",
    "CogniteFileCategoryGraphQL",
]


CogniteFileCategoryTextFields = Literal[
    "external_id", "aliases", "code", "description", "name", "standard", "standard_reference", "tags"
]
CogniteFileCategoryFields = Literal[
    "external_id", "aliases", "code", "description", "name", "standard", "standard_reference", "tags"
]

_COGNITEFILECATEGORY_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "code": "code",
    "description": "description",
    "name": "name",
    "standard": "standard",
    "standard_reference": "standardReference",
    "tags": "tags",
}


class CogniteFileCategoryGraphQL(GraphQLCore):
    """This represents the reading version of Cognite file category, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file category.
        data_record: The data record of the Cognite file category node.
        aliases: Alternative names for the node
        code: An identifier for the category, for example, 'AA' for Accounting (from Norsok.)
        description: Description of the instance
        name: Name of the instance
        standard: The name of the standard the category originates from, for example, 'Norsok'.
        standard_reference: A reference to the source of the category standard.
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFileCategory", "v1")
    aliases: Optional[list[str]] = None
    code: Optional[str] = None
    description: Optional[str] = None
    name: Optional[str] = None
    standard: Optional[str] = None
    standard_reference: Optional[str] = Field(None, alias="standardReference")
    tags: Optional[list[str]] = None

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

    def as_read(self) -> CogniteFileCategory:
        """Convert this GraphQL format of Cognite file category to the reading format."""
        return CogniteFileCategory.model_validate(as_read_args(self))

    def as_write(self) -> CogniteFileCategoryWrite:
        """Convert this GraphQL format of Cognite file category to the writing format."""
        return CogniteFileCategoryWrite.model_validate(as_write_args(self))


class CogniteFileCategory(CogniteDescribableNode):
    """This represents the reading version of Cognite file category.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file category.
        data_record: The data record of the Cognite file category node.
        aliases: Alternative names for the node
        code: An identifier for the category, for example, 'AA' for Accounting (from Norsok.)
        description: Description of the instance
        name: Name of the instance
        standard: The name of the standard the category originates from, for example, 'Norsok'.
        standard_reference: A reference to the source of the category standard.
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFileCategory", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    code: str
    standard: Optional[str] = None
    standard_reference: Optional[str] = Field(None, alias="standardReference")

    def as_write(self) -> CogniteFileCategoryWrite:
        """Convert this read version of Cognite file category to the writing version."""
        return CogniteFileCategoryWrite.model_validate(as_write_args(self))


class CogniteFileCategoryWrite(CogniteDescribableNodeWrite):
    """This represents the writing version of Cognite file category.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file category.
        data_record: The data record of the Cognite file category node.
        aliases: Alternative names for the node
        code: An identifier for the category, for example, 'AA' for Accounting (from Norsok.)
        description: Description of the instance
        name: Name of the instance
        standard: The name of the standard the category originates from, for example, 'Norsok'.
        standard_reference: A reference to the source of the category standard.
        tags: Text based labels for generic use, limited to 1000
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "code",
        "description",
        "name",
        "standard",
        "standard_reference",
        "tags",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFileCategory", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    code: str
    standard: Optional[str] = None
    standard_reference: Optional[str] = Field(None, alias="standardReference")


class CogniteFileCategoryList(DomainModelList[CogniteFileCategory]):
    """List of Cognite file categories in the read version."""

    _INSTANCE = CogniteFileCategory

    def as_write(self) -> CogniteFileCategoryWriteList:
        """Convert these read versions of Cognite file category to the writing versions."""
        return CogniteFileCategoryWriteList([node.as_write() for node in self.data])


class CogniteFileCategoryWriteList(DomainModelWriteList[CogniteFileCategoryWrite]):
    """List of Cognite file categories in the writing version."""

    _INSTANCE = CogniteFileCategoryWrite


def _create_cognite_file_category_filter(
    view_id: dm.ViewId,
    code: str | list[str] | None = None,
    code_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    standard: str | list[str] | None = None,
    standard_prefix: str | None = None,
    standard_reference: str | list[str] | None = None,
    standard_reference_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(code, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("code"), value=code))
    if code and isinstance(code, list):
        filters.append(dm.filters.In(view_id.as_property_ref("code"), values=code))
    if code_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("code"), value=code_prefix))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(standard, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("standard"), value=standard))
    if standard and isinstance(standard, list):
        filters.append(dm.filters.In(view_id.as_property_ref("standard"), values=standard))
    if standard_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("standard"), value=standard_prefix))
    if isinstance(standard_reference, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("standardReference"), value=standard_reference))
    if standard_reference and isinstance(standard_reference, list):
        filters.append(dm.filters.In(view_id.as_property_ref("standardReference"), values=standard_reference))
    if standard_reference_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("standardReference"), value=standard_reference_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteFileCategoryQuery(NodeQueryCore[T_DomainModelList, CogniteFileCategoryList]):
    _view_id = CogniteFileCategory._view_id
    _result_cls = CogniteFileCategory
    _result_list_cls_end = CogniteFileCategoryList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.code = StringFilter(self, self._view_id.as_property_ref("code"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.standard = StringFilter(self, self._view_id.as_property_ref("standard"))
        self.standard_reference = StringFilter(self, self._view_id.as_property_ref("standardReference"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.code,
                self.description,
                self.name,
                self.standard,
                self.standard_reference,
            ]
        )

    def list_cognite_file_category(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteFileCategoryList:
        return self._list(limit=limit)


class CogniteFileCategoryQuery(_CogniteFileCategoryQuery[CogniteFileCategoryList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteFileCategoryList)
