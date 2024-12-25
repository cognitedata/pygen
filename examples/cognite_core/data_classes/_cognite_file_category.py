from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

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
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite


__all__ = [
    "CogniteFileCategory",
    "CogniteFileCategoryWrite",
    "CogniteFileCategoryApply",
    "CogniteFileCategoryList",
    "CogniteFileCategoryWriteList",
    "CogniteFileCategoryApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteFileCategory:
        """Convert this GraphQL format of Cognite file category to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteFileCategory(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            code=self.code,
            description=self.description,
            name=self.name,
            standard=self.standard,
            standard_reference=self.standard_reference,
            tags=self.tags,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteFileCategoryWrite:
        """Convert this GraphQL format of Cognite file category to the writing format."""
        return CogniteFileCategoryWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            code=self.code,
            description=self.description,
            name=self.name,
            standard=self.standard,
            standard_reference=self.standard_reference,
            tags=self.tags,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteFileCategoryWrite:
        """Convert this read version of Cognite file category to the writing version."""
        return CogniteFileCategoryWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            code=self.code,
            description=self.description,
            name=self.name,
            standard=self.standard,
            standard_reference=self.standard_reference,
            tags=self.tags,
        )

    def as_apply(self) -> CogniteFileCategoryWrite:
        """Convert this read version of Cognite file category to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFileCategory", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    code: str
    standard: Optional[str] = None
    standard_reference: Optional[str] = Field(None, alias="standardReference")

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

        if self.code is not None:
            properties["code"] = self.code

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.standard is not None or write_none:
            properties["standard"] = self.standard

        if self.standard_reference is not None or write_none:
            properties["standardReference"] = self.standard_reference

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

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

        return resources


class CogniteFileCategoryApply(CogniteFileCategoryWrite):
    def __new__(cls, *args, **kwargs) -> CogniteFileCategoryApply:
        warnings.warn(
            "CogniteFileCategoryApply is deprecated and will be removed in v1.0. Use CogniteFileCategoryWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteFileCategory.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteFileCategoryList(DomainModelList[CogniteFileCategory]):
    """List of Cognite file categories in the read version."""

    _INSTANCE = CogniteFileCategory

    def as_write(self) -> CogniteFileCategoryWriteList:
        """Convert these read versions of Cognite file category to the writing versions."""
        return CogniteFileCategoryWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteFileCategoryWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteFileCategoryWriteList(DomainModelWriteList[CogniteFileCategoryWrite]):
    """List of Cognite file categories in the writing version."""

    _INSTANCE = CogniteFileCategoryWrite


class CogniteFileCategoryApplyList(CogniteFileCategoryWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
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
