from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite


__all__ = [
    "CogniteUnit",
    "CogniteUnitWrite",
    "CogniteUnitApply",
    "CogniteUnitList",
    "CogniteUnitWriteList",
    "CogniteUnitApplyList",
    "CogniteUnitFields",
    "CogniteUnitTextFields",
    "CogniteUnitGraphQL",
]


CogniteUnitTextFields = Literal[
    "external_id", "aliases", "description", "name", "quantity", "source", "source_reference", "symbol", "tags"
]
CogniteUnitFields = Literal[
    "external_id", "aliases", "description", "name", "quantity", "source", "source_reference", "symbol", "tags"
]

_COGNITEUNIT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "quantity": "quantity",
    "source": "source",
    "source_reference": "sourceReference",
    "symbol": "symbol",
    "tags": "tags",
}


class CogniteUnitGraphQL(GraphQLCore):
    """This represents the reading version of Cognite unit, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite unit.
        data_record: The data record of the Cognite unit node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        quantity: Specifies the physical quantity the unit measures
        source: Source of the unit definition
        source_reference: Reference to the source of the unit definition
        symbol: The symbol for the unit of measurement
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteUnit", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    quantity: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = Field(None, alias="sourceReference")
    symbol: Optional[str] = None
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
    def as_read(self) -> CogniteUnit:
        """Convert this GraphQL format of Cognite unit to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteUnit(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            quantity=self.quantity,
            source=self.source,
            source_reference=self.source_reference,
            symbol=self.symbol,
            tags=self.tags,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteUnitWrite:
        """Convert this GraphQL format of Cognite unit to the writing format."""
        return CogniteUnitWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            quantity=self.quantity,
            source=self.source,
            source_reference=self.source_reference,
            symbol=self.symbol,
            tags=self.tags,
        )


class CogniteUnit(CogniteDescribableNode):
    """This represents the reading version of Cognite unit.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite unit.
        data_record: The data record of the Cognite unit node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        quantity: Specifies the physical quantity the unit measures
        source: Source of the unit definition
        source_reference: Reference to the source of the unit definition
        symbol: The symbol for the unit of measurement
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteUnit", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    quantity: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = Field(None, alias="sourceReference")
    symbol: Optional[str] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteUnitWrite:
        """Convert this read version of Cognite unit to the writing version."""
        return CogniteUnitWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            quantity=self.quantity,
            source=self.source,
            source_reference=self.source_reference,
            symbol=self.symbol,
            tags=self.tags,
        )

    def as_apply(self) -> CogniteUnitWrite:
        """Convert this read version of Cognite unit to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteUnitWrite(CogniteDescribableNodeWrite):
    """This represents the writing version of Cognite unit.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite unit.
        data_record: The data record of the Cognite unit node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        quantity: Specifies the physical quantity the unit measures
        source: Source of the unit definition
        source_reference: Reference to the source of the unit definition
        symbol: The symbol for the unit of measurement
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteUnit", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    quantity: Optional[str] = None
    source: Optional[str] = None
    source_reference: Optional[str] = Field(None, alias="sourceReference")
    symbol: Optional[str] = None

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

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.quantity is not None or write_none:
            properties["quantity"] = self.quantity

        if self.source is not None or write_none:
            properties["source"] = self.source

        if self.source_reference is not None or write_none:
            properties["sourceReference"] = self.source_reference

        if self.symbol is not None or write_none:
            properties["symbol"] = self.symbol

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


class CogniteUnitApply(CogniteUnitWrite):
    def __new__(cls, *args, **kwargs) -> CogniteUnitApply:
        warnings.warn(
            "CogniteUnitApply is deprecated and will be removed in v1.0. "
            "Use CogniteUnitWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteUnit.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteUnitList(DomainModelList[CogniteUnit]):
    """List of Cognite units in the read version."""

    _INSTANCE = CogniteUnit

    def as_write(self) -> CogniteUnitWriteList:
        """Convert these read versions of Cognite unit to the writing versions."""
        return CogniteUnitWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteUnitWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteUnitWriteList(DomainModelWriteList[CogniteUnitWrite]):
    """List of Cognite units in the writing version."""

    _INSTANCE = CogniteUnitWrite


class CogniteUnitApplyList(CogniteUnitWriteList): ...


def _create_cognite_unit_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    quantity: str | list[str] | None = None,
    quantity_prefix: str | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    source_reference: str | list[str] | None = None,
    source_reference_prefix: str | None = None,
    symbol: str | list[str] | None = None,
    symbol_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
    if isinstance(quantity, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("quantity"), value=quantity))
    if quantity and isinstance(quantity, list):
        filters.append(dm.filters.In(view_id.as_property_ref("quantity"), values=quantity))
    if quantity_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("quantity"), value=quantity_prefix))
    if isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if isinstance(source_reference, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceReference"), value=source_reference))
    if source_reference and isinstance(source_reference, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceReference"), values=source_reference))
    if source_reference_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceReference"), value=source_reference_prefix))
    if isinstance(symbol, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("symbol"), value=symbol))
    if symbol and isinstance(symbol, list):
        filters.append(dm.filters.In(view_id.as_property_ref("symbol"), values=symbol))
    if symbol_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("symbol"), value=symbol_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteUnitQuery(NodeQueryCore[T_DomainModelList, CogniteUnitList]):
    _view_id = CogniteUnit._view_id
    _result_cls = CogniteUnit
    _result_list_cls_end = CogniteUnitList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.quantity = StringFilter(self, self._view_id.as_property_ref("quantity"))
        self.source = StringFilter(self, self._view_id.as_property_ref("source"))
        self.source_reference = StringFilter(self, self._view_id.as_property_ref("sourceReference"))
        self.symbol = StringFilter(self, self._view_id.as_property_ref("symbol"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
                self.quantity,
                self.source,
                self.source_reference,
                self.symbol,
            ]
        )

    def list_cognite_unit(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteUnitList:
        return self._list(limit=limit)


class CogniteUnitQuery(_CogniteUnitQuery[CogniteUnitList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteUnitList)
