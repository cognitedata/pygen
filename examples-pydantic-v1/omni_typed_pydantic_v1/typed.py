from __future__ import annotations

from datetime import date, datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.typed_instances import (
    PropertyOptions,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)


class _CDFExternalReferencesProperties:

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "CDFExternalReferences", "1")


class CDFExternalReferencesApply(_CDFExternalReferencesProperties, TypedNodeApply):
    """This represents the writing format of cdf external reference.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external reference.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        file: str | None = None,
        sequence: str | None = None,
        timeseries: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.file = file
        self.sequence = sequence
        self.timeseries = timeseries


class CDFExternalReferences(_CDFExternalReferencesProperties, TypedNode):
    """This represents the reading format of cdf external reference.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external reference.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        file: str | None = None,
        sequence: str | None = None,
        timeseries: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.file = file
        self.sequence = sequence
        self.timeseries = timeseries

    def as_write(self) -> CDFExternalReferencesApply:
        return CDFExternalReferencesApply(
            self.space,
            self.external_id,
            file=self.file,
            sequence=self.sequence,
            timeseries=self.timeseries,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionItemAProperties:
    other_direct = PropertyOptions("otherDirect")
    self_direct = PropertyOptions("selfDirect")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionItemA", "1")


class ConnectionItemAApply(_ConnectionItemAProperties, TypedNodeApply):
    """This represents the writing format of connection item a.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item a.
        name: The name field.
        other_direct: The other direct field.
        self_direct: The self direct field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        other_direct: DirectRelationReference | tuple[str, str] | None = None,
        self_direct: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.name = name
        self.other_direct = DirectRelationReference.load(other_direct) if other_direct else None
        self.self_direct = DirectRelationReference.load(self_direct) if self_direct else None


class ConnectionItemA(_ConnectionItemAProperties, TypedNode):
    """This represents the reading format of connection item a.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item a.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: The name field.
        other_direct: The other direct field.
        self_direct: The self direct field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        other_direct: DirectRelationReference | None = None,
        self_direct: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.name = name
        self.other_direct = DirectRelationReference.load(other_direct) if other_direct else None
        self.self_direct = DirectRelationReference.load(self_direct) if self_direct else None

    def as_write(self) -> ConnectionItemAApply:
        return ConnectionItemAApply(
            self.space,
            self.external_id,
            name=self.name,
            other_direct=self.other_direct,
            self_direct=self.self_direct,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionItemBProperties:

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionItemB", "1")


class ConnectionItemBApply(_ConnectionItemBProperties, TypedNodeApply):
    """This represents the writing format of connection item b.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        name: The name field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.name = name


class ConnectionItemB(_ConnectionItemBProperties, TypedNode):
    """This represents the reading format of connection item b.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: The name field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.name = name

    def as_write(self) -> ConnectionItemBApply:
        return ConnectionItemBApply(
            self.space,
            self.external_id,
            name=self.name,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionItemDProperties:
    direct_multi = PropertyOptions("directMulti")
    direct_single = PropertyOptions("directSingle")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionItemD", "1")


class ConnectionItemDApply(_ConnectionItemDProperties, TypedNodeApply):
    """This represents the writing format of connection item d.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item d.
        direct_multi: The direct multi field.
        direct_single: The direct single field.
        name: The name field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        direct_multi: DirectRelationReference | tuple[str, str] | None = None,
        direct_single: DirectRelationReference | tuple[str, str] | None = None,
        name: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.direct_multi = DirectRelationReference.load(direct_multi) if direct_multi else None
        self.direct_single = DirectRelationReference.load(direct_single) if direct_single else None
        self.name = name


class ConnectionItemD(_ConnectionItemDProperties, TypedNode):
    """This represents the reading format of connection item d.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item d.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        direct_multi: The direct multi field.
        direct_single: The direct single field.
        name: The name field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        direct_multi: DirectRelationReference | None = None,
        direct_single: DirectRelationReference | None = None,
        name: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.direct_multi = DirectRelationReference.load(direct_multi) if direct_multi else None
        self.direct_single = DirectRelationReference.load(direct_single) if direct_single else None
        self.name = name

    def as_write(self) -> ConnectionItemDApply:
        return ConnectionItemDApply(
            self.space,
            self.external_id,
            direct_multi=self.direct_multi,
            direct_single=self.direct_single,
            name=self.name,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionItemEProperties:
    direct_no_source = PropertyOptions("directNoSource")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionItemE", "1")


class ConnectionItemEApply(_ConnectionItemEProperties, TypedNodeApply):
    """This represents the writing format of connection item e.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        direct_no_source: The direct no source field.
        name: The name field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        direct_no_source: DirectRelationReference | tuple[str, str] | None = None,
        name: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.direct_no_source = DirectRelationReference.load(direct_no_source) if direct_no_source else None
        self.name = name


class ConnectionItemE(_ConnectionItemEProperties, TypedNode):
    """This represents the reading format of connection item e.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        direct_no_source: The direct no source field.
        name: The name field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        direct_no_source: DirectRelationReference | None = None,
        name: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.direct_no_source = DirectRelationReference.load(direct_no_source) if direct_no_source else None
        self.name = name

    def as_write(self) -> ConnectionItemEApply:
        return ConnectionItemEApply(
            self.space,
            self.external_id,
            direct_no_source=self.direct_no_source,
            name=self.name,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionItemFProperties:
    direct_list = PropertyOptions("directList")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionItemF", "1")


class ConnectionItemFApply(_ConnectionItemFProperties, TypedNodeApply):
    """This represents the writing format of connection item f.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        direct_list: The direct list field.
        name: The name field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        direct_list: list[DirectRelationReference | tuple[str, str]] | None = None,
        name: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.direct_list = (
            [DirectRelationReference.load(direct_list) for direct_list in direct_list] if direct_list else None
        )
        self.name = name


class ConnectionItemF(_ConnectionItemFProperties, TypedNode):
    """This represents the reading format of connection item f.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        direct_list: The direct list field.
        name: The name field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        direct_list: list[DirectRelationReference] | None = None,
        name: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.direct_list = (
            [DirectRelationReference.load(direct_list) for direct_list in direct_list] if direct_list else None
        )
        self.name = name

    def as_write(self) -> ConnectionItemFApply:
        return ConnectionItemFApply(
            self.space,
            self.external_id,
            direct_list=self.direct_list,  # type: ignore[arg-type]
            name=self.name,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionItemGProperties:

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionItemG", "1")


class ConnectionItemGApply(_ConnectionItemGProperties, TypedNodeApply):
    """This represents the writing format of connection item g.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item g.
        name: The name field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.name = name


class ConnectionItemG(_ConnectionItemGProperties, TypedNode):
    """This represents the reading format of connection item g.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item g.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: The name field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.name = name

    def as_write(self) -> ConnectionItemGApply:
        return ConnectionItemGApply(
            self.space,
            self.external_id,
            name=self.name,
            existing_version=self.version,
            type=self.type,
        )


class _EmptyProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "Empty", "1")


class EmptyApply(_EmptyProperties, TypedNodeApply):
    """This represents the writing format of empty.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the empty.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        boolean: bool | None = None,
        date: date | None = None,
        float_32: float | None = None,
        float_64: float | None = None,
        int_32: int | None = None,
        int_64: int | None = None,
        json_: dict | None = None,
        text: str | None = None,
        timestamp: datetime | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp


class Empty(_EmptyProperties, TypedNode):
    """This represents the reading format of empty.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the empty.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        boolean: bool | None = None,
        date: date | None = None,
        float_32: float | None = None,
        float_64: float | None = None,
        int_32: int | None = None,
        int_64: int | None = None,
        json_: dict | None = None,
        text: str | None = None,
        timestamp: datetime | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp

    def as_write(self) -> EmptyApply:
        return EmptyApply(
            self.space,
            self.external_id,
            boolean=self.boolean,
            date=self.date,
            float_32=self.float_32,
            float_64=self.float_64,
            int_32=self.int_32,
            int_64=self.int_64,
            json_=self.json_,
            text=self.text,
            timestamp=self.timestamp,
            existing_version=self.version,
            type=self.type,
        )


class _PrimitiveNullableListedProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "PrimitiveNullableListed", "1")


class PrimitiveNullableListedApply(_PrimitiveNullableListedProperties, TypedNodeApply):
    """This represents the writing format of primitive nullable listed.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        boolean: list[bool] | None = None,
        date: list[date] | None = None,
        float_32: list[float] | None = None,
        float_64: list[float] | None = None,
        int_32: list[int] | None = None,
        int_64: list[int] | None = None,
        json_: list[dict] | None = None,
        text: list[str] | None = None,
        timestamp: list[datetime] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp


class PrimitiveNullableListed(_PrimitiveNullableListedProperties, TypedNode):
    """This represents the reading format of primitive nullable listed.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        boolean: list[bool] | None = None,
        date: list[date] | None = None,
        float_32: list[float] | None = None,
        float_64: list[float] | None = None,
        int_32: list[int] | None = None,
        int_64: list[int] | None = None,
        json_: list[dict] | None = None,
        text: list[str] | None = None,
        timestamp: list[datetime] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp

    def as_write(self) -> PrimitiveNullableListedApply:
        return PrimitiveNullableListedApply(
            self.space,
            self.external_id,
            boolean=self.boolean,
            date=self.date,
            float_32=self.float_32,
            float_64=self.float_64,
            int_32=self.int_32,
            int_64=self.int_64,
            json_=self.json_,
            text=self.text,
            timestamp=self.timestamp,
            existing_version=self.version,
            type=self.type,
        )


class _PrimitiveRequiredProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "PrimitiveRequired", "1")


class PrimitiveRequiredApply(_PrimitiveRequiredProperties, TypedNodeApply):
    """This represents the writing format of primitive required.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        boolean: bool,
        date: date,
        float_32: float,
        float_64: float,
        int_32: int,
        int_64: int,
        json_: dict,
        text: str,
        timestamp: datetime,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp


class PrimitiveRequired(_PrimitiveRequiredProperties, TypedNode):
    """This represents the reading format of primitive required.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        boolean: bool,
        date: date,
        float_32: float,
        float_64: float,
        int_32: int,
        int_64: int,
        json_: dict,
        text: str,
        timestamp: datetime,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp

    def as_write(self) -> PrimitiveRequiredApply:
        return PrimitiveRequiredApply(
            self.space,
            self.external_id,
            boolean=self.boolean,
            date=self.date,
            float_32=self.float_32,
            float_64=self.float_64,
            int_32=self.int_32,
            int_64=self.int_64,
            json_=self.json_,
            text=self.text,
            timestamp=self.timestamp,
            existing_version=self.version,
            type=self.type,
        )


class _PrimitiveWithDefaultsProperties:
    auto_increment_int_32 = PropertyOptions("autoIncrementInt32")
    default_boolean = PropertyOptions("defaultBoolean")
    default_float_32 = PropertyOptions("defaultFloat32")
    default_object = PropertyOptions("defaultObject")
    default_string = PropertyOptions("defaultString")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "PrimitiveWithDefaults", "1")


class PrimitiveWithDefaultsApply(_PrimitiveWithDefaultsProperties, TypedNodeApply):
    """This represents the writing format of primitive with default.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        auto_increment_int_32: int,
        default_boolean: bool | None = None,
        default_float_32: float | None = None,
        default_object: dict | None = None,
        default_string: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.auto_increment_int_32 = auto_increment_int_32
        self.default_boolean = default_boolean
        self.default_float_32 = default_float_32
        self.default_object = default_object
        self.default_string = default_string


class PrimitiveWithDefaults(_PrimitiveWithDefaultsProperties, TypedNode):
    """This represents the reading format of primitive with default.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        auto_increment_int_32: int,
        default_boolean: bool | None = None,
        default_float_32: float | None = None,
        default_object: dict | None = None,
        default_string: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.auto_increment_int_32 = auto_increment_int_32
        self.default_boolean = default_boolean
        self.default_float_32 = default_float_32
        self.default_object = default_object
        self.default_string = default_string

    def as_write(self) -> PrimitiveWithDefaultsApply:
        return PrimitiveWithDefaultsApply(
            self.space,
            self.external_id,
            auto_increment_int_32=self.auto_increment_int_32,
            default_boolean=self.default_boolean,
            default_float_32=self.default_float_32,
            default_object=self.default_object,
            default_string=self.default_string,
            existing_version=self.version,
            type=self.type,
        )


class _Implementation1NonWriteableProperties:
    value_1 = PropertyOptions("value1")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "Implementation1NonWriteable", "1")


class Implementation1NonWriteableApply(_Implementation1NonWriteableProperties, SubInterfaceApply):
    """This represents the writing format of implementation 1 non writeable.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        main_value: str | None = None,
        sub_value: str | None = None,
        value_1: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space, external_id, main_value=main_value, sub_value=sub_value, existing_version=existing_version, type=type
        )
        self.value_1 = value_1


class Implementation1NonWriteable(_Implementation1NonWriteableProperties, SubInterface):
    """This represents the reading format of implementation 1 non writeable.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        main_value: str | None = None,
        sub_value: str | None = None,
        value_1: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            main_value=main_value,
            sub_value=sub_value,
            type=type,
            deleted_time=deleted_time,
        )
        self.value_1 = value_1

    def as_write(self) -> Implementation1NonWriteableApply:
        return Implementation1NonWriteableApply(
            self.space,
            self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
            existing_version=self.version,
            type=self.type,
        )


class _ConnectionEdgeAProperties:
    end_time = PropertyOptions("endTime")
    start_time = PropertyOptions("startTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "ConnectionEdgeA", "1")


class ConnectionEdgeAApply(_ConnectionEdgeAProperties, TypedEdgeApply):
    """This represents the writing format of connection edge a.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection edge a.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.

        end_time: The end time field.
        name: The name field.
        start_time: The start time field.
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge
            (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        end_time: datetime | None = None,
        name: str | None = None,
        start_time: datetime | None = None,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.end_time = end_time
        self.name = name
        self.start_time = start_time


class ConnectionEdgeA(_ConnectionEdgeAProperties, TypedEdge):
    """This represents the reading format of connection edge a.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection edge a.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time: The end time field.
        name: The name field.
        start_time: The start time field.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        end_time: datetime | None = None,
        name: str | None = None,
        start_time: datetime | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedEdge.__init__(
            self,
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            None,
        )
        self.end_time = end_time
        self.name = name
        self.start_time = start_time

    def as_write(self) -> ConnectionEdgeAApply:
        return ConnectionEdgeAApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            end_time=self.end_time,
            name=self.name,
            start_time=self.start_time,
            existing_version=self.version,
        )


class ConnectionItemCEdgeApply(_ConnectionItemCProperties, TypedEdgeApply):
    """This represents the writing format of connection item c edge.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.

        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge
            (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)


class ConnectionItemCEdge(_ConnectionItemCProperties, TypedEdge):
    """This represents the reading format of connection item c edge.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        deleted_time: int | None = None,
    ) -> None:
        TypedEdge.__init__(
            self,
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            None,
        )

    def as_write(self) -> ConnectionItemCEdgeApply:
        return ConnectionItemCEdgeApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            existing_version=self.version,
        )
