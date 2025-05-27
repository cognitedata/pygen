from __future__ import annotations

from datetime import date, datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    PropertyOptions,
    TypedNode,
    TypedNodeApply,
)


class _CDFExternalReferencesListedProperties:

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "CDFExternalReferencesListed", "1")


class CDFExternalReferencesListedApply(_CDFExternalReferencesListedProperties, TypedNodeApply):
    """This represents the writing format of cdf external references listed.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        files: The file field.
        sequences: The sequence field.
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
        files: list[str] | None = None,
        sequences: list[str] | None = None,
        timeseries: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.files = files
        self.sequences = sequences
        self.timeseries = timeseries


class CDFExternalReferencesListed(_CDFExternalReferencesListedProperties, TypedNode):
    """This represents the reading format of cdf external references listed.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        files: The file field.
        sequences: The sequence field.
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
        files: list[str] | None = None,
        sequences: list[str] | None = None,
        timeseries: list[str] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.files = files
        self.sequences = sequences
        self.timeseries = timeseries

    def as_write(self) -> CDFExternalReferencesListedApply:
        return CDFExternalReferencesListedApply(
            self.space,
            self.external_id,
            files=self.files,
            sequences=self.sequences,
            timeseries=self.timeseries,
            existing_version=self.version,
            type=self.type,
        )


class _DependentOnNonWritableProperties:
    a_value = PropertyOptions("aValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "DependentOnNonWritable", "1")


class DependentOnNonWritableApply(_DependentOnNonWritableProperties, TypedNodeApply):
    """This represents the writing format of dependent on non writable.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        a_value: The a value field.
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
        a_value: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.a_value = a_value


class DependentOnNonWritable(_DependentOnNonWritableProperties, TypedNode):
    """This represents the reading format of dependent on non writable.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        a_value: The a value field.
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
        a_value: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.a_value = a_value

    def as_write(self) -> DependentOnNonWritableApply:
        return DependentOnNonWritableApply(
            self.space,
            self.external_id,
            a_value=self.a_value,
            existing_version=self.version,
            type=self.type,
        )


class _Implementation1Properties:
    value_2 = PropertyOptions("value2")
    main_value = PropertyOptions("mainValue")
    sub_value = PropertyOptions("subValue")
    value_1 = PropertyOptions("value1")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "Implementation1", "1")


class Implementation1Apply(_Implementation1Properties, TypedNodeApply):
    """This represents the writing format of implementation 1.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1.
        value_2: The value 2 field.
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
        value_2: str,
        main_value: str | None = None,
        sub_value: str | None = None,
        value_1: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.value_2 = value_2
        self.main_value = main_value
        self.sub_value = sub_value
        self.value_1 = value_1


class Implementation1(_Implementation1Properties, TypedNode):
    """This represents the reading format of implementation 1.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        value_2: The value 2 field.
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
        value_2: str,
        main_value: str | None = None,
        sub_value: str | None = None,
        value_1: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.value_2 = value_2
        self.main_value = main_value
        self.sub_value = sub_value
        self.value_1 = value_1

    def as_write(self) -> Implementation1Apply:
        return Implementation1Apply(
            self.space,
            self.external_id,
            value_2=self.value_2,
            main_value=self.main_value,
            sub_value=self.sub_value,
            value_1=self.value_1,
            existing_version=self.version,
            type=self.type,
        )


class _Implementation2Properties:
    main_value = PropertyOptions("mainValue")
    sub_value = PropertyOptions("subValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "Implementation2", "1")


class Implementation2Apply(_Implementation2Properties, TypedNodeApply):
    """This represents the writing format of implementation 2.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 2.
        main_value: The main value field.
        sub_value: The sub value field.
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
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.main_value = main_value
        self.sub_value = sub_value


class Implementation2(_Implementation2Properties, TypedNode):
    """This represents the reading format of implementation 2.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 2.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        main_value: The main value field.
        sub_value: The sub value field.
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
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.main_value = main_value
        self.sub_value = sub_value

    def as_write(self) -> Implementation2Apply:
        return Implementation2Apply(
            self.space,
            self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
            existing_version=self.version,
            type=self.type,
        )


class _MainInterfaceProperties:
    main_value = PropertyOptions("mainValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "MainInterface", "1")


class MainInterfaceApply(_MainInterfaceProperties, TypedNodeApply):
    """This represents the writing format of main interface.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        main_value: The main value field.
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
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.main_value = main_value


class MainInterface(_MainInterfaceProperties, TypedNode):
    """This represents the reading format of main interface.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        main_value: The main value field.
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
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.main_value = main_value

    def as_write(self) -> MainInterfaceApply:
        return MainInterfaceApply(
            self.space,
            self.external_id,
            main_value=self.main_value,
            existing_version=self.version,
            type=self.type,
        )


class _PrimitiveNullableProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "PrimitiveNullable", "1")


class PrimitiveNullableApply(_PrimitiveNullableProperties, TypedNodeApply):
    """This represents the writing format of primitive nullable.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable.
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
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp


class PrimitiveNullable(_PrimitiveNullableProperties, TypedNode):
    """This represents the reading format of primitive nullable.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp

    def as_write(self) -> PrimitiveNullableApply:
        return PrimitiveNullableApply(
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


class _PrimitiveRequiredListedProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "PrimitiveRequiredListed", "1")


class PrimitiveRequiredListedApply(_PrimitiveRequiredListedProperties, TypedNodeApply):
    """This represents the writing format of primitive required listed.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
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
        boolean: list[bool],
        date: list[date],
        float_32: list[float],
        float_64: list[float],
        int_32: list[int],
        int_64: list[int],
        json_: list[dict],
        text: list[str],
        timestamp: list[datetime],
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp


class PrimitiveRequiredListed(_PrimitiveRequiredListedProperties, TypedNode):
    """This represents the reading format of primitive required listed.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
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
        boolean: list[bool],
        date: list[date],
        float_32: list[float],
        float_64: list[float],
        int_32: list[int],
        int_64: list[int],
        json_: list[dict],
        text: list[str],
        timestamp: list[datetime],
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.boolean = boolean
        self.date = date
        self.float_32 = float_32
        self.float_64 = float_64
        self.int_32 = int_32
        self.int_64 = int_64
        self.json_ = json_
        self.text = text
        self.timestamp = timestamp

    def as_write(self) -> PrimitiveRequiredListedApply:
        return PrimitiveRequiredListedApply(
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


class _SubInterfaceProperties:
    main_value = PropertyOptions("mainValue")
    sub_value = PropertyOptions("subValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_pygen_models", "SubInterface", "1")


class SubInterfaceApply(_SubInterfaceProperties, TypedNodeApply):
    """This represents the writing format of sub interface.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        main_value: The main value field.
        sub_value: The sub value field.
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
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.main_value = main_value
        self.sub_value = sub_value


class SubInterface(_SubInterfaceProperties, TypedNode):
    """This represents the reading format of sub interface.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        main_value: The main value field.
        sub_value: The sub value field.
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
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.main_value = main_value
        self.sub_value = sub_value

    def as_write(self) -> SubInterfaceApply:
        return SubInterfaceApply(
            self.space,
            self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
            existing_version=self.version,
            type=self.type,
        )
