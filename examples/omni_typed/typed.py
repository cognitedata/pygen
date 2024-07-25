from __future__ import annotations

from datetime import date, datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.typed_instances import (
    PropertyOptions,
    TypedNode,
    TypedNodeApply,
)


class CDFExternalReferencesListedProperties:

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "CDFExternalReferencesListed", "1")


class CDFExternalReferencesListedApply(CDFExternalReferencesListedProperties, TypedNodeApply):
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
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.files = files
        self.sequences = sequences
        self.timeseries = timeseries


class CDFExternalReferencesListed(CDFExternalReferencesListedProperties, TypedNode):
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
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


class DependentOnNonWritableProperties:
    a_value = PropertyOptions("aValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "DependentOnNonWritable", "1")


class DependentOnNonWritableApply(DependentOnNonWritableProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        a_value: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.a_value = a_value


class DependentOnNonWritable(DependentOnNonWritableProperties, TypedNode):
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.a_value = a_value

    def as_write(self) -> DependentOnNonWritableApply:
        return DependentOnNonWritableApply(
            self.space,
            self.external_id,
            a_value=self.a_value,
            existing_version=self.version,
            type=self.type,
        )


class MainInterfaceProperties:
    main_value = PropertyOptions("mainValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "MainInterface", "1")


class MainInterfaceApply(MainInterfaceProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        main_value: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.main_value = main_value


class MainInterface(MainInterfaceProperties, TypedNode):
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.main_value = main_value

    def as_write(self) -> MainInterfaceApply:
        return MainInterfaceApply(
            self.space,
            self.external_id,
            main_value=self.main_value,
            existing_version=self.version,
            type=self.type,
        )


class PrimitiveNullableProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "PrimitiveNullable", "1")


class PrimitiveNullableApply(PrimitiveNullableProperties, TypedNodeApply):
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


class PrimitiveNullable(PrimitiveNullableProperties, TypedNode):
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


class PrimitiveRequiredListedProperties:
    float_32 = PropertyOptions("float32")
    float_64 = PropertyOptions("float64")
    int_32 = PropertyOptions("int32")
    int_64 = PropertyOptions("int64")
    json_ = PropertyOptions("json")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "PrimitiveRequiredListed", "1")


class PrimitiveRequiredListedApply(PrimitiveRequiredListedProperties, TypedNodeApply):
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


class PrimitiveRequiredListed(PrimitiveRequiredListedProperties, TypedNode):
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


class SubInterfaceProperties:
    sub_value = PropertyOptions("subValue")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "SubInterface", "1")


class SubInterfaceApply(SubInterfaceProperties, MainInterfaceApply):
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
        super().__init__(space, external_id, main_value=main_value, existing_version=existing_version, type=type)
        self.sub_value = sub_value


class SubInterface(SubInterfaceProperties, MainInterface):
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            main_value=main_value,
            type=type,
            deleted_time=deleted_time,
        )
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


class Implementation1Properties:
    value_2 = PropertyOptions("value2")
    value_1 = PropertyOptions("value1")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "Implementation1", "1")


class Implementation1Apply(Implementation1Properties, SubInterfaceApply):
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
        super().__init__(
            space, external_id, main_value=main_value, sub_value=sub_value, existing_version=existing_version, type=type
        )
        self.value_2 = value_2
        self.value_1 = value_1


class Implementation1(Implementation1Properties, SubInterface):
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
        self.value_2 = value_2
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


class Implementation2Properties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("pygen-models", "Implementation2", "1")


class Implementation2Apply(Implementation2Properties, SubInterfaceApply):
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
        super().__init__(
            space, external_id, main_value=main_value, sub_value=sub_value, existing_version=existing_version, type=type
        )


class Implementation2(Implementation2Properties, SubInterface):
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

    def as_write(self) -> Implementation2Apply:
        return Implementation2Apply(
            self.space,
            self.external_id,
            main_value=self.main_value,
            sub_value=self.sub_value,
            existing_version=self.version,
            type=self.type,
        )
