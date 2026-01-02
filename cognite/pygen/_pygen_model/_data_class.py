from collections.abc import Iterable

from ._model import CodeModel


class Field(CodeModel):
    cdf_prop_id: str
    name: str
    dtype: str
    type_hint: str
    default_value: str | None = None
    filter_name: str | None = None
    description: str | None = None


class DataClass(CodeModel):
    name: str
    display_name: str
    description: str
    fields: list[Field]

    def list_fields(self, dtype: str | None = None) -> Iterable[Field]:
        return (field for field in self.fields if field.dtype == dtype or dtype is None)


class ListDataClass(CodeModel):
    name: str


class FilterClass(CodeModel):
    name: str
