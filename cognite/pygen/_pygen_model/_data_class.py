from collections.abc import Iterable, Set

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

    def list_fields(self, dtype: str | Set[str] | None = None) -> Iterable[Field]:
        iterable = (field for field in self.fields)
        if dtype is None:
            yield from iterable
        elif isinstance(dtype, str):
            yield from (field for field in iterable if field.dtype == dtype)
        else:
            yield from (field for field in iterable if field.dtype in dtype)


class ListDataClass(CodeModel):
    name: str


class FilterClass(CodeModel):
    name: str
