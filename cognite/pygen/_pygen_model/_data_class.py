from ._field import Field
from ._model import CodeModel


class DataClass(CodeModel):
    name: str
    display_name: str
    description: str
    fields: list[Field]


class ListDataClass(CodeModel):
    name: str


class FilterClass(CodeModel):
    name: str
