from cognite.pygen._client.models import ViewReference

from .field import Field
from .model import CodeModel


class DataClass(CodeModel):
    view_id: ViewReference
    name: str
    fields: list[Field]


class ReadDataClass(DataClass):
    write_class_name: str | None = None
