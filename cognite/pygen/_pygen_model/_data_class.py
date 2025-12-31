from typing import Literal

from cognite.pygen._client.models import ViewReference

from ._field import Field
from ._model import CodeModel


class DataClass(CodeModel):
    view_id: ViewReference
    name: str
    fields: list[Field]
    instance_type: Literal["node", "edge"]
    display_name: str
    description: str


class ReadDataClass(DataClass):
    write_class_name: str | None = None
