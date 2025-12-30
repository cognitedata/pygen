from typing import Literal

from cognite.pygen._client.models import ViewReference

from .field import Field
from .model import CodeModel


class DataClass(CodeModel):
    view_id: ViewReference
    name: str
    fields: list[Field]
    instance_type: Literal["node", "edge"]


class ReadDataClass(DataClass):
    write_class_name: str | None = None
