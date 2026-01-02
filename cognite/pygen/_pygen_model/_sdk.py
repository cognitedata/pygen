from typing import Literal

from cognite.pygen._client.models import ViewReference

from ._data_class import DataClass, FilterClass, ListDataClass
from ._model import CodeModel


class DataClassFile(CodeModel):
    filename: str
    instance_type: Literal["node", "edge"]
    view_id: ViewReference
    read: DataClass
    read_list: ListDataClass
    filter: FilterClass
    write: DataClass | None = None


class APIClassFile(CodeModel):
    filename: str
    name: str
    client_attribute_name: str


class PygenSDKModel(CodeModel):
    data_classes: list[DataClassFile]
    api_classes: list[APIClassFile]
