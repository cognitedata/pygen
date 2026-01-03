from collections.abc import Iterable, Set
from typing import Literal

from cognite.pygen._client.models import ViewReference

from ._data_class import DataClass, Field, FilterClass, ListDataClass
from ._model import CodeModel


class DataClassFile(CodeModel):
    filename: str
    instance_type: Literal["node", "edge"]
    view_id: ViewReference
    read: DataClass
    read_list: ListDataClass
    filter: FilterClass
    write: DataClass | None = None

    def list_fields(self, dtype: str | Set[str] | None = None) -> Iterable[Field]:
        yield from self.read.list_fields(dtype)
        if self.write:
            yield from self.write.list_fields(dtype)


class APIClassFile(CodeModel):
    filename: str
    name: str
    client_attribute_name: str
    data_class: DataClassFile


class PygenSDKModel(CodeModel):
    data_classes: list[DataClassFile]
    api_classes: list[APIClassFile]
