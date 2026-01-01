from ._data_class import DataClass, FilterClass, ListDataClass, ReadDataClass
from ._model import CodeModel


class DataClassFile(CodeModel):
    filename: str
    read: ReadDataClass
    read_list: ListDataClass
    filter: FilterClass
    write: DataClass | None = None


class APIClassFile(CodeModel):
    filename: str
    name: str
    client_attribute_name: str
    read_class_name: str
    read_list_class_name: str
    filter_class: FilterClass


class PygenSDKModel(CodeModel):
    data_classes: list[DataClassFile]
    api_classes: list[APIClassFile]
