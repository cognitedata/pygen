from ._data_class import DataClass, ReadDataClass
from ._model import CodeModel


class DataClassFile(CodeModel):
    filename: str
    read: ReadDataClass
    write: DataClass | None = None


class PygenSDKModel(CodeModel):
    data_classes: list[DataClassFile]
