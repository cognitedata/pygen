from ._model import CodeModel


class Field(CodeModel):
    cdf_prop_id: str
    name: str
    type_hint: str
    default_value: str | None = None
    filter_name: str | None = None
    description: str | None = None
