from .model import CodeModel


class Field(CodeModel):
    cdf_prop_id: str
    name: str
    type_hint: str
    filter_name: str | None = None
    description: str | None = None
