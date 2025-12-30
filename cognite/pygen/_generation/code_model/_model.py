from pydantic import BaseModel, ConfigDict


class CodeModel(BaseModel):
    """Base class for code models used in code generation."""

    model_config = ConfigDict()
