from dataclasses import dataclass

from cognite.client import data_modeling as dm

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni.data_classes import DomainModel, DomainModelApply
else:
    from omni_pydantic_v1.data_classes import DomainModel, DomainModelApply


@dataclass
class OmniClassPair:
    read: type[DomainModel]
    write: type[DomainModelApply]
    view: dm.View
