from __future__ import annotations

from dataclasses import dataclass

from cognite.client import data_modeling as dm

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni.data_classes import DomainModel, DomainModelApply
else:
    from omni_pydantic_v1.data_classes import DomainModel, DomainModelApply


@dataclass
class OmniClasses:
    read: type[DomainModel]
    write: type[DomainModelApply] | None
    api_name: str
    view: dm.View


class OmniView:
    connection_item_a: str = "ConnectionItemA"
    connection_item_b: str = "ConnectionItemB"
    connection_item_c: str = "ConnectionItemC"

    primitive_nullable: str = "PrimitiveNullable"
    primitive_nullable_listed: str = "PrimitiveNullableListed"
    primitive_required: str = "PrimitiveRequired"
    primitive_required_listed: str = "PrimitiveRequiredListed"
    cdf_external_references: str = "CDFExternalReferences"
    cdf_external_references_listed: str = "CDFExternalReferencesListed"
    primitive_with_defaults: str = "PrimitiveWithDefaults"
    empty: str = "Empty"
