from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from equipment_unit.client.data_classes import UnitProcedureList, DomainModelList

if TYPE_CHECKING:
    from .equipment_module_query import EquipmentModuleQuery


class UnitProcedureQuery:
    def __init__(self):
        ...

    def work_units(
        self,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        space: str = "IntegrationTestsImmutable",
        limit: int | None = None,
    ) -> EquipmentModuleQuery:
        ...

    def query(self, retrieve_unit_procedure: bool = True) -> DomainModelList:
        ...
