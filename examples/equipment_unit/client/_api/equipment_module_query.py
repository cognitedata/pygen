from cognite.client import data_modeling as dm
from examples.equipment_unit.client.data_classes import DomainModelList
from ._core import QueryExpression, QueryBuilder, QueryAPI
from cognite.client import CogniteClient


class EquipmentModuleQueryAPI(QueryAPI):
    def __init__(self, client: CogniteClient, builder: QueryBuilder, from_: str, equipment_view: dm.ViewId):
        super().__init__(client, builder, from_)
        self._equipment_view = equipment_view

    def query(self, retrieve_equipment_module: bool = True) -> DomainModelList:
        # Todo add equipment module to builder if retrieve_equipment_module
        return self._query()
