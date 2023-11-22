from examples.equipment_unit.client.data_classes import DomainModelList


class EquipmentModuleQuery:
    def __init__(self):
        ...

    def query(self, retrieve_equipment_module: bool = True) -> DomainModelList:
        ...
