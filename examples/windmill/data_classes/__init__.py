from ._core import (
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
    ResourcesWriteResult,
)
from ._blade import Blade, BladeFields, BladeList, BladeTextFields, BladeWrite, BladeWriteList
from ._gearbox import Gearbox, GearboxFields, GearboxList, GearboxWrite, GearboxWriteList
from ._generator import Generator, GeneratorFields, GeneratorList, GeneratorWrite, GeneratorWriteList
from ._high_speed_shaft import (
    HighSpeedShaft,
    HighSpeedShaftFields,
    HighSpeedShaftList,
    HighSpeedShaftWrite,
    HighSpeedShaftWriteList,
)
from ._main_shaft import MainShaft, MainShaftFields, MainShaftList, MainShaftWrite, MainShaftWriteList
from ._metmast import Metmast, MetmastFields, MetmastList, MetmastWrite, MetmastWriteList
from ._nacelle import Nacelle, NacelleFields, NacelleList, NacelleWrite, NacelleWriteList
from ._power_inverter import (
    PowerInverter,
    PowerInverterFields,
    PowerInverterList,
    PowerInverterWrite,
    PowerInverterWriteList,
)
from ._rotor import Rotor, RotorFields, RotorList, RotorWrite, RotorWriteList
from ._sensor_position import (
    SensorPosition,
    SensorPositionFields,
    SensorPositionList,
    SensorPositionWrite,
    SensorPositionWriteList,
)
from ._windmill import Windmill, WindmillFields, WindmillList, WindmillTextFields, WindmillWrite, WindmillWriteList

Blade.model_rebuild()
BladeWrite.model_rebuild()
Nacelle.model_rebuild()
NacelleWrite.model_rebuild()
Windmill.model_rebuild()
WindmillWrite.model_rebuild()

__all__ = [
    "DataRecord",
    "DataRecordWrite",
    "ResourcesWrite",
    "DomainModel",
    "DomainModelCore",
    "DomainModelWrite",
    "DomainModelList",
    "DomainRelationWrite",
    "ResourcesWriteResult",
    "Blade",
    "BladeWrite",
    "BladeList",
    "BladeWriteList",
    "BladeFields",
    "BladeTextFields",
    "Gearbox",
    "GearboxWrite",
    "GearboxList",
    "GearboxWriteList",
    "GearboxFields",
    "Generator",
    "GeneratorWrite",
    "GeneratorList",
    "GeneratorWriteList",
    "GeneratorFields",
    "HighSpeedShaft",
    "HighSpeedShaftWrite",
    "HighSpeedShaftList",
    "HighSpeedShaftWriteList",
    "HighSpeedShaftFields",
    "MainShaft",
    "MainShaftWrite",
    "MainShaftList",
    "MainShaftWriteList",
    "MainShaftFields",
    "Metmast",
    "MetmastWrite",
    "MetmastList",
    "MetmastWriteList",
    "MetmastFields",
    "Nacelle",
    "NacelleWrite",
    "NacelleList",
    "NacelleWriteList",
    "NacelleFields",
    "PowerInverter",
    "PowerInverterWrite",
    "PowerInverterList",
    "PowerInverterWriteList",
    "PowerInverterFields",
    "Rotor",
    "RotorWrite",
    "RotorList",
    "RotorWriteList",
    "RotorFields",
    "SensorPosition",
    "SensorPositionWrite",
    "SensorPositionList",
    "SensorPositionWriteList",
    "SensorPositionFields",
    "Windmill",
    "WindmillWrite",
    "WindmillList",
    "WindmillWriteList",
    "WindmillFields",
    "WindmillTextFields",
]
