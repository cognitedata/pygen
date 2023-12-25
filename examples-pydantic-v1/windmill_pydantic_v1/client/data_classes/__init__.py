from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    ResourcesApplyResult,
)
from ._blade import Blade, BladeApply, BladeApplyList, BladeFields, BladeList, BladeTextFields
from ._gearbox import Gearbox, GearboxApply, GearboxApplyList, GearboxFields, GearboxList
from ._generator import Generator, GeneratorApply, GeneratorApplyList, GeneratorFields, GeneratorList
from ._high_speed_shaft import (
    HighSpeedShaft,
    HighSpeedShaftApply,
    HighSpeedShaftApplyList,
    HighSpeedShaftFields,
    HighSpeedShaftList,
)
from ._main_shaft import MainShaft, MainShaftApply, MainShaftApplyList, MainShaftFields, MainShaftList
from ._metmast import Metmast, MetmastApply, MetmastApplyList, MetmastFields, MetmastList
from ._nacelle import Nacelle, NacelleApply, NacelleApplyList, NacelleFields, NacelleList
from ._power_inverter import (
    PowerInverter,
    PowerInverterApply,
    PowerInverterApplyList,
    PowerInverterFields,
    PowerInverterList,
)
from ._rotor import Rotor, RotorApply, RotorApplyList, RotorFields, RotorList
from ._sensor_position import (
    SensorPosition,
    SensorPositionApply,
    SensorPositionApplyList,
    SensorPositionFields,
    SensorPositionList,
)
from ._windmill import Windmill, WindmillApply, WindmillApplyList, WindmillFields, WindmillList, WindmillTextFields


Blade.update_forward_refs(
    SensorPosition=SensorPosition,
)
BladeApply.update_forward_refs(
    SensorPositionApply=SensorPositionApply,
)

Nacelle.update_forward_refs(
    Gearbox=Gearbox,
    Generator=Generator,
    HighSpeedShaft=HighSpeedShaft,
    MainShaft=MainShaft,
    PowerInverter=PowerInverter,
)
NacelleApply.update_forward_refs(
    GearboxApply=GearboxApply,
    GeneratorApply=GeneratorApply,
    HighSpeedShaftApply=HighSpeedShaftApply,
    MainShaftApply=MainShaftApply,
    PowerInverterApply=PowerInverterApply,
)

Windmill.update_forward_refs(
    Blade=Blade,
    Metmast=Metmast,
    Nacelle=Nacelle,
    Rotor=Rotor,
)
WindmillApply.update_forward_refs(
    BladeApply=BladeApply,
    MetmastApply=MetmastApply,
    NacelleApply=NacelleApply,
    RotorApply=RotorApply,
)

__all__ = [
    "ResourcesApply",
    "DomainModel",
    "DomainModelApply",
    "DomainModelList",
    "DomainRelationApply",
    "ResourcesApplyResult",
    "Blade",
    "BladeApply",
    "BladeList",
    "BladeApplyList",
    "BladeFields",
    "BladeTextFields",
    "Gearbox",
    "GearboxApply",
    "GearboxList",
    "GearboxApplyList",
    "GearboxFields",
    "Generator",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorApplyList",
    "GeneratorFields",
    "HighSpeedShaft",
    "HighSpeedShaftApply",
    "HighSpeedShaftList",
    "HighSpeedShaftApplyList",
    "HighSpeedShaftFields",
    "MainShaft",
    "MainShaftApply",
    "MainShaftList",
    "MainShaftApplyList",
    "MainShaftFields",
    "Metmast",
    "MetmastApply",
    "MetmastList",
    "MetmastApplyList",
    "MetmastFields",
    "Nacelle",
    "NacelleApply",
    "NacelleList",
    "NacelleApplyList",
    "NacelleFields",
    "PowerInverter",
    "PowerInverterApply",
    "PowerInverterList",
    "PowerInverterApplyList",
    "PowerInverterFields",
    "Rotor",
    "RotorApply",
    "RotorList",
    "RotorApplyList",
    "RotorFields",
    "SensorPosition",
    "SensorPositionApply",
    "SensorPositionList",
    "SensorPositionApplyList",
    "SensorPositionFields",
    "Windmill",
    "WindmillApply",
    "WindmillList",
    "WindmillApplyList",
    "WindmillFields",
    "WindmillTextFields",
]
