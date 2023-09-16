from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class Naming:
    """
    Naming convention for a class or variable.

    Args:
        case: Case convention. One of "pascal", "snake", "unchanged".
        number: Number convention. One of "plural", "singular", "unchanged".
    """

    case: Literal["pascal", "snake", "unchanged"]
    number: Literal["plural", "singular", "unchanged"] = "unchanged"


@dataclass(frozen=True)
class NamingConfig:
    """
    The configuration of the naming used by pygen.

    Args:
        api_class_name: Naming convention for API class names. The api class name will be suffixed with "API".
                        For example, a view with external id "MyView" will generate an API class named "MyViewAPI".
        api_class_variable: Naming convention for API class variable. This is used when writing, for example,
                        my_client.[api_class_variable].list().
        data_class_name: Naming convention for data class names. Pygen will create two data classes one for reading
                        and one for writing. The writing class will be suffixed with "Apply".
        data_class_field: Naming convention for data class fields. Data class fields are the attributes of the
                        data classes and are generated from the properties of the view.
    """

    api_class_name: Naming = field(default_factory=lambda: Naming("pascal", "plural"))
    api_class_variable: Naming = field(default_factory=lambda: Naming("snake", "plural"))
    data_class_name: Naming = field(default_factory=lambda: Naming("pascal", "singular"))
    data_class_field: Naming = field(default_factory=lambda: Naming("snake"))


@dataclass
class PygenConfig:
    naming: NamingConfig
