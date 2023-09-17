from dataclasses import dataclass, field
from enum import Enum

# Make into pydantic models and require validation on assigment.


class Case(Enum):
    pascal = "pascal"
    snake = "snake"
    camel = "camel"


class Number(Enum):
    plural = "plural"
    singular = "singular"
    unchanged = "unchanged"


@dataclass(frozen=True)
class Naming:
    """
    Naming convention for a class or variable.

    Args:
        case: Case convention. One of "pascal", "snake", "unchanged".
        number: Number convention. One of "plural", "singular", "unchanged".
    """

    case: Case
    number: Number = Number.unchanged


# @dataclass(frozen=True)
# class APIClassNaming:
#     """
#     The configuration of the naming used by pygen.
#
#     Args:
#         api_class_name: Naming convention for API class names. The api class name will be suffixed with "API".
#                         For example, a view with external id "MyView" will generate an API class named "MyViewAPI".
#         data_class: The naming convention for the generated data classes.
#         api_class_variable: Naming convention for API class variable. This is used when writing, for example,
#                         my_client.[api_class_variable].list().
#     """
#


@dataclass(frozen=True)
class FieldNaming:
    """
    Naming convention for fields.

    Args:
        name: Naming convention for data class fields. Data class fields are the attributes of the
                data classes and are generated from the properties of the view.

    """

    name: Naming = field(default_factory=lambda: Naming(Case.snake, Number.unchanged))
    variable: Naming = field(default_factory=lambda: Naming(Case.snake, Number.singular))
    edge_api_class: Naming = field(default_factory=lambda: Naming(Case.pascal, Number.plural))
    edge_api_attribute: Naming = field(default_factory=lambda: Naming(Case.snake, Number.plural))


@dataclass(frozen=True)
class DataClassNaming:
    """
    Naming convention for data classes.

    Args:
        name: Naming convention for data class names. Pygen will create two data classes one for reading
            and one for writing. The writing class will be suffixed with "Apply".
        variable: Naming convention for data class variable. This is used, for example, in as the
            parameter name in the apply method of the API class.
        file: Naming convention for data class file.
    """

    name: Naming = field(default_factory=lambda: Naming(Case.pascal, Number.singular))
    variable: Naming = field(default_factory=lambda: Naming(Case.snake, Number.singular))
    variable_list: Naming = field(default_factory=lambda: Naming(Case.snake, Number.plural))
    file: Naming = field(default_factory=lambda: Naming(Case.snake, Number.plural))


@dataclass(frozen=True)
class APIClassNaming:
    """
    Naming convention for API classes.

    Args:
        name: Naming convention for API class names. The api class name will be suffixed with "API".
                        For example, a view with external id "MyView" will generate an API class named "MyViewAPI".
        variable: Naming convention for API class variable. This is used when writing, for example,
                        my_client.[api_class_variable].list().
    """

    name: Naming = field(default_factory=lambda: Naming(Case.pascal, Number.plural))
    variable: Naming = field(default_factory=lambda: Naming(Case.snake, Number.singular))
    variable_list: Naming = field(default_factory=lambda: Naming(Case.snake, Number.plural))
    file_name: Naming = field(default_factory=lambda: Naming(Case.snake, Number.plural))
    client_attribute: Naming = field(default_factory=lambda: Naming(Case.snake, Number.plural))


@dataclass(frozen=True)
class APIsClassNaming:
    """
    Naming convention for a set of API classes created from a data model
    """

    name = Naming(Case.pascal, Number.singular)
    variable = Naming(Case.snake, Number.singular)


@dataclass(frozen=True)
class NamingConfig:
    """
    The configuration of the naming used by pygen.

    Args:
        data_class: The naming convention for the generated data classes.
    """

    field_: FieldNaming = field(default_factory=FieldNaming)
    data_class: DataClassNaming = field(default_factory=DataClassNaming)
    api_class: APIClassNaming = field(default_factory=APIClassNaming)
    apis_class: APIsClassNaming = field(default_factory=APIsClassNaming)


@dataclass(frozen=True)
class PygenConfig:
    naming: NamingConfig = field(default_factory=NamingConfig)
