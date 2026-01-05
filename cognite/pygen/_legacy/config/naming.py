from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import Enum


class Case(Enum):
    """
    Available case conventions.

    For example, if we have the name `work order, we have the following options:

     * pascal ➔ "WorkOrder"
     * snake ➔ "work_order"
     * camel ➔ "workOrder"

    Attributes:
        pascal: Pascal case.
        snake: Snake case.
        camel: Camel case.
    """

    pascal = "pascal"
    snake = "snake"
    camel = "camel"
    human = "human"


class Number(Enum):
    """
    Available number conventions. The number convention is used to determine if a name should be
    singular or plural.

    For example, if we have the name "asset", we have the following options:

     * plural ➔ "assets"
     * singular ➔ "asset"
     * unchanged ➔ "asset"

    Attributes:
        plural: The name should be pluralized.
        singular: The name should be singularized.
        unchanged: The name should remain unchanged.

    """

    plural = "plural"
    singular = "singular"
    unchanged = "unchanged"


@dataclass(frozen=True)
class Naming:
    """
    Naming convention for a class or variable.

    A naming convention is a combination of a case and a number convention.

    Args:
        case: Case convention.
        number: Number convention.
    """

    case: Case
    number: Number = Number.unchanged


@dataclass(frozen=True)
class FieldNaming:
    """
    Naming convention for fields.

    Args:
        name: Naming convention for data class fields. Data class fields are the attributes of the
                data classes and are generated from the properties of the view.
        variable: Naming convention for data class field variables. This is used, for example, in the
            internals implementation of the API class.
        edge_api_class: Naming convention for edge API class names. The edge API class name will be suffixed with "API".
        api_class_attribute: Naming convention for edge API class attributes.

    """

    name: Naming = Naming(Case.snake, Number.unchanged)
    variable: Naming = Naming(Case.snake, Number.singular)
    edge_api_file: Naming = Naming(Case.snake, Number.unchanged)
    edge_api_class: Naming = Naming(Case.pascal, Number.unchanged)
    api_class_attribute: Naming = Naming(Case.snake, Number.unchanged)


@dataclass(frozen=True)
class DataClassNaming:
    """
    Naming convention for data classes.

    Args:
        name: Naming convention for data class names. Pygen will create two data classes one for reading
            and one for writing. The writing class will be suffixed with "Apply". In addition, pygen will
            also create a list class, suffixed with "List".
        file: Naming convention for data class file. This will be prefixed with an underscore.
        variable: Naming convention for data class variable. This is used, for example, in as the
            parameter name in the apply method of the API class.
        variable_list: Naming convention for data class variable list. This is used, for example,
            in the internals implementation of the API class.

    Examples:

    If we have a data model with two views, with name "My view" and "My other view",
    we will generate two API classes, with name as singular pascal case. The generated data classes
    will be available:

    ```python
    from cognite.my_client.data_class emport MyViewApply, MyOtherViewApply
    ```
    """

    name: Naming = Naming(Case.pascal, Number.unchanged)
    variable: Naming = Naming(Case.snake, Number.singular)
    variable_list: Naming = Naming(Case.snake, Number.plural)
    file: Naming = Naming(Case.snake, Number.unchanged)


@dataclass(frozen=True)
class APIClassNaming:
    """
    Naming convention for API classes.

    Args:
        name: Naming convention for API class names. The api class name will be suffixed with "API".
        file_name: Naming convention for the file name of the API class.
        client_attribute: Naming convention for the client attribute.
        variable: Naming convention for the API class variable.

    Examples:

    If we have a data model with two views, "MyView" and "MyOtherView", we will generate two API classes,
    with client attribute as plural snake case, and name as pascal case plural. The generated client
    will then get the following attributes:

    ```python
    from cognite import pygen
    my_client = pygen.generate_sdk_notebook(...)
    # my_client.[client_attribute].list()/.retrieve()/.apply/.delete()
    my_client.my_views.list()

    # and
    my_client.my_other_views.list()
    ```

    """

    name: Naming = Naming(Case.pascal, Number.unchanged)
    file_name: Naming = Naming(Case.snake, Number.unchanged)
    client_attribute: Naming = Naming(Case.snake, Number.unchanged)
    variable: Naming = Naming(Case.snake, Number.singular)
    doc_name: Naming = Naming(Case.human, Number.singular)


@dataclass(frozen=True)
class MultiAPIClassNaming:
    """
    Naming convention for a set of API classes created from a data model.

    Args:
        client_attribute: Naming convention for the client attribute.
        name: Naming convention for the API class names. The api class name will be suffixed with "APIs".

    Examples:

    If we have a data model with two views, "MyView" and "MyOtherView", we will generate two API classes,
    with client attribute as plural snake case, and name as pascal case plural. The generated client
    will then get the following attributes:

    ```python
    from cognite import pygen
    my_client = pygen.generate_sdk_notebook(...)
    # my_client.[client_attribute].list()/.retrieve()/.apply/.delete()
    my_client.my_views.list()

    # and
    my_client.my_other_views.list()
    ```

    """

    client_attribute: Naming = Naming(Case.snake, Number.unchanged)
    name: Naming = Naming(Case.pascal, Number.singular)


@dataclass
class NamingConfig:
    """
    The configuration of the naming used by pygen.

    Args:
        field: The naming convention for the generated fields used in the data classes.
        data_class: The naming convention for the generated data classes.
        api_class: The naming convention for the generated API classes.
        multi_api_class: The naming convention for the generated multi API classes (Used when generating
            an SDK for multiple data models).
    """

    field: FieldNaming = dataclass_field(default_factory=FieldNaming)
    data_class: DataClassNaming = dataclass_field(default_factory=DataClassNaming)
    api_class: APIClassNaming = dataclass_field(default_factory=APIClassNaming)
    multi_api_class: MultiAPIClassNaming = dataclass_field(default_factory=MultiAPIClassNaming)
