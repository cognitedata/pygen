"""
This module contains a configurable external id factory that can be used to create external ids for each domain class.

This is useful when you want to use pygen for ingesting data into CDF from JSON or another format that does not have
external ids. The external id factories can be set on the DomainModelWrite class and will be called when creating
instances of the DomainModelWrite class.

It is, however, recommended that you create your own external ids for each domain class, as this will make it easier
to create external ids which are unique and meaningful. This can be done by creating a custom function with the
following signature `def custom_factory(domain_cls: type, data: dict) -> str:` or by using the
create_external_id_factory function and passing in any custom configuration.

For example, if you are ingesting windmills, you could create an external id based on an existing windmill id field
you can write a custom factory function:

Example:
    ```python
    from cognite.pygen.utils.external_id_factories import uuid_factory, domain_name_factory, create_external_id_factory
    from windmill.client.data_classes import DomainModelWrite, WindmillWrite, RotorWrite

    def custom_factory_by_domain_class(domain_cls: type, data: dict) -> str:
        if domain_cls is WindmillWrite and "name" in data:
            return data["name"]
        elif domain_cls is RotorWrite and "name" in data:
            return data["name"]
        else:
            # Fallback to uuid factory
            return uuid_factory(domain_cls, data)

    # Finally, we can configure the new factory on the DomainModelWrite class a few different ways:

    # Using the custom factory directly:
    DomainModelWrite.external_id_factory = custom_factory_by_domain_class
    # Example input and output:
    windmill_with_name = WindmillWrite(name="Oslo 1")
    print(windmill_with_name.external_id)   # "Oslo 1"
    rotor_no_name = RotorWrite(rotor_speed_controller="time_series_external_id")
    print(rotor_no_name.external_id)   # "c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1"
    windmill_with_external_id = WindmillWrite(external_id="custom_external_id")
    print(windmill_with_name.external_id)   # "df323f497c749b1f0d0ce494f848404c4bf33e09262b787c293c5e472155aca9"


    # Using the create_external_id_factory function to have a prefix and suffix combination:
    DomainModelWrite.external_id_factory = create_external_id_factory(
        override_external_id=False,
        separator="_",
        prefix_ext_id_factory=domain_name_factory,
        suffix_ext_id_factory=custom_factory_by_domain_class,
    )
    # Example input and output:
    windmill_with_name = WindmillWrite(name="Oslo 1")
    print(windmill_with_name.external_id)   # "windmill_Oslo 1"
    rotor_no_name = RotorWrite(rotor_speed_controller="time_series_external_id")
    print(rotor_no_name.external_id)   # "rotor_c6c9d7497c9a7d180d7c8a5f88b01a5030260cc4bc833467fcc45dcfb1a4a4d1"
    windmill_with_external_id = WindmillWrite(external_id="custom_external_id")
    print(windmill_with_name.external_id)   # "custom_external_id"
    ```

The example above has created the SDK for the windmill data with the following configuration:

```toml
[tool.pygen]
top_level_package = "windmill.client"
client_name = "WindmillClient"
```

"""

import logging
import uuid as uuid_
from collections import defaultdict
from hashlib import sha256 as sha256_
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def shorten_string(input_str: str, length: int = 7) -> str:
    """
    This shortens the input string to the specified length.

    Args:
        input_str: The string to shorten
        length: The length to shorten the input string to

    Returns:
        The shortened string
    """
    return input_str[:length]


def domain_name(domain_cls: type, data: dict) -> str:
    """
    This creates an string based on the domain class name.

    Args:
        domain_cls: The domain class
        data: The data for the domain class instance

    Returns:
        A string based on the domain class name
    """
    return domain_cls.__name__.casefold().removesuffix("write")


def uuid(domain_cls: type, data: dict) -> str:
    """
    This creates a uuid.

    Args:
        domain_cls: The domain class
        data: The data for the domain class instance

    Returns:
        A uuid
    """
    return uuid_.uuid4().hex


def sha256(domain_cls: type, data: dict) -> str:
    """
    This creates a sha256 hash based on the data.

    Args:
        domain_cls: The domain class
        data: The data for the domain class instance

    Returns:
        A sha256 hash
    """
    return sha256_(str(data).encode()).hexdigest()


INCREMENTAL_ID_REGISTRY: dict[str, int] = defaultdict(int)


def incremental_id(domain_cls: type, data: dict) -> str:
    """
    This creates an incremental id for each domain class instance.
    WARNING: This is not recommended for production use due to potential conflicts in threads and external services
    potentially using the same incremental indexing.

    Args:
        domain_cls: The domain class
        data: The data for the domain class instance

    Returns:
        An incremental integer given the domain class
    """
    global INCREMENTAL_ID_REGISTRY
    domain_cls_name = domain_cls.__name__
    INCREMENTAL_ID_REGISTRY[domain_cls_name] += 1

    return str(INCREMENTAL_ID_REGISTRY[domain_cls_name])


class ExternalIdFactory:
    """
    This is a factory class that can be used to create external ids for domain class instances.
    """

    def __init__(self, factory: Callable[[type, dict], str], shorten_length: Optional[int] = None):
        self.factory = factory

        if shorten_length is not None and shorten_length <= 0:
            raise ValueError("Shorten length must be greater than 0 or None")
        else:
            self.shorten_length = shorten_length or 7

    def __call__(self, domain_cls: type, data: dict) -> str:
        return self.factory(domain_cls, data)

    def short(self, domain_cls: type, data: dict) -> str:
        return shorten_string(self.factory(domain_cls, data), self.shorten_length)


def domain_name_factory(shorten_length=None):
    return ExternalIdFactory(factory=domain_name, shorten_length=shorten_length)


def uuid_factory(shorten_length=None):
    return ExternalIdFactory(factory=uuid, shorten_length=shorten_length)


def sha256_factory(shorten_length=None):
    return ExternalIdFactory(factory=sha256, shorten_length=shorten_length)


def incremental_factory(shorten_length=None):
    return ExternalIdFactory(factory=incremental_id, shorten_length=shorten_length)


def create_external_id_factory(
    override_external_id: bool = True,
    separator: str = ":",
    prefix_ext_id_factory: ExternalIdFactory = domain_name_factory(),
    suffix_ext_id_factory: ExternalIdFactory = uuid_factory(),
) -> Callable[[type, dict], str]:
    """
    This creates an external id factory that can be set on the DomainModelWrite class provided a custom configuration.

    Args:
        override_external_id: If True, any existing external_id in the data will always be overridden by the factory.
        separator: The separator between the prefix and suffix, should be a single character.
        prefix_ext_id_factory: The function to create the prefix, defaults to the domain_name_factory.
        suffix_ext_id_factory: The function to create the suffix, defaults to the uuid_factory.

    Returns:
        A factory function that can be set on the DomainModelWrite class.
    """

    if len(separator) != 1:
        raise ValueError("Separator must be a single character")

    if prefix_ext_id_factory.shorten_length is not None:
        prefix_callable = prefix_ext_id_factory.short
    else:
        prefix_callable = prefix_ext_id_factory

    if suffix_ext_id_factory.shorten_length is not None:
        suffix_callable = suffix_ext_id_factory.short
    else:
        suffix_callable = suffix_ext_id_factory

    def factory(domain_cls: type, data: dict) -> str:
        return external_id_generator(
            domain_cls, data, override_external_id, separator, prefix_callable, suffix_callable
        )

    return factory


def external_id_generator(
    domain_cls: type,
    data: dict,
    override_external_id: bool = True,
    separator: str = ":",
    prefix_callable: Callable[[type, dict], str] = domain_name_factory(),
    suffix_callable: Callable[[type, dict], str] = uuid_factory(),
) -> str:
    """
    This creates an external id for each domain class instance provided a custom configuration.

    Args:
        domain_cls: The domain class
        data: The data for the domain class instance
        override_external_id: If True, any existing external_id in the data will always be overridden by the factory.
        separator: The separator between the prefix and suffix.
        prefix_callable: The function to create the prefix, defaults to the domain_name_factory.
        suffix_callable: The function to create the suffix, defaults to the uuid_factory.

    Returns:
        An external id for the domain class instance given the configuration following the format `prefix:suffix`
    """
    if override_external_id is False and "external_id" in data:
        return data["external_id"]

    return f"{prefix_callable(domain_cls, data)}{separator}{suffix_callable(domain_cls, data)}"
