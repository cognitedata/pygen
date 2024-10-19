"""
This module contains a configurable external id factory that can be used to create external ids for each domain class.

This is useful when you want to use pygen for ingesting data into CDF from JSON or another format that does not have
external ids. The external id factories can be set on the DomainModelWrite class and will be called when creating
instances of the DomainModelWrite class.

It is, however, recommended that you create your own external ids for each domain class, as this will make it easier
to create external ids which are unique and meaningful. This can be done by creating a custom function with the
following signature `def custom_factory(domain_cls: type, data: dict) -> str:` or using the ExternalIdFactory class.

For example, if you are ingesting windmills, you could create an external id based on an existing windmill field
and write a custom factory function utilizing that field:

Example:
    ```python
    import uuid as uuid_
    from cognite.pygen.utils.external_id_factories import ExternalIdFactory
    from windmill.client.data_classes import DomainModelWrite, WindmillWrite, RotorWrite

    fallback_factory = ExternalIdFactory.uuid_factory()

    def custom_factory_by_domain_class(domain_cls: type, data: dict) -> str:
        if domain_cls is WindmillWrite and "name" in data:
            return data["name"]
        elif domain_cls is RotorWrite and "name" in data:
            return data["name"]
        else:
            # Fallback to uuid factory
            return fallback_factory(domain_cls, data)

    # Finally, we can configure the new factory on the DomainModelWrite class a few different ways:

    # Using the custom factory directly:
    DomainModelWrite.external_id_factory = ExternalIdFactory(custom_factory_by_domain_class)
    # Example input and output:
    windmill_with_name = WindmillWrite(name="Oslo 1")
    assert windmill_with_name.external_id == "Oslo 1"
    rotor_no_name = RotorWrite(rotor_speed_controller="time_series_external_id")
    assert uuid_.UUID(rotor_no_name.external_id)
    windmill_with_external_id = WindmillWrite(external_id="custom_external_id")
    assert uuid_.UUID(windmill_with_external_id.external_id)


    # Using the create_external_id_factory function to have a prefix and suffix combination:
    DomainModelWrite.external_id_factory = ExternalIdFactory.create_external_id_factory(
        override_external_id=False,
        separator="_",
        prefix_ext_id_factory=ExternalIdFactory.domain_name_factory(),
        suffix_ext_id_factory=ExternalIdFactory(custom_factory_by_domain_class),
    )
    # Example input and output:
    windmill_with_name = WindmillWrite(name="Oslo 1")
    assert windmill_with_name.external_id == "windmill_Oslo 1"
    rotor_no_name = RotorWrite(rotor_speed_controller="time_series_external_id")
    assert rotor_no_name.external_id.startswith("rotor_")
    assert uuid_.UUID(rotor_no_name.external_id.removeprefix("rotor_"))
    windmill_with_external_id = WindmillWrite(external_id="custom_external_id")
    assert windmill_with_external_id.external_id == "custom_external_id"
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
import warnings
from collections import defaultdict
from collections.abc import Callable
from hashlib import sha256 as sha256_
from typing import Any, Optional

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

    # Note: Using mro instead of issubclass(domain_cls, DomainModelWrite) because DomainModelWrite is not available here
    if "DomainModelWrite" in [c.__name__ for c in domain_cls.__mro__]:
        return domain_cls.__name__.casefold().removesuffix("write")
    return domain_cls.__name__.casefold()


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


_INCREMENTAL_ID_REGISTRY: dict[str, int] = defaultdict(int)


def incremental_id(domain_cls: type, data: dict) -> str:
    """
    This creates an incremental id for each domain class instance.

    Note:
        warning: This is not recommended for production use due to potential conflicts in threads and external
                    services potentially using the same incremental indexing.

    Args:
        domain_cls: The domain class
        data: The data for the domain class instance

    Returns:
        An incremental integer given the domain class
    """
    global _INCREMENTAL_ID_REGISTRY
    domain_cls_name = domain_cls.__name__
    _INCREMENTAL_ID_REGISTRY[domain_cls_name] += 1

    return str(_INCREMENTAL_ID_REGISTRY[domain_cls_name])


class ExternalIdFactory:
    """
    This is a factory class that can be used to create external ids for domain class instances.
    """

    def __init__(self, factory: Callable[[type, dict], str], shorten_length: Optional[int] = None):
        self.factory = factory

        if shorten_length is not None and shorten_length <= 0:
            raise ValueError("Shorten length must be greater than 0 or None")
        else:
            self.shorten_length = shorten_length

    def __call__(self, domain_cls: type, data: dict) -> str:
        if self.shorten_length is None:
            return self.factory(domain_cls, data)
        else:
            return self.short(domain_cls, data)

    def short(self, domain_cls: type, data: dict) -> str:
        return shorten_string(self.factory(domain_cls, data), self.shorten_length or 7)

    @classmethod
    def domain_name_factory(self):
        """
        This creates a domain name external id factory instance of the ExternalIdFactory class.
        """
        return self(domain_name)

    @classmethod
    def uuid_factory(self):
        """
        This creates a uuid external id factory instance of the ExternalIdFactory class.
        """
        return self(uuid)

    @classmethod
    def sha256_factory(self):
        """
        This creates a sha256 hash external id factory instance of the ExternalIdFactory class.
        """
        return self(sha256)

    @classmethod
    def incremental_factory(self):
        """
        This creates an incremental external id factory instance of the ExternalIdFactory class.

        Note:
            warning: This is not recommended for production use due to potential conflicts in threads and external
                     services potentially using the same incremental indexing.
        """
        return self(incremental_id)

    @classmethod
    def create_external_id_factory(
        self,
        override_external_id: bool = True,
        separator: str = ":",
        prefix_ext_id_factory: Optional[Callable[[type, dict], str]] = None,
        suffix_ext_id_factory: Optional[Callable[[type, dict], str]] = None,
    ) -> Callable[[type, dict], str]:
        """
        This creates an external id factory with a prefix:suffix format that can be set on the DomainModelWrite
        class provided a custom configuration.

        Args:
            override_external_id: If True, any existing external_id in the data will always be overridden.
            separator: The separator between the prefix and suffix, should be a single character.
            prefix_ext_id_factory: The function to create the prefix, defaults to the domain_name_factory.
            suffix_ext_id_factory: The function to create the suffix, defaults to the uuid_factory.

        Returns:
            A factory function that can be set on the DomainModelWrite class.
        """

        if len(separator) != 1:
            raise ValueError("Separator must be a single character")

        if prefix_ext_id_factory is None:
            prefix_ext_id_factory = self.domain_name_factory()

        if suffix_ext_id_factory is None:
            suffix_ext_id_factory = self.uuid_factory()

        def factory(domain_cls: type, data: dict) -> str:
            return self.external_id_generator(
                domain_cls, data, override_external_id, separator, prefix_ext_id_factory, suffix_ext_id_factory
            )

        return self(factory)

    @staticmethod
    def external_id_generator(
        domain_cls: type,
        data: dict,
        override_external_id: bool = True,
        separator: str = ":",
        prefix_callable: Optional[Callable[[type, dict], str]] = None,
        suffix_callable: Optional[Callable[[type, dict], str]] = None,
    ) -> str:
        """
        This creates an external id for each domain class instance provided a custom configuration.

        Args:
            domain_cls: The domain class
            data: The data for the domain class instance
            override_external_id: If True, any existing external_id in the data will be overridden by the factory.
            separator: The separator between the prefix and suffix.
            prefix_callable: The function to create the prefix, defaults to the domain_name_factory.
            suffix_callable: The function to create the suffix, defaults to the uuid_factory.

        Returns:
            An external id for the domain class instance given the configuration following the format `prefix:suffix`
        """
        if prefix_callable is None:
            prefix_callable = ExternalIdFactory.domain_name_factory()

        if suffix_callable is None:
            suffix_callable = ExternalIdFactory.uuid_factory()

        if override_external_id is False and "external_id" in data:
            return data["external_id"]

        return f"{prefix_callable(domain_cls, data)}{separator}{suffix_callable(domain_cls, data)}"


def create_uuid_factory(shorten: bool = True) -> Callable[[type, dict], str]:
    """
    This creates a uuid external id factory.

    Note:
        deprecation warning: This function is deprecated and will be removed in v1.0. Use
                             ExternalIdFactory.create_external_id_factory(
                                suffix_ext_id_factory=ExternalIdFactory.uuid_factory()
                             ) instead.

    Args:
        shorten: If True, the external id will be shortened to 7 characters.

    Returns:
        A factory function that can be set on the DomainModelApply class.
    """
    warnings.warn(
        "create_uuid_factory is deprecated and will be removed in v1.0. "
        "Use ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory="
        f"ExternalIdFactory.uuid_factory(){'.short' if shorten else ''}) instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    def uuid_factory_wrapped(domain_cls: type, data: dict) -> str:
        """
        This creates an uuid external id for each domain class.
        Args:
            domain_cls:
            data:

        Returns:

        """
        prefix = domain_cls.__name__.casefold().removesuffix("write")
        uuid_str = str(uuid_.uuid4())
        if shorten:
            return f"{prefix}:{uuid_str[:7]}"
        else:
            return f"{prefix}:{uuid_str}"

    return uuid_factory_wrapped


def uuid_factory(domain_cls: type, data: dict) -> str:
    """
    This creates an uuid external id for each domain class.

    Note:
        deprecation warning: This function is deprecated and will be removed in v1.0. Use
                             ExternalIdFactory.create_external_id_factory(
                                suffix_ext_id_factory=ExternalIdFactory.uuid_factory()
                             ) instead.

    Args:
        domain_cls:
        data:

    Returns:

    """
    warnings.warn(
        "uuid_factory is deprecated and will be removed in v1.0. "
        "Use ExternalIdFactory.create_external_id_factory() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    return f"{domain_cls.__name__.casefold().removesuffix('write')}:{uuid_.uuid4()}"


def create_sha256_factory(shorten: bool = True) -> Callable[[type, dict], str]:
    """
    This creates a sha256 hash external id factory.

    Note:
        deprecation warning: This function is deprecated and will be removed in v1.0. Use
                             ExternalIdFactory.create_external_id_factory(
                                suffix_ext_id_factory=ExternalIdFactory.sha256_factory()
                             ) instead.

    Args:
        shorten: If True, the external id will be shortened to 7 characters.

    Returns:
        A factory function that can be set on the DomainModelApply class.
    """
    warnings.warn(
        "create_sha256_factory is deprecated and will be removed in v1.0. "
        "Use ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory="
        f"ExternalIdFactory.sha256_factory(){'.short' if shorten else ''}) instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    def sha256_factory_wrapped(domain_cls: type, data: dict) -> str:
        """
        This creates a sha256 hash external id for each domain class.

        Args:
            domain_cls: The domain class
            data: The data to create the external id from

        Returns:
            A sha256 hash of the data

        """
        prefix = domain_cls.__name__.casefold().removesuffix("write")
        hash_ = sha256_(str(data).encode()).hexdigest()
        if shorten:
            return f"{prefix}:{hash_[:7]}"
        else:
            return f"{prefix}:{hash_}"

    return sha256_factory_wrapped


def sha256_factory(domain_cls: type, data: dict) -> str:
    """
    This creates a sha256 hash external id for each domain class.

    Note:
        deprecation warning: This function is deprecated and will be removed in v1.0. Use
                             ExternalIdFactory.create_external_id_factory(
                                suffix_ext_id_factory=ExternalIdFactory.sha256_factory()
                             ) instead.

    Args:
        domain_cls: The domain class
        data: The data to create the external id from

    Returns:
        A sha256 hash of the data

    """
    warnings.warn(
        "sha256_factory is deprecated and will be removed in v1.0. "
        "Use ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory="
        "ExternalIdFactory.sha256_factory()) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return f"{domain_cls.__name__.casefold().removesuffix('write')}:{sha256_(str(data).encode()).hexdigest()}"


def create_incremental_factory() -> Callable[[type, dict], str]:
    """
    This creates a factory that will create incremental external ids for each domain class.

    Note:
        deprecation warning: This function is deprecated and will be removed in v1.0. Use
                             ExternalIdFactory.create_external_id_factory(
                                suffix_ext_id_factory=ExternalIdFactory.incremental_factory()
                             ) instead.
        warning: This is not recommended for production use due to potential conflicts in threads and external
                 services potentially using the same incremental indexing.

    Returns:
        A factory function that can be set on the DomainModelApply class.
    """
    warnings.warn(
        "create_incremental_factory is deprecated and will be removed in v1.0. "
        "Use ExternalIdFactory.create_external_id_factory(suffix_ext_id_factory="
        "ExternalIdFactory.incremental_factory()) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    registry: dict[str, int] = defaultdict(int)

    def incremental_factory(domain_cls: type, data: dict[str, Any]) -> str:
        registry[domain_cls.__name__] += 1
        return f"{domain_cls.__name__.casefold().removesuffix('write')}:{registry[domain_cls.__name__]}"

    return incremental_factory
