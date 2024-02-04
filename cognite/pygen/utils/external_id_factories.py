"""
This module contains a set of external id factories that can be used to create external ids for each domain class.

This is useful when you want to use pygen for ingesting data into CDF from JSON or another format that does not have
external ids. The external id factories can be set on the DomainModelApply class and will be called when creating
instances of the DomainModelApply class.

It is, however, recommended that you create your own external ids for each domain class, as this will make it easier
to create external ids which are unique and meaningful.

For example, if you are ingesting wells, you could create an external id based on an existing well id field
you can write a factory like this:

Example:
    ```python
    from cognite.pygen.utils.external_id_factories import uuid_factory
    from windmill.client.data_classes import DomainModelApply, WindmillApply

    def windmill_factory(domain_cls: type, data: dict) -> str:
        if domain_cls is WindmillApply:
            return data["name"]
        else:
            # Fallback to uuid factory
            return uuid_factory(domain_cls, data)

    # Finally, we set the new factory
    DomainModelApply.external_id_factory = windmill_factory
    ```

The example above has created the SDK for the windmill data with the following configuration:

```toml
[tool.pygen]
top_level_package = "windmill.client"
client_name = "WindmillClient"
```

"""

import uuid
from collections import defaultdict
from hashlib import sha256
from typing import Any, Callable


def create_uuid_factory(shorten: bool = True) -> Callable[[type, dict], str]:
    """
    This creates a uuid external id factory.

    Args:
        shorten: If True, the external id will be shortened to 7 characters.


    Returns:
        A factory function that can be set on the DomainModelApply class.
    """

    def uuid_factory_wrapped(domain_cls: type, data: dict) -> str:
        """
        This creates an uuid external id for each domain class.
        Args:
            domain_cls:
            data:

        Returns:

        """
        prefix = domain_cls.__name__.casefold().removesuffix("apply")
        uuid_ = str(uuid.uuid4())
        if shorten:
            return f"{prefix}:{uuid_[:7]}"
        else:
            return f"{prefix}:{uuid_}"

    return uuid_factory_wrapped


def uuid_factory(domain_cls: type, data: dict) -> str:
    """
    This creates an uuid external id for each domain class.
    Args:
        domain_cls:
        data:

    Returns:

    """
    return f"{domain_cls.__name__.casefold().removesuffix('apply')}:{uuid.uuid4()}"


def create_sha256_factory(shorten: bool = True) -> Callable[[type, dict], str]:
    """
    This creates a sha256 hash external id factory.

    Args:
        shorten: If True, the external id will be shortened to 7 characters.

    Returns:
        A factory function that can be set on the DomainModelApply class.
    """

    def sha256_factory_wrapped(domain_cls: type, data: dict) -> str:
        """
        This creates a sha256 hash external id for each domain class.

        Args:
            domain_cls: The domain class
            data: The data to create the external id from

        Returns:
            A sha256 hash of the data

        """
        prefix = domain_cls.__name__.casefold().removesuffix("apply")
        hash_ = sha256(str(data).encode()).hexdigest()
        if shorten:
            return f"{prefix}:{hash_[:7]}"
        else:
            return f"{prefix}:{hash_}"

    return sha256_factory_wrapped


def sha256_factory(domain_cls: type, data: dict) -> str:
    """
    This creates a sha256 hash external id for each domain class.

    Args:
        domain_cls: The domain class
        data: The data to create the external id from

    Returns:
        A sha256 hash of the data

    """
    return f"{domain_cls.__name__.casefold().removesuffix('apply')}:{sha256(str(data).encode()).hexdigest()}"


def create_incremental_factory() -> Callable[[type, dict], str]:
    """
    This creates a factory that will create incremental external ids for each domain class.

    Returns:
        A factory function that can be set on the DomainModelApply class.
    """
    registry: dict[str, int] = defaultdict(int)

    def incremental_factory(domain_cls: type, data: dict[str, Any]) -> str:
        registry[domain_cls.__name__] += 1
        return f"{domain_cls.__name__.casefold().removesuffix('apply')}:{registry[domain_cls.__name__]}"

    return incremental_factory
