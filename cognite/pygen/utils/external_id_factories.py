"""
This module contains a set of external id factories that can be used to create external ids for each domain class.

This is useful when you want to use pygen for ingesting data into CDF from JSON or another format that does not have
external ids. The external id factories can be set on the DomainModelApply class and will be called when creating
instances of the DomainModelApply class.

It is, however, recommended that you create your own external ids for each domain class, as this will make it easier
to create external ids which are unique and meaningful.

# Todo Add example of how to create external ids for each domain class.
Example:
    ```python
    from pygen import DomainModelApply
    from pygen.utils.external_id_factories import uuid_factory
    DomainModelApply.external_id_factory = uuid_factory

    ```

"""
import uuid
from collections import defaultdict
from hashlib import sha256
from typing import Any, Callable


def uuid_factory(domain_cls: type, data: dict) -> str:
    """
    This creates an uuid external id for each domain class.
    Args:
        domain_cls:
        data:

    Returns:

    """
    return f"{domain_cls.__name__.casefold()}:{uuid.uuid4()}"


def sha256_factory(domain_cls: type, data: dict) -> str:
    """
    This creates a sha256 hash external id for each domain class.

    Args:
        domain_cls: The domain class
        data: The data to create the external id from

    Returns:
        A sha256 hash of the data

    """
    return f"{domain_cls.__name__.casefold()}:{sha256(str(data).encode()).hexdigest()}"


def create_incremental_factory() -> Callable[[type, dict], str]:
    """
    This creates a factory that will create incremental external ids for each domain class.

    Returns:
        A factory function that can be set on the DomainModelApply class.
    """
    registry: dict[str, int] = defaultdict(int)

    def incremental_factory(domain_cls: type, data: dict[str, Any]) -> str:
        registry[domain_cls.__name__] += 1
        return f"{domain_cls.__name__.casefold()}:{registry[domain_cls.__name__]}"

    return incremental_factory
