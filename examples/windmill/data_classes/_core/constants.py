DEFAULT_QUERY_LIMIT = 5
INSTANCE_QUERY_LIMIT = 1_000
# This is the actual limit of the API, we typically set it to a lower value to avoid hitting the limit.
ACTUAL_INSTANCE_QUERY_LIMIT = 10_000
DEFAULT_INSTANCE_SPACE = "windmill-instances"


class _NotSetSentinel:
    """This is a special class that indicates that a value has not been set.
    It is used when we need to distinguish between not set and None."""

    ...
