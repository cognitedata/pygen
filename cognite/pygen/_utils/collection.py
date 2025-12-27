from collections.abc import Collection, Iterator, Sequence
from typing import Any, TypeVar

T_Sequence = TypeVar("T_Sequence", bound=Sequence)


def chunker_sequence(sequence: T_Sequence, size: int) -> Iterator[T_Sequence]:
    """Yield successive n-sized chunks from sequence."""
    for i in range(0, len(sequence), size):
        # MyPy does not expect sequence[i : i + size] to be of type T_Sequence
        yield sequence[i : i + size]  # type: ignore[misc]


def humanize_collection(collection: Collection[Any], /, *, sort: bool = True, bind_word: str = "and") -> str:
    """Convert a collection of items to a human-readable string.

    Args:
        collection: The collection of items to convert.
        sort: Whether to sort the collection before converting. Default is True.
        bind_word: The word to use to bind the last two items. Default is "and".

    Returns:
        A human-readable string of the collection.

    Examples:
        >>> humanize_collection(["b", "c", "a"])
        'a, b and c'
        >>> humanize_collection(["b", "c", "a"], sort=False)
        'b, c and a'
        >>> humanize_collection(["a", "b"])
        'a and b'
        >>> humanize_collection(["a"])
        'a'
        >>> humanize_collection([])
        ''

    """
    if not collection:
        return ""
    elif len(collection) == 1:
        return str(next(iter(collection)))

    strings = (str(item) for item in collection)
    if sort:
        sequence = sorted(strings)
    else:
        sequence = list(strings)

    return f"{', '.join(sequence[:-1])} {bind_word} {sequence[-1]}"
