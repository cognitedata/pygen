import contextlib
import os
from collections.abc import Iterator
from pathlib import Path


@contextlib.contextmanager
def chdir(new_dir: Path) -> Iterator[None]:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = Path.cwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)


def _get_literal_str(text: str) -> str | None:
    """Finds the Literal in a string and returns it.

    Args:
        text: The string to search for the Literal.

    Returns:
        The Literal in the string, or None if not found.

    Examples:

        >>> _get_literal_str("Literal['hello']")
        "Literal['hello']"
        >>> _get_literal_str("str | int")

        >>> _get_literal_str("Literal['hello', 'world'] | int")
        "Literal['hello', 'world']"
        >>> _get_literal_str('None | Literal["hello", "world", "!"]')
        'Literal["hello", "world", "!"]'
        >>> _get_literal_str('Literal["incomplete')

    """
    if "Literal[" not in text:
        return None

    start = text.find("Literal[")
    end = text.find("]", start)

    if start == -1 or end == -1:
        return None

    return text[start : end + 1]
