import re


def sanitize(name: str) -> str:
    """Sanitize a string to be used as a file or directory name."""
    # Characters to remove completely (invalid in filenames)
    result = re.sub(r'[<>"/\\|?*]', '', name)
    # Characters to replace with underscore
    result = re.sub(r'[: ]', '_', result)

    # Handle trailing dots - strip all and add underscore + single dot
    if result.endswith('.'):
        result = result.rstrip('.') + '_.'

    return result
