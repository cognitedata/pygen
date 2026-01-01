import re


def sanitize(name: str) -> str:
    """Sanitize a string to be used as a file or directory name."""
    result = re.sub(r'[<>"/\\|?*]', "", name)
    result = re.sub(r"[: ]", "_", result)
    result = re.sub("_+", "_", result)

    original_len = len(result)
    result = result.rstrip(".")
    if len(result) != original_len:
        result += "_" * (original_len - len(result))

    return result
