import re


def sanitize(name: str) -> str:
    """Sanitize a string to be used as a file or directory name."""
    result = re.sub(r'[<>"/\\|?*]', "", name)
    result = re.sub(r"[: ]", "_", result)
    result = re.sub("_+", "_", result)

    if result.endswith("."):
        result = result.rstrip(".") + "_."

    return result
