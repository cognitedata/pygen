import datetime

MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


def ms_to_datetime(ms: int | float) -> datetime.datetime:
    """Converts valid Cognite timestamps, i.e. milliseconds since epoch, to datetime object.

    Args:
        ms (int | float): Milliseconds since epoch.

    Raises:
        ValueError: On invalid Cognite timestamps.

    Returns:
        datetime: Aware datetime object in UTC.
    """
    if not (MIN_TIMESTAMP_MS <= ms <= MAX_TIMESTAMP_MS):
        raise ValueError(f"Input {ms=} does not satisfy: {MIN_TIMESTAMP_MS} <= ms <= {MAX_TIMESTAMP_MS}")

    # Note: We don't use fromtimestamp because it typically fails for negative values on Windows
    return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(milliseconds=ms)
