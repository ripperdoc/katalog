import datetime


def timestamp_to_utc(ts: float | None) -> datetime.datetime | None:
    if ts is None:
        return None
    return datetime.datetime.utcfromtimestamp(ts)