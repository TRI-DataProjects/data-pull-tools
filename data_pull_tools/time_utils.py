from datetime import datetime, timezone

# TODO: Cache tz for some time (ttl)


def system_time_zone() -> timezone:
    tz = datetime.now(timezone.utc).astimezone().tzinfo
    if tz is None:
        tz = timezone.utc
    return tz  # type: ignore


def system_now() -> datetime:
    return datetime.now(system_time_zone())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


if __name__ == "__main__":
    print(system_now())
    print(system_now().tzinfo)
