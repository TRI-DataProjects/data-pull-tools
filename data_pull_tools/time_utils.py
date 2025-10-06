from datetime import UTC, datetime, timezone

# TODO: Cache tz for some time (ttl)


def system_time_zone() -> timezone:
    tz: timezone = datetime.now(UTC).astimezone().tzinfo  # pyright: ignore[reportAssignmentType]
    if tz is None:
        tz = UTC
    return tz


def system_now() -> datetime:
    return datetime.now(system_time_zone())


def utc_now() -> datetime:
    return datetime.now(UTC)


if __name__ == "__main__":
    print(system_now())
    print(system_now().tzinfo)
