import itertools

_counter = itertools.count(1)


def next_id() -> int:
    """Return the next globally unique message ID (monotonically increasing, starts at 1)."""
    return next(_counter)
