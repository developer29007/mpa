def ms_to_nanos(ms: int):
    return ms * 1000_000


def nanos_to_ms(nanos: int):
    return nanos // 1000_000

def nanos_to_ms_str(nanos: int):
    total_millis = nanos_to_ms(nanos)
    remainder, mills = divmod(total_millis, 1000)
    hrs, remainder = divmod(remainder, 3600)
    mins, sec = divmod(remainder, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}.{mills:03d}"


def nanos_to_us(nanos: int):
    return nanos // 1000


def nanos_to_us_str(nanos: int):
    total_us = nanos // 1000
    total_ms, us = divmod(total_us, 1000)
    total_sec, ms = divmod(total_ms, 1000)
    hrs, remainder = divmod(total_sec, 3600)
    mins, sec = divmod(remainder, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}.{ms:03d},{us:03d}"

