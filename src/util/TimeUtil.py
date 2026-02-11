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

