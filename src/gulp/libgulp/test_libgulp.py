#!/usr/bin/env python3
from muty.time import number_to_nanos_from_unix_epoch, string_to_nanos_from_unix_epoch

from gulp.libgulp import (
    c_ensure_iso8601,
    c_number_to_nanos_from_unix_epoch,
    c_string_to_nanos_from_unix_epoch,
)

if __name__ == "__main__":
    a = number_to_nanos_from_unix_epoch(7322110)
    b = c_number_to_nanos_from_unix_epoch(7322110)
    print(a, b)
    a = number_to_nanos_from_unix_epoch("7322110")
    b = c_number_to_nanos_from_unix_epoch("7322110")
    print(a, b)

    print(c_ensure_iso8601(
        "11111111"))

    print("1")
    print(c_string_to_nanos_from_unix_epoch(
        "2025-03-17T15:54:52Z"))
    print("2")
    print(c_ensure_iso8601(1742228852000))
    print("3")
    print(c_ensure_iso8601(
        11111111111111111111111111111111111111111111111111111111111111111111111111111111))
    print("4")
    b = c_ensure_iso8601("1742224128")
    print("5")
    b = c_string_to_nanos_from_unix_epoch(b)
    print("6")
    print(a, b)
