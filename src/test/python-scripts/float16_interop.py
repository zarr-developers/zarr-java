"""Cross-implementation check for the float16 data type.

Usage:
    float16_interop.py write  <store_path> <zarr_format> <endian>
    float16_interop.py verify <store_path> <zarr_format>

`write` produces a float16 array for zarr-java to read; `verify` reads an array
zarr-java produced and asserts it holds the same values. Both directions are needed:
a zarr-java round-trip cannot catch a symmetric encode/decode bug.

Every value below is exactly representable in binary16, so the comparison is exact
and never depends on rounding.
"""

import sys

import numpy as np
import zarr
from zarr.storage import LocalStore

# Ordered edge cases: signed zeros, min/max subnormal, min normal, max finite,
# infinities, NaN, and a few rounded values.
VALUES = np.array(
    [
        0.0,
        -0.0,
        1.0,
        -2.0,
        5.960464477539063e-08,  # smallest positive subnormal, 2**-24
        6.097555160522461e-05,  # largest subnormal
        6.103515625e-05,  # smallest positive normal, 2**-14
        65504.0,  # largest finite
        -65504.0,
        np.inf,
        -np.inf,
        np.nan,
        0.333251953125,  # 1/3 rounded to binary16
        -0.0999755859375,  # 0.1 rounded to binary16, negated
    ],
    dtype=np.float16,
)

SHAPE = (len(VALUES),)
CHUNKS = (5,)  # Deliberately not a divisor of 14, so the last chunk is partial.


def assert_matches(actual):
    actual = np.asarray(actual, dtype=np.float16)
    assert actual.shape == SHAPE, f"shape {actual.shape} != {SHAPE}"
    for i, (got, want) in enumerate(zip(actual, VALUES)):
        if np.isnan(want):
            assert np.isnan(got), f"index {i}: got {got!r}, expected NaN"
        else:
            # Exact bit comparison, so that +0.0 and -0.0 are told apart.
            got_bits = np.frombuffer(np.float16(got).tobytes(), dtype=np.uint16)[0]
            want_bits = np.frombuffer(np.float16(want).tobytes(), dtype=np.uint16)[0]
            assert got_bits == want_bits, (
                f"index {i}: got {got!r} (0x{got_bits:04X}), "
                f"expected {want!r} (0x{want_bits:04X})"
            )


def main():
    mode = sys.argv[1]
    store_path = sys.argv[2]
    zarr_format = int(sys.argv[3])

    if mode == "write":
        endian = sys.argv[4]
        dtype = ">f2" if endian == "BIG" else "<f2"
        kwargs = {}
        if zarr_format == 3:
            # zarr v3 expresses endianness through the bytes codec, not the dtype string.
            from zarr.codecs import BytesCodec

            kwargs["serializer"] = BytesCodec(endian=endian.lower())
            dtype = "float16"
        a = zarr.create_array(
            LocalStore(store_path),
            shape=SHAPE,
            chunks=CHUNKS,
            dtype=dtype,
            fill_value=np.float16(0.0),
            compressors=None,
            zarr_format=zarr_format,
            **kwargs,
        )
        a[:] = VALUES
        print(f"wrote float16 v{zarr_format} {endian} to {store_path}")

    elif mode == "verify":
        a = zarr.open_array(store=LocalStore(store_path), mode="r")
        # Compare kind and width rather than the dtype object: zarr v2 encodes byte order in the
        # dtype string, so a big-endian array is ">f2", which is not == to native "float16".
        assert a.dtype.kind == "f" and a.dtype.itemsize == 2, (
            f"dtype is {a.dtype}, expected a 2-byte float"
        )
        assert_matches(a[:])
        print(f"verified float16 v{zarr_format} at {store_path}")

    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
