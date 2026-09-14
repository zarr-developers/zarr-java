"""Live zarr-python oracle for the Zarr v2 ``order`` key.

Usage:
    zarr_python_order_v2.py write <path> <shape> <chunks> <order>
    zarr_python_order_v2.py read  <path> <shape> <chunks> <order>

``shape`` and ``chunks`` are comma-separated, e.g. ``5,7,3``. ``order`` is ``C`` or ``F``.

``write`` creates a store holding ``arange(prod(shape))`` for zarr-java to read.
``read`` opens a store written by zarr-java and asserts it holds exactly that, so a
wrong layout on the Java side fails here with a non-zero exit code.

Both directions are needed: zarr-java's reader and writer shared the same ``order``
bug, so they agreed with each other and a Java-only round-trip stayed green.
"""

import sys

import numpy as np
import zarr


def expected(shape):
    return np.arange(int(np.prod(shape)), dtype="int32").reshape(shape)


def main():
    mode, path, shape_arg, chunks_arg, order = sys.argv[1:6]
    shape = tuple(int(s) for s in shape_arg.split(","))
    chunks = tuple(int(c) for c in chunks_arg.split(","))
    values = expected(shape)

    if mode == "write":
        array = zarr.create_array(
            store=path,
            shape=shape,
            chunks=chunks,
            dtype="int32",
            zarr_format=2,
            order=order,
            compressors=None,
            fill_value=0,
        )
        array[...] = values
        print(f"wrote {path} shape={shape} chunks={chunks} order={order}")
    elif mode == "read":
        array = zarr.open_array(path, mode="r", zarr_format=2)
        assert array.metadata.order == order, (
            f"expected order {order!r} in .zarray, found {array.metadata.order!r}"
        )
        assert array.shape == shape, f"expected shape {shape}, found {array.shape}"
        actual = array[...]
        if not np.array_equal(actual, values):
            raise AssertionError(
                f"zarr-python read {path} (order={order}) incorrectly.\n"
                f"expected:\n{values}\nactual:\n{actual}"
            )
        print(f"verified {path} shape={shape} chunks={chunks} order={order}")
    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
