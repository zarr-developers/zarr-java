"""Regenerates the committed Zarr v2 ``order`` golden fixtures in testdata/golden/.

zarr-java's writer and reader shared the same ``order: "F"`` bug, so a Java-only
round-trip test agreed with itself and could never detect it. These fixtures are
written by zarr-python, the reference implementation, and are committed so that
both the read direction (open a known-correct store) and the write direction
(compare the bytes zarr-java produces against known-correct bytes) are covered
offline, with no Python needed at test time.

Run from the repository root after a zarr-python upgrade:

    uv run src/test/python-scripts/generate_v2_order_golden.py

The fixtures are deliberately tiny (a few hundred bytes in total) and use no
compressor, so the chunk files are the raw element bytes.
"""

import pathlib
import shutil

import numpy as np
import zarr

GOLDEN = pathlib.Path("testdata/golden")

# name -> (shape, chunks, order)
# int32 throughout: 4 bytes per element keeps the chunk files readable in a hex dump.
CASES = {
    # Minimal C/F pair. Same logical values, different chunk bytes.
    "v2_order_c": ((3, 4), (3, 4), "C"),
    "v2_order_f": ((3, 4), (3, 4), "F"),
    # Rank > 2, so that reversing all axes is exercised rather than a plain 2D swap.
    "v2_order_f_3d": ((2, 3, 4), (2, 3, 4), "F"),
    # Chunk shape divides neither dimension evenly, so the trailing chunks are
    # partially outside the array and get fill values.
    "v2_order_f_multichunk": ((5, 7), (2, 3), "F"),
}


def main() -> None:
    for name, (shape, chunks, order) in CASES.items():
        path = GOLDEN / name
        shutil.rmtree(path, ignore_errors=True)
        values = np.arange(int(np.prod(shape)), dtype="int32").reshape(shape)
        array = zarr.create_array(
            store=str(path),
            shape=shape,
            chunks=chunks,
            dtype="int32",
            zarr_format=2,
            order=order,
            compressors=None,
            fill_value=0,
        )
        array[...] = values
        assert np.array_equal(array[...], values), name
        # zarr-python always writes .zattrs; an empty one carries no information.
        zattrs = path / ".zattrs"
        if zattrs.is_file() and zattrs.read_text().strip() in ("{}", ""):
            zattrs.unlink()
        chunk_files = sorted(p.name for p in path.iterdir() if p.name != ".zarray")
        print(f"{name}: shape={shape} chunks={chunks} order={order} -> {chunk_files}")


if __name__ == "__main__":
    main()
