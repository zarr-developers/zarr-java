"""Shared zarr-python fixture operations used by the interop tests.

Every operation the Java interop suite asks of zarr-python lives here as a
function, so the one-shot CLI scripts and the long-lived worker
(``zarr_python_worker.py``) execute literally the same code. That matters: the
worker exists purely to avoid paying ``import zarr`` once per test case, and it
would be worthless if it could drift from the behaviour the CLI scripts had.

Each function either returns normally or raises. Assertion messages are
preserved verbatim from the original scripts so failure output is unchanged.
"""

from pathlib import Path

import numpy as np
import zarr
from zarr.storage import LocalStore

from parse_codecs import parse_codecs_zarr_python

SHAPE = (16, 16, 16)
CHUNKS = (2, 4, 8)


def _testdata_v3(dtype):
    if dtype == 'bool':
        return np.arange(16 * 16 * 16, dtype='uint8').reshape(16, 16, 16) % 2 == 0
    return np.arange(16 * 16 * 16, dtype=dtype).reshape(16, 16, 16)


def _testdata_v2(dtype):
    if 'b1' in dtype:
        return np.arange(16 * 16 * 16, dtype='uint8').reshape(16, 16, 16) % 2 == 0
    return np.arange(16 * 16 * 16, dtype=dtype).reshape(16, 16, 16)


def write_v3(codec_string, param_string, dtype, store_path):
    """Write a v3 array with zarr-python for zarr-java to read back."""
    compressor, serializer, filters = parse_codecs_zarr_python(codec_string, param_string)
    store_path = Path(store_path)
    testdata = _testdata_v3(dtype)

    a = zarr.create_array(
        LocalStore(store_path),
        shape=SHAPE,
        chunks=CHUNKS,
        dtype=dtype,
        filters=filters,
        serializer=serializer,
        compressors=compressor,
        attributes={'answer': 42}
    )
    a[:, :] = testdata


def read_v3(codec_string, param_string, dtype, store_path):
    """Verify with zarr-python that a v3 array zarr-java wrote is correct."""
    compressor, serializer, filters = parse_codecs_zarr_python(codec_string, param_string)
    store_path = Path(store_path)
    expected_data = _testdata_v3(dtype)

    a = zarr.open_array(store=LocalStore(store_path))
    read_data = a[:, :]
    assert np.array_equal(read_data, expected_data), \
        f"got:\n {read_data} \nbut expected:\n {expected_data}"

    b = zarr.create_array(
        LocalStore(store_path / "expected"),
        shape=SHAPE,
        chunks=CHUNKS,
        dtype=dtype,
        fill_value=0,
        filters=filters,
        serializer=serializer,
        compressors=compressor,
        attributes={'test_key': 'test_value'},
        overwrite=True,
    )

    assert a.metadata == b.metadata, f"not equal: \n{a.metadata=}\n{b.metadata=}"


def write_v2(codec_string, param_string, dtype, store_path):
    """Write a v2 array with zarr-python for zarr-java to read back."""
    compressor, serializer, filters = parse_codecs_zarr_python(
        codec_string, param_string, zarr_version=2)
    store_path = Path(store_path)
    testdata = _testdata_v2(dtype)

    a = zarr.create_array(
        LocalStore(store_path),
        zarr_format=2,
        shape=SHAPE,
        chunks=CHUNKS,
        dtype=dtype,
        filters=filters,
        serializer=serializer,
        compressors=compressor,
        attributes={'answer': 42}
    )
    a[:, :] = testdata


def read_v2(codec_string, param_string, dtype, store_path):
    """Verify with zarr-python that a v2 array zarr-java wrote is correct."""
    compressor, serializer, filters = parse_codecs_zarr_python(
        codec_string, param_string, zarr_version=2)
    store_path = Path(store_path)
    expected_data = _testdata_v2(dtype)

    a = zarr.open_array(store=LocalStore(store_path))
    read_data = a[:, :]
    assert np.array_equal(read_data, expected_data), \
        f"got:\n {read_data} \nbut expected:\n {expected_data}"

    b = zarr.create_array(
        LocalStore(store_path / "expected"),
        zarr_format=2,
        shape=SHAPE,
        chunks=CHUNKS,
        dtype=dtype,
        fill_value=0,
        filters=filters,
        serializer=serializer,
        compressors=compressor,
        attributes={'test_key': 'test_value'},
        overwrite=True,
    )

    assert a.metadata == b.metadata, f"not equal: \n{a.metadata=}\n{b.metadata=}"


def group(store_path_read, store_path_write, zarr_format):
    """Read a group zarr-java wrote, then write one for zarr-java to read."""
    store_path_read = Path(store_path_read)
    store_path_write = Path(store_path_write)
    zarr_format = int(zarr_format)
    assert zarr_format in (2, 3), f"unexpected zarr format: {zarr_format}"

    expected_data = np.arange(16 * 16 * 16, dtype='int32').reshape(16, 16, 16)

    g = zarr.open_group(store=LocalStore(store_path_read), zarr_format=zarr_format)
    assert g.attrs['attr'] == 'value'
    a = g['group']['array']
    read_data = a[:, :]
    assert np.array_equal(read_data, expected_data), \
        f"got:\n {read_data} \nbut expected:\n {expected_data}"

    g2 = zarr.create_group(store=LocalStore(store_path_write), zarr_format=zarr_format,
                           overwrite=True)
    g2.attrs['attr'] = 'value'
    a2 = g2.create_group('group2').create_array(
        name='array2',
        shape=SHAPE,
        chunks=CHUNKS,
        dtype="int32",
        fill_value=0,
    )
    a2[:] = expected_data


def zstd_decompress(data_path, expected):
    """Check that zarr-java's zstd output decompresses to the expected integer."""
    import zstandard as zstd

    with open(data_path, "rb") as f:
        compressed = f.read()

    decompressed = zstd.ZstdDecompressor().decompress(compressed)
    number = int.from_bytes(decompressed, byteorder='big')
    assert number == int(expected)


#: Operations the worker exposes, by name.
OPERATIONS = {
    "write_v3": write_v3,
    "read_v3": read_v3,
    "write_v2": write_v2,
    "read_v2": read_v2,
    "group": group,
    "zstd_decompress": zstd_decompress,
}
