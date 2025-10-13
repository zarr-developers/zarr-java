import sys
from pathlib import Path

import numpy as np

import zarr

from zarr.storage import LocalStore
from parse_codecs import parse_codecs_zarr_python

codec_string = sys.argv[1]
param_string = sys.argv[2]
compressor, serializer, filters = parse_codecs_zarr_python(codec_string, param_string, zarr_version=2)
dtype = sys.argv[3]
store_path = Path(sys.argv[4])

if dtype == 'bool':
    expected_data = np.arange(16 * 16 * 16, dtype='uint8').reshape(16, 16, 16) % 2 == 0
else:
    expected_data = np.arange(16 * 16 * 16, dtype=dtype).reshape(16, 16, 16)

a = zarr.open_array(store=LocalStore(store_path))
read_data = a[:, :]
assert np.array_equal(read_data, expected_data), f"got:\n {read_data} \nbut expected:\n {expected_data}"

b = zarr.create_array(
    LocalStore(store_path / "expected"),
    zarr_format=2,
    shape=(16, 16, 16),
    chunks=(2, 4, 8),
    dtype=dtype,
    fill_value=0,
    filters=filters,
    serializer=serializer,
    compressors=compressor,
#     attributes={'test_key': 'test_value'},
)

assert a.metadata == b.metadata, f"not equal: \n{a.metadata=}\n{b.metadata=}"
