import sys
from pathlib import Path

import numpy as np

import zarr
from zarr.storage import LocalStore
from parse_codecs import parse_codecs_zarr_python

codec_string = sys.argv[1]
param_string = sys.argv[2]
compressor, serializer, filters = parse_codecs_zarr_python(codec_string, param_string)
store_path = Path(sys.argv[3])

expected_data = np.arange(16 * 16 * 16, dtype='int32').reshape(16, 16, 16)

a = zarr.open_array(store=LocalStore(store_path))
read_data = a[:, :]
assert np.array_equal(read_data, expected_data), f"got:\n {read_data} \nbut expected:\n {expected_data}"

b = zarr.create_array(
    LocalStore(store_path / "expected"),
    shape=(16, 16, 16),
    chunks=(2, 4, 8),
    dtype="uint32",
    fill_value=0,
    filters=filters,
    serializer=serializer,
    compressors=compressor,
    attributes={'test_key': 'test_value'},
)

assert a.metadata == b.metadata, f"not equal: \n{a.metadata=}\n{b.metadata=}"
