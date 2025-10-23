import sys
from pathlib import Path

import numpy as np

import zarr
from zarr.storage import LocalStore

store_path_read = Path(sys.argv[1])

expected_data = np.arange(16 * 16 * 16, dtype='int32').reshape(16, 16, 16)

g = zarr.open_group(store=LocalStore(store_path_read), zarr_format=2)
a = g['group']['array']
read_data = a[:, :]
assert np.array_equal(read_data, expected_data), f"got:\n {read_data} \nbut expected:\n {expected_data}"

store_path_write = Path(sys.argv[2])
g2 = zarr.create_group(store=LocalStore(store_path_write), zarr_format=2)
a2 = g2.create_group('group2').create_array(
    name='array2',
    shape=(16, 16, 16),
    chunks=(2, 4, 8),
    dtype="int32",
    fill_value=0,
)
a2[:] = expected_data