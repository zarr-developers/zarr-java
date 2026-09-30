import numpy as np
import sys
import zarr
from pathlib import Path
from zarr.storage import LocalStore

from parse_codecs import parse_codecs_zarr_python

codec_string = sys.argv[1]
param_string = sys.argv[2]
compressor, serializer, filters = parse_codecs_zarr_python(codec_string, param_string, zarr_version=2)
dtype = sys.argv[3]
store_path = Path(sys.argv[4])

if 'b1' in dtype:
    testdata = np.arange(16 * 16 * 16, dtype='uint8').reshape(16, 16, 16) % 2 == 0
else:
    testdata = np.arange(16 * 16 * 16, dtype=dtype).reshape(16, 16, 16)

a = zarr.create_array(
    LocalStore(store_path),
    zarr_format=2,
    shape=(16, 16, 16),
    chunks=(2, 4, 8),
    dtype=dtype,
    filters=filters,
    serializer=serializer,
    compressors=compressor,
    attributes={'answer': 42}
)
a[:, :] = testdata
