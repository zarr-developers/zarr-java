"""Verify a v3 array zarr-java wrote. One-shot CLI wrapper.

See zarr_python_write.py for why the suite prefers zarr_python_worker.py.

    uv run src/test/python-scripts/zarr_python_read.py blosc blosclz_shuffle_3 int32 /tmp/store
"""

import sys

from zarr_fixtures import read_v3

read_v3(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
