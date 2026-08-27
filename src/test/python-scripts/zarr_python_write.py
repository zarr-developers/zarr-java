"""Write a v3 array with zarr-python. One-shot CLI wrapper.

The interop suite normally drives this through zarr_python_worker.py to avoid
paying interpreter startup per test case; this entry point is kept for running a
single case by hand:

    uv run src/test/python-scripts/zarr_python_write.py blosc blosclz_shuffle_3 int32 /tmp/store
"""

import sys

from zarr_fixtures import write_v3

write_v3(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
