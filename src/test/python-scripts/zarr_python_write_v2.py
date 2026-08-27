"""Write a v2 array with zarr-python. One-shot CLI wrapper.

See zarr_python_write.py for why the suite prefers zarr_python_worker.py.

    uv run src/test/python-scripts/zarr_python_write_v2.py zlib 0 '<i4' /tmp/store
"""

import sys

from zarr_fixtures import write_v2

write_v2(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
