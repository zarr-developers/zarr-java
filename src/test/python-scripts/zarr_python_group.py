"""Round-trip a group through zarr-python. One-shot CLI wrapper.

See zarr_python_write.py for why the suite prefers zarr_python_worker.py.

    uv run src/test/python-scripts/zarr_python_group.py /tmp/written /tmp/toread 3
"""

import sys

from zarr_fixtures import group

group(sys.argv[1], sys.argv[2], sys.argv[3])
