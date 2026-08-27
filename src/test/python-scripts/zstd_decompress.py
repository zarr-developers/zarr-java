"""Check zarr-java's zstd output with the reference library. One-shot CLI wrapper.

See zarr_python_write.py for why the suite prefers zarr_python_worker.py.

    uv run src/test/python-scripts/zstd_decompress.py /tmp/compressed.bin 123456
"""

import sys

from zarr_fixtures import zstd_decompress

zstd_decompress(sys.argv[1], sys.argv[2])
