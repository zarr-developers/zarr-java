import sys
from pathlib import Path

import numpy as np
import zarr
from zarr.storage import LocalStore

store_path_read = Path(sys.argv[1])
store_path_write = Path(sys.argv[2])

expected_members = ["arr", "sub", "sub/deep", "sub/deep/deepArray", "sub/nested"]

# Read a hierarchy that zarr-java consolidated.
g = zarr.open_group(store=LocalStore(store_path_read), zarr_format=3, use_consolidated=True)
consolidated = g.metadata.consolidated_metadata
assert consolidated is not None, "zarr-python did not pick up the consolidated metadata"
# zarr-python re-nests the flat keys when it reads them, so the top level only holds the direct
# children and the rest lives in the caches it builds for the subgroups.
assert list(consolidated.metadata.keys()) == ["arr", "sub"], list(consolidated.metadata.keys())
members = sorted(k for k, _ in g.members(max_depth=None))
assert members == expected_members, f"got {members}, expected {expected_members}"
assert g["arr"].shape == (64, 64), g["arr"].shape
assert g["sub"]["deep"]["deepArray"].shape == (8, 8)

# Write a hierarchy of the same shape and consolidate it, for zarr-java to read.
g2 = zarr.create_group(store=LocalStore(store_path_write), zarr_format=3)
g2.attrs["attr"] = "value"
arr = g2.create_array(name="arr", shape=(64, 64), chunks=(8, 8), dtype="uint8", fill_value=0)
arr[:] = np.arange(64 * 64, dtype="uint8").reshape(64, 64)
sub = g2.create_group("sub")
sub.create_array(name="nested", shape=(8, 8), chunks=(8, 8), dtype="uint8", fill_value=0)
deep = sub.create_group("deep")
deep.create_array(name="deepArray", shape=(8, 8), chunks=(8, 8), dtype="uint8", fill_value=0)
zarr.consolidate_metadata(g2.store)
