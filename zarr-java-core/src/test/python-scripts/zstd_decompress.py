import sys

import zstandard as zstd

data_path = sys.argv[1]
expected = sys.argv[2]

with open(data_path, "rb") as f:
    compressed = f.read()

decompressed = zstd.ZstdDecompressor().decompress(compressed)
number = int.from_bytes(decompressed, byteorder='big')
assert number == int(expected)
