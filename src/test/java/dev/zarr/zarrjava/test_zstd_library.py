import struct
import sys

import zstandard as zstd

zstd_file = sys.argv[1]


def compress_data_to_file(file_path, integer_value):
    data = struct.pack('>i', integer_value)
    compressor = zstd.ZstdCompressor(level=0)
    compressed_data = compressor.compress(data)
    with open(file_path, 'wb') as file:
        file.write(compressed_data)


def decompress_zstd_file(file_path):
    with open(file_path, 'rb') as file:
        compressed_data = file.read()
    decompressor = zstd.ZstdDecompressor()  # is with FORMAT_ZSTD1

    return decompressor.decompress(compressed_data)


# for comparison
compress_data_to_file(zstd_file + "_", 42)

decompressed_data = decompress_zstd_file(zstd_file)
int_value = int.from_bytes(decompressed_data[:4], byteorder='big')
assert int_value == 42
