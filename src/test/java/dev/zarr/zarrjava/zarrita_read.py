import zarrita
import numpy as np
import sys


codec_string = sys.argv[1]
codec = []
if codec_string == "blosc":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.blosc_codec(typesize=4)]
elif codec_string == "gzip":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.gzip_codec()]
elif codec_string == "zstd":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.zstd_codec()]
elif codec_string == "bytes":
    codec = [zarrita.codecs.bytes_codec()]
elif codec_string == "transpose":
    codec = [zarrita.codecs.transpose_codec((1, 0)), zarrita.codecs.bytes_codec()]
elif codec_string == "sharding_start":
    codec= zarrita.codecs.sharding_codec(chunk_shape=(4, 4), codecs=[zarrita.codecs.bytes_codec("little")], index_location= zarrita.metadata.ShardingCodecIndexLocation.start),
elif codec_string == "sharding_end":
    codec= zarrita.codecs.sharding_codec(chunk_shape=(4, 4), codecs=[zarrita.codecs.bytes_codec("little")], index_location= zarrita.metadata.ShardingCodecIndexLocation.end),
elif codec_string == "crc32c":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.crc32c_codec()]
else:
    raise ValueError(f"Invalid {codec=}")


store = zarrita.LocalStore(sys.argv[2])
expected_data = np.arange(16*16, dtype='int32').reshape(16, 16)

a = zarrita.Array.open(store / 'write_to_zarrita' / codec_string)
read_data = a[:, :]
assert np.array_equal(read_data, expected_data), f"got:\n {read_data} \nbut expected:\n {expected_data}"

# might need to check individual properties
b = zarrita.Array.create(
    store / 'read_from_zarrita_expected' / codec_string,
    shape=(16, 16),
    chunk_shape=(8, 8),
    dtype="uint32",
    fill_value=0,
    codecs=codec
    )

assert a.metadata == b.metadata,  f"not equal: \n{a.metadata.codecs=}\n{b.metadata.codecs=}"