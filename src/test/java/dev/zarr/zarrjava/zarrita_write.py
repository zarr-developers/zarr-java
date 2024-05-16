import sys

import zarrita
import numpy as np

codec_string = sys.argv[1]
codec = []
if codec_string == "blosc":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.blosc_codec(typesize=4)]
elif codec_string == "gzip":
    codecs = [zarrita.codecs.gzip_codec()], zarrita.codecs.bytes_codec()
elif codec_string == "zstd":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.zstd_codec()]
elif codec_string == "bytes":
    codec = [zarrita.codecs.bytes_codec()]
elif codec_string == "transpose":
    codec = [zarrita.codecs.transpose_codec("F"), zarrita.codecs.bytes_codec()]
elif codec_string == "sharding":
    codec = [zarrita.codecs.sharding_codec(chunk_shape=(1, 2), codecs=[zarrita.codecs.bytes_codec()])]
elif codec_string == "crc32c":
    codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.crc32c_codec()]
else:
    raise ValueError("Invalid codec")

store = zarrita.LocalStore(sys.argv[2])
testdata = np.arange(0, 16 * 16, dtype='int32').reshape((16, 16))

a = zarrita.Array.create(
    store / 'zarrita_write' / codec_string,
    shape=(16, 16),
    dtype='int32',
    chunk_shape=(2, 8),
    codecs=codec,
    attributes={'answer': 42}
)
a[:, :] = testdata
print(testdata)
