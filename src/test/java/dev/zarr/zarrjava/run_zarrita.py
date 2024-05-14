import zarrita
import numpy as np

store = zarrita.LocalStore('testoutput')

testdata = np.arange(0, 16 * 16, dtype='int32').reshape((16, 16))

a = zarrita.Array.create(
    store / 'array',
    shape=(16, 16),
    dtype='int32',
    chunk_shape=(2, 8),
    codecs=[
        zarrita.codecs.bytes_codec(),
        zarrita.codecs.blosc_codec(typesize=4),
    ],
    attributes={'answer': 42}
)
a[:, :] = testdata
print(testdata)