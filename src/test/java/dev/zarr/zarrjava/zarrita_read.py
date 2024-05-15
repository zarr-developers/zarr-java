import zarrita
import numpy as np

store = zarrita.LocalStore('testoutput')
expected_data = np.zeros((16, 16), dtype='int32')
expected_data[0, 10] = 42

a = zarrita.Array.open(store / 'array')
assert np.array_equal(a[:, :], expected_data)


# might need to check individual properties
b = zarrita.Array.create(
    store / "b",
    shape=(16, 16),
    chunk_shape=(16, 16),
    dtype="uint32",
    fill_value=0,
    codecs=[
        zarrita.codecs.bytes_codec(),
    ],
    )

assert a.metadata == b.metadata

print(a.metadata)