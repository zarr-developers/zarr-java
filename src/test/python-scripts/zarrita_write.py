import sys

import zarrita
import numpy as np
from zarrita.metadata import ShardingCodecIndexLocation
from parse_codecs import parse_codecs_zarrita

codec_string = sys.argv[1]
param_string = sys.argv[2]
codec = parse_codecs_zarrita(codec_string, param_string)

store = zarrita.LocalStore(sys.argv[3])
testdata = np.arange(16 * 16 * 16, dtype='int32').reshape(16, 16, 16)

a = zarrita.Array.create(
    store / 'read_from_zarrita' / codec_string / param_string,
    shape=(16, 16, 16),
    chunk_shape=(2, 4, 8),
    dtype='int32',
    codecs=codec,
    attributes={'answer': 42}
)
a[:, :] = testdata
