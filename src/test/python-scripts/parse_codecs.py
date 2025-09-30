import zarr
from zarr.codecs.blosc import BloscCodec
from zarr.codecs.bytes import BytesCodec
from zarr.codecs.crc32c_ import Crc32cCodec
from zarr.codecs.gzip import GzipCodec
from zarr.codecs.sharding import ShardingCodec, ShardingCodecIndexLocation
from zarr.codecs.transpose import TransposeCodec
from zarr.codecs.zstd import ZstdCodec
import zarrita
import numcodecs

def parse_codecs_zarr_python(codec_string: str, param_string: str, zarr_version: int = 3):
    compressor = None
    filters = "auto"
    serializer = "auto"

    if codec_string == "blosc" and zarr_version == 3:
        cname, shuffle, clevel = param_string.split("_")
        compressor = BloscCodec(typesize=4, cname=cname, shuffle=shuffle, clevel=int(clevel))
    elif codec_string == "blosc" and zarr_version == 2:
        cname, shuffle, clevel = param_string.split("_")
        if shuffle == "noshuffle":
            shuffle = numcodecs.Blosc.NOSHUFFLE
        elif shuffle == "shuffle":
            shuffle = numcodecs.Blosc.SHUFFLE
        elif shuffle == "bitshuffle":
            shuffle = numcodecs.Blosc.BITSHUFFLE
        else:
            raise ValueError(f"Invalid shuffle: {shuffle}")
        compressor = numcodecs.Blosc(typesize=4, cname=cname, shuffle=shuffle, clevel=int(clevel))
    elif codec_string == "zlib" and zarr_version == 2:
        compressor = numcodecs.Zlib(level=int(param_string))
    elif codec_string == "gzip" and zarr_version == 3:
        compressor = GzipCodec(level=int(param_string))
    elif codec_string == "zstd" and zarr_version == 3:
        level, checksum = param_string.split("_")
        compressor = ZstdCodec(checksum=checksum == 'true', level=int(level))
    elif codec_string == "bytes" and zarr_version == 3:
        serializer = BytesCodec(endian=param_string.lower())
    elif codec_string == "transpose" and zarr_version == 3:
        filters = [TransposeCodec(order=(1, 0, 2))]
    elif codec_string == "sharding" and zarr_version == 3:
        serializer = ShardingCodec(chunk_shape=(2, 2, 4), codecs=(BytesCodec(endian="little"),),
                                              index_location=ShardingCodecIndexLocation.start if param_string == "start"
                                              else ShardingCodecIndexLocation.end)
    elif codec_string == "sharding_nested" and zarr_version == 3:
        serializer = ShardingCodec(chunk_shape=(2, 2, 4), codecs=(ShardingCodec(chunk_shape=(2, 1, 2),
                                              codecs=[BytesCodec(endian="little")]),))
    elif codec_string == "crc32c" and zarr_version == 3:
        compressor = Crc32cCodec()
    else:
        raise ValueError(f"Invalid codec: {codec_string}, zarr_version: {zarr_version}")

    return compressor, serializer, filters

def parse_codecs_zarrita(codec_string: str, param_string: str):
    codec = []
    if codec_string == "blosc":
        cname, shuffle, clevel = param_string.split("_")
        codec = [zarrita.codecs.bytes_codec(),
                 zarrita.codecs.blosc_codec(typesize=4, cname=cname, shuffle=shuffle, clevel=int(clevel))]
    elif codec_string == "gzip":
        codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.gzip_codec(level=int(param_string))]
    elif codec_string == "zstd":
        level, checksum = param_string.split("_")
        codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.zstd_codec(checksum=checksum == 'true', level=int(level))]
    elif codec_string == "bytes":
        codec = [zarrita.codecs.bytes_codec(endian=param_string.lower())]
    elif codec_string == "transpose":
        codec = [zarrita.codecs.transpose_codec((1, 0, 2)), zarrita.codecs.bytes_codec()]
    elif codec_string == "sharding":
        codec = zarrita.codecs.sharding_codec(chunk_shape=(2, 2, 4), codecs=[zarrita.codecs.bytes_codec("little")],
                                              index_location=zarrita.metadata.ShardingCodecIndexLocation.start if param_string == "start"
                                              else zarrita.metadata.ShardingCodecIndexLocation.end),
    elif codec_string == "sharding_nested":
        codec = zarrita.codecs.sharding_codec(chunk_shape=(2, 2, 4),
                                              codecs=[zarrita.codecs.sharding_codec(chunk_shape=(2, 1, 2), codecs=[
                                                  zarrita.codecs.bytes_codec("little")])]),
    elif codec_string == "crc32c":
        codec = [zarrita.codecs.bytes_codec(), zarrita.codecs.crc32c_codec()]
    else:
        raise ValueError(f"Invalid codec: {codec_string}")
    return codec
