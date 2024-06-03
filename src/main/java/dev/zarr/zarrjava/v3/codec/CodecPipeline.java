package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata.CoreArrayMetadata;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Nonnull;
import ucar.ma2.Array;

public class CodecPipeline {

  @Nonnull
  final Codec[] codecs;
  public final CoreArrayMetadata arrayMetadata;

  public CodecPipeline(@Nonnull Codec[] codecs, CoreArrayMetadata arrayMetadata) throws ZarrException {
    this.arrayMetadata = arrayMetadata;
    long arrayBytesCodecCount = Arrays.stream(codecs).filter(c -> c instanceof ArrayBytesCodec)
        .count();
    if (arrayBytesCodecCount != 1) {
      throw new ZarrException(
          "Exactly 1 ArrayBytesCodec is required. Found " + arrayBytesCodecCount + ".");
    }
    Codec prevCodec = null;
    CoreArrayMetadata codecArrayMetadata = arrayMetadata;
    for (Codec codec : codecs) {
      if (prevCodec != null) {
        if (codec instanceof ArrayBytesCodec && prevCodec instanceof ArrayBytesCodec) {
          throw new ZarrException(
              "ArrayBytesCodec '" + codec.getClass() + "' cannot follow after ArrayBytesCodec '" +
                  prevCodec.getClass() + "' because only 1 ArrayBytesCodec is allowed.");
        }
        if (codec instanceof ArrayBytesCodec && prevCodec instanceof BytesBytesCodec) {
          throw new ZarrException(
              "ArrayBytesCodec '" + codec.getClass() + "' cannot follow after BytesBytesCodec '" +
                  prevCodec.getClass() + "'.");
        }
        if (codec instanceof ArrayArrayCodec && prevCodec instanceof ArrayBytesCodec) {
          throw new ZarrException(
              "ArrayArrayCodec '" + codec.getClass() + "' cannot follow after ArrayBytesCodec '" +
                  prevCodec.getClass() + "'.");
        }
        if (codec instanceof ArrayArrayCodec && prevCodec instanceof BytesBytesCodec) {
          throw new ZarrException(
              "ArrayArrayCodec '" + codec.getClass() + "' cannot follow after BytesBytesCodec '" +
                  prevCodec.getClass() + "'.");
        }
      }
      codec.setCoreArrayMetadata(codecArrayMetadata);
      codecArrayMetadata = codec.resolveArrayMetadata();
      prevCodec = codec;
    }

    this.codecs = codecs;
  }

  ArrayArrayCodec[] getArrayArrayCodecs() {
    return Arrays.stream(codecs)
        .filter(c -> c instanceof ArrayArrayCodec)
        .toArray(ArrayArrayCodec[]::new);
  }

  ArrayBytesCodec getArrayBytesCodec() {
    for (Codec codec : codecs) {
      if (codec instanceof ArrayBytesCodec) {
        return (ArrayBytesCodec) codec;
      }
    }
    throw new RuntimeException(
        "Unreachable because the existence of exactly 1 ArrayBytes codec is asserted upon construction.");
  }

  BytesBytesCodec[] getBytesBytesCodecs() {
    return Arrays.stream(codecs)
        .filter(c -> c instanceof BytesBytesCodec)
        .toArray(BytesBytesCodec[]::new);
  }

  public boolean supportsPartialDecode() {
    return codecs.length == 1 && codecs[0] instanceof ArrayBytesCodec.WithPartialDecode;
  }

  @Nonnull
  public Array decodePartial(
      @Nonnull StoreHandle storeHandle,
      long[] offset, int[] shape
  ) throws ZarrException {
    if (!supportsPartialDecode()) {
      throw new ZarrException(
          "Partial decode is not supported for these codecs. " + Arrays.toString(codecs));
    }
    Array chunkArray = ((ArrayBytesCodec.WithPartialDecode) getArrayBytesCodec()).decodePartial(
        storeHandle, offset, shape);
    if (chunkArray == null) {
      throw new ZarrException("chunkArray is null. This is likely a bug in one of the codecs.");
    }
    return chunkArray;
  }

  @Nonnull
  public Array decode(
      @Nonnull ByteBuffer chunkBytes
  ) throws ZarrException {
    if (chunkBytes == null) {
      throw new ZarrException("chunkBytes is null. Ohh nooo.");
    }

    BytesBytesCodec[] bytesBytesCodecs = getBytesBytesCodecs();
    for (int i = bytesBytesCodecs.length - 1; i >= 0; --i) {
      BytesBytesCodec codec = bytesBytesCodecs[i];
      chunkBytes = codec.decode(chunkBytes);
    }

    if (chunkBytes == null) {
      throw new ZarrException(
          "chunkBytes is null. This is likely a bug in one of the codecs. " + Arrays.toString(
              getBytesBytesCodecs()));
    }
    Array chunkArray = getArrayBytesCodec().decode(chunkBytes);
    if (chunkArray == null) {
      throw new ZarrException("chunkArray is null. This is likely a bug in one of the codecs.");
    }

    ArrayArrayCodec[] arrayArrayCodecs = getArrayArrayCodecs();
    for (int i = arrayArrayCodecs.length - 1; i >= 0; --i) {
      ArrayArrayCodec codec = arrayArrayCodecs[i];
      chunkArray = codec.decode(chunkArray);
    }

    if (chunkArray == null) {
      throw new ZarrException("chunkArray is null. This is likely a bug in one of the codecs.");
    }
    return chunkArray;
  }

  @Nonnull
  public ByteBuffer encode(
      @Nonnull Array chunkArray
  ) throws ZarrException {
    for (ArrayArrayCodec codec : getArrayArrayCodecs()) {
      chunkArray = codec.encode(chunkArray);
    }

    ByteBuffer chunkBytes = getArrayBytesCodec().encode(chunkArray);

    for (BytesBytesCodec codec : getBytesBytesCodecs()) {
      chunkBytes = codec.encode(chunkBytes);
    }
    return chunkBytes;
  }

  public long computeEncodedSize(long inputByteLength, CoreArrayMetadata arrayMetadata)
      throws ZarrException {
    for (Codec codec : codecs) {
      inputByteLength = codec.computeEncodedSize(inputByteLength, arrayMetadata);
    }
    return inputByteLength;
  }

  public Array partialDecode(
      StoreHandle valueHandle, long[] offset, int[] shape,
      CoreArrayMetadata arrayMetadata
  ) {
    return null; // TODO
  }

  public ByteBuffer partialEncode(
      StoreHandle oldValueHandle, Array array, long[] offset, int[] shape,
      CoreArrayMetadata arrayMetadata
  ) {
    return null; // TODO
  }
}
