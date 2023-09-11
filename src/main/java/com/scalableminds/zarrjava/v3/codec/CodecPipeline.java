package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.store.StoreHandle;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Nonnull;
import ucar.ma2.Array;

public class CodecPipeline {

  @Nonnull
  final Codec[] codecs;

  public CodecPipeline(@Nonnull Codec[] codecs) throws ZarrException {
    long arrayBytesCodecCount = Arrays.stream(codecs).filter(c -> c instanceof ArrayBytesCodec)
        .count();
    if (arrayBytesCodecCount != 1) {
      throw new ZarrException(
          "Exactly 1 ArrayBytesCodec is required. Found " + arrayBytesCodecCount + ".");
    }
    Codec prevCodec = null;
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

  @Nonnull
  public Array decode(
      @Nonnull ByteBuffer chunkBytes,
      @Nonnull ArrayMetadata.CoreArrayMetadata arrayMetadata
  ) throws ZarrException {
    if (chunkBytes == null) {
      throw new ZarrException("chunkBytes is null. Ohh nooo.");
    }
    for (BytesBytesCodec codec : getBytesBytesCodecs()) { // TODO iterate in reverse
      chunkBytes = codec.decode(chunkBytes, arrayMetadata);
    }
    if (chunkBytes == null) {
      throw new ZarrException(
          "chunkBytes is null. This is likely a bug in one of the codecs. " + Arrays.toString(
              getBytesBytesCodecs()));
    }
    Array chunkArray = getArrayBytesCodec().decode(chunkBytes, arrayMetadata);
    if (chunkArray == null) {
      throw new ZarrException("chunkArray is null. This is likely a bug in one of the codecs.");
    }
    for (ArrayArrayCodec codec : getArrayArrayCodecs()) { // TODO iterate in reverse
      chunkArray = codec.decode(chunkArray, arrayMetadata);
    }
    if (chunkArray == null) {
      throw new ZarrException("chunkArray is null. This is likely a bug in one of the codecs.");
    }
    return chunkArray;
  }

  @Nonnull
  public ByteBuffer encode(
      @Nonnull Array chunkArray, @Nonnull ArrayMetadata.CoreArrayMetadata arrayMetadata
  ) throws ZarrException {
    for (ArrayArrayCodec codec : getArrayArrayCodecs()) {
      chunkArray = codec.encode(chunkArray, arrayMetadata);
    }

    ByteBuffer chunkBytes = getArrayBytesCodec().encode(chunkArray, arrayMetadata);

    for (BytesBytesCodec codec : getBytesBytesCodecs()) {
      chunkBytes = codec.encode(chunkBytes, arrayMetadata);
    }
    return chunkBytes;
  }

  public Array partialDecode(
      StoreHandle valueHandle, long[] offset, int[] shape,
      ArrayMetadata.CoreArrayMetadata arrayMetadata
  ) {
    return null; // TODO
  }

  public ByteBuffer partialEncode(
      StoreHandle oldValueHandle, Array array, long[] offset, int[] shape,
      ArrayMetadata.CoreArrayMetadata arrayMetadata
  ) {
    return null; // TODO
  }
}
