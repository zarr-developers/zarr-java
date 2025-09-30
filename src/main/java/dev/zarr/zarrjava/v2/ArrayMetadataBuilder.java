package dev.zarr.zarrjava.v2;

import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.v2.codec.core.BloscCodec;
import dev.zarr.zarrjava.v2.codec.core.ZlibCodec;

public class ArrayMetadataBuilder {
  long[] shape = null;
  int[] chunks = null;
  DataType dataType = null;
  Order order = Order.C;
  Separator dimensionSeparator = Separator.DOT;
  Object fillValue = null;
  Codec[] filters = null;
  Codec compressor = null;


  protected ArrayMetadataBuilder() {
  }

  protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata) {
    ArrayMetadataBuilder builder = new ArrayMetadataBuilder();
    builder.shape = arrayMetadata.shape;
    builder.chunks = arrayMetadata.chunks;
    builder.dataType = arrayMetadata.dataType;
    builder.order = arrayMetadata.order;
    builder.dimensionSeparator = arrayMetadata.dimensionSeparator;
    builder.fillValue = arrayMetadata.parsedFillValue;
    builder.filters = arrayMetadata.filters;
    builder.compressor = arrayMetadata.compressor;
    return builder;
  }

  public ArrayMetadataBuilder withShape(long... shape) {
    this.shape = shape;
    return this;
  }

  public ArrayMetadataBuilder withChunks(int... chunks) {
    this.chunks = chunks;
    return this;
  }

  public ArrayMetadataBuilder withDataType(DataType dataTypeV2) {
    this.dataType = dataTypeV2;
    return this;
  }

  public ArrayMetadataBuilder withOrder(Order order) {
    this.order = order;
    return this;
  }

  public ArrayMetadataBuilder withDimensionSeparator(Separator dimensionSeparator) {
    this.dimensionSeparator = dimensionSeparator;
    return this;
  }

  public ArrayMetadataBuilder withFillValue(Object fillValue) {
    this.fillValue = fillValue;
    return this;
  }

  public ArrayMetadataBuilder withCompressor(Codec compressor) {
    this.compressor = compressor;
    return this;
  }

  public ArrayMetadataBuilder withBloscCompressor(
      Blosc.Compressor cname, Blosc.Shuffle shuffle, int clevel, int typeSize,
      int blockSize
  ) {
    try {
      this.compressor = new BloscCodec(cname, shuffle, clevel, typeSize, blockSize);
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public ArrayMetadataBuilder withBloscCompressor(String cname, String shuffle, int clevel, int blockSize) {
    if (shuffle.equals("shuffle")) {
      shuffle = "byteshuffle";
    }
    return withBloscCompressor(Blosc.Compressor.fromString(cname), Blosc.Shuffle.fromString(shuffle), clevel,
        dataType.getByteCount(), blockSize
    );
  }

  public ArrayMetadataBuilder withBloscCompressor(String cname, String shuffle, int clevel) {
    return withBloscCompressor(cname, shuffle, clevel, 0);
  }

  public ArrayMetadataBuilder withBloscCompressor(String cname, int clevel) {
    return withBloscCompressor(cname, "noshuffle", clevel);
  }

  public ArrayMetadataBuilder withBloscCompressor(String cname) {
    return withBloscCompressor(cname, 5);
  }

  public ArrayMetadataBuilder withBloscCompressor() {
    return withBloscCompressor("zstd");
  }

  public ArrayMetadataBuilder withZlibCompressor(int level) {
    try {
      this.compressor = new ZlibCodec(level);
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public ArrayMetadataBuilder withZlibCompressor() {
    return withZlibCompressor(5);
  }

  public ArrayMetadata build() throws ZarrException {
    if (shape == null) {
      throw new IllegalStateException("Please call `withShape` first.");
    }
    if (chunks == null) {
      throw new IllegalStateException("Please call `withChunks` first.");
    }
    if (dataType == null) {
      throw new IllegalStateException("Please call `withDataType` first.");
    }
    return new ArrayMetadata(
        2,
        shape,
        chunks,
        dataType,
        fillValue,
        order,
        filters,
        compressor,
        dimensionSeparator
    );
  }
}