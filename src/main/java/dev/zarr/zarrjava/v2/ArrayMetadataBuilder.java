package dev.zarr.zarrjava.v2;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.v2.codec.CodecBuilder;

import java.util.function.Function;

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

  public ArrayMetadataBuilder withFilters(Codec... filters) {
    this.filters = filters;
    return this;
  }

  public ArrayMetadataBuilder withFilters(Function<CodecBuilder, CodecBuilder> codecBuilder) throws ZarrException {
    if (dataType == null) {
      throw new IllegalStateException("Please call `withDataType` first.");
    }
    CodecBuilder nestedCodecBuilder = new CodecBuilder(dataType);
    this.filters = codecBuilder.apply(nestedCodecBuilder)
        .build();
    return this;
  }

  public ArrayMetadataBuilder withCompressor(Codec compressor) {
    this.compressor = compressor;
    return this;
  }

  public ArrayMetadataBuilder withBloscCompressor(String cname,  String shuffle, int clevel) {
      this.compressor = new CodecBuilder(dataType)
          .withBlosc(cname, shuffle, clevel)
          .build()[0];
      return this;
  }

  public ArrayMetadataBuilder withZlibCompressor(int level) {
      this.compressor = new CodecBuilder(dataType)
          .withZlib(level)
          .build()[0];
      return this;
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