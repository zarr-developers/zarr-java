package dev.zarr.zarrjava.v2;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.v2.codec.CodecBuilder;

import java.util.function.Function;

public class ArrayMetadataBuilder {
  long[] shape = null;
  int[] chunks = null;
  DataTypeV2 dataTypeV2 = null;
  Order order = Order.C;
  Separator dimensionSeparator = Separator.DOT;
  Object fillValue = 0;
  Codec[] filters = null;
  Codec compressor = null;


  protected ArrayMetadataBuilder() {
  }

  protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata) {
    ArrayMetadataBuilder builder = new ArrayMetadataBuilder();
    builder.shape = arrayMetadata.shape;
    builder.chunks = arrayMetadata.chunks;
    builder.dataTypeV2 = arrayMetadata.dataTypeV2;
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

  public ArrayMetadataBuilder withDataType(DataTypeV2 dataTypeV2) {
    this.dataTypeV2 = dataTypeV2;
    return this;
  }

  public ArrayMetadataBuilder withDataType(DataType dataType) {
    this.dataTypeV2 = DataTypeV2.fromDataType(dataType);
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
    if (dataTypeV2 == null) {
      throw new IllegalStateException("Please call `withDataType` first.");
    }
    CodecBuilder nestedCodecBuilder = new CodecBuilder(dataTypeV2.toV3());
    this.filters = codecBuilder.apply(nestedCodecBuilder)
        .build();
    return this;
  }

  public ArrayMetadataBuilder withCompressor(Codec compressor) {
    this.compressor = compressor;
    return this;
  }

  public ArrayMetadataBuilder withBloscCompressor(String cname,  String shuffle, int clevel) {
    try {
      this.compressor = new CodecBuilder(dataTypeV2.toV3())
          .withBlosc(cname, shuffle, clevel)
          .build()[0];
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public ArrayMetadataBuilder withZlibCompressor(int level) {
    try {
      this.compressor = new CodecBuilder(dataTypeV2.toV3())
          .withZlib(level)
          .build()[0];
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public ArrayMetadata build() throws ZarrException {
    if (shape == null) {
      throw new IllegalStateException("Please call `withShape` first.");
    }
    if (chunks == null) {
      throw new IllegalStateException("Please call `withChunks` first.");
    }
    if (dataTypeV2 == null) {
      throw new IllegalStateException("Please call `withDataType` first.");
    }
    return new ArrayMetadata(
        2,
        shape,
        chunks,
        dataTypeV2,
        fillValue,
        order,
        dimensionSeparator,
        filters,
        compressor
    );
  }
}