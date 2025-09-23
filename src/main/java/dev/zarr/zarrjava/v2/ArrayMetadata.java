package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.v2.chunkkeyencoding.V2ChunkKeyEncoding;
import dev.zarr.zarrjava.v2.codec.Codec;
import ucar.ma2.Array;

import javax.annotation.Nullable;

import static dev.zarr.zarrjava.core.ArrayMetadata.parseFillValue;


public class ArrayMetadata implements dev.zarr.zarrjava.core.ArrayMetadata {
  static final int ZARR_FORMAT = 2;

  @JsonProperty("zarr_format")
  public final int zarrFormat = ZARR_FORMAT;

  public long[] shape;
  public int[] chunks;

  @JsonProperty("dtype")
  public DataType dataType;

  @JsonIgnore
  public final Endianness endianness;

  @JsonProperty("order")
  public Order order;

  @JsonProperty("dimension_separator")
  public Separator dimensionSeparator;

  @JsonProperty("fill_value")
  public Object fillValue;
  @JsonIgnore
  public final Object parsedFillValue;

  @Nullable
  public Codec[] filters;
  @Nullable
  public Codec compressor;

  @JsonIgnore
  public CoreArrayMetadata coreArrayMetadata;


  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ArrayMetadata(
      @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
      @JsonProperty(value = "shape", required = true) long[] shape,
      @JsonProperty(value = "chunks", required = true) int[] chunks,
      @JsonProperty(value = "dtype", required = true) DataType dataType,
      @Nullable @JsonProperty(value = "fill_value", required = true) Object fillValue,
      @JsonProperty(value = "order", required = true) Order order,
      @Nullable @JsonProperty(value = "filters", required = true) Codec[] filters,
      @Nullable @JsonProperty(value = "compressor", required = true) Codec compressor,
      @Nullable @JsonProperty(value = "dimension_separator") Separator dimensionSeparator
  ) throws ZarrException {
    super();
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
          "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }
    this.shape = shape;
    this.chunks = chunks;
    this.dataType = dataType;
    this.endianness = dataType.getEndianness();
    this.fillValue = fillValue;
    if (fillValue == null) {
      this.parsedFillValue = null;
    } else {
      this.parsedFillValue = parseFillValue(fillValue, this.dataType);
    }
    this.order = order;
    this.dimensionSeparator = dimensionSeparator;
    this.filters = filters;
    this.compressor = compressor;
    this.coreArrayMetadata =
        new ArrayMetadata.CoreArrayMetadata(shape, chunks,
            this.dataType,
            parsedFillValue
        );
  }


  public int ndim() {
    return shape.length;
  }

  @Override
  public int[] chunkShape() {
    return chunks;
  }

  @Override
  public long[] shape() {
    return shape;
  }

  @Override
  public DataType dataType() {
    return dataType;
  }

  @Override
  public Array allocateFillValueChunk() {
      ucar.ma2.Array outputArray = ucar.ma2.Array.factory(dataType.getMA2DataType(), chunks);
      if (parsedFillValue != null) MultiArrayUtils.fill(outputArray, parsedFillValue);
      return outputArray;
  }

  @Override
  public ChunkKeyEncoding chunkKeyEncoding() {
    Separator separator = dimensionSeparator == null ? Separator.DOT : dimensionSeparator;
    return new V2ChunkKeyEncoding(separator);
  }

  @Override
  public Object parsedFillValue() {
    return parsedFillValue;
  }
}
