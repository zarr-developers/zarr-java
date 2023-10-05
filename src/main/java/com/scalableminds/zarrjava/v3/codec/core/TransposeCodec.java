package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.ArrayArrayCodec;
import javax.annotation.Nonnull;
import ucar.ma2.Array;

public class TransposeCodec implements ArrayArrayCodec {

  @Nonnull
  public final String name = "transpose";
  @Nonnull
  public final Configuration configuration;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public TransposeCodec(
      @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
  ) {
    this.configuration = configuration;
  }

  int[] reverseDims(int ndim) {
    int[] dims = new int[ndim];
    for (int dimIdx = 0; dimIdx < ndim; dimIdx++) {
      dims[dimIdx] = ndim - dimIdx - 1;
    }
    return dims;
  }

  @Override
  public Array decode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
    if (configuration.order.equals("F")) {
      chunkArray.permute(reverseDims(arrayMetadata.ndim()));
    }
    return chunkArray;
  }

  @Override
  public Array encode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
    if (configuration.order.equals("F")) {
      chunkArray.permute(reverseDims(arrayMetadata.ndim()));
    }
    return chunkArray;
  }

  @Override
  public long computeEncodedSize(long inputByteLength,
      ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
    return inputByteLength;
  }

  public static final class Configuration {

    public final String order;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Configuration(@JsonProperty(value = "order", defaultValue = "C") String order)
        throws ZarrException {
      if (!order.equals("C") && !order.equals("F")) {
        throw new ZarrException("Only 'C' or 'F' are supported.");
      }
      this.order = order;
    }
  }
}
