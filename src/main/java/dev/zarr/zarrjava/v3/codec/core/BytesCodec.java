package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.ArrayMetadata;

import java.nio.ByteOrder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BytesCodec extends dev.zarr.zarrjava.core.codec.core.BytesCodec implements Codec {

  public final String name = "bytes";
  @Nullable
  public final Configuration configuration;

  @JsonCreator
  public BytesCodec(
      @JsonProperty(value = "configuration") Configuration configuration
  ) {
    this.configuration = configuration;
  }

  public BytesCodec() {
    this((Configuration) null);
  }

  public BytesCodec(Endian endian) {
    this(new BytesCodec.Configuration(endian));
  }

  @Override
  public long computeEncodedSize(long inputByteLength,
      ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
    return inputByteLength;
  }

  @Override
  protected ByteOrder getByteOrder() throws ZarrException {
    if (configuration == null) {
      throw new ZarrException("BytesCodec configuration is required to determine endianess.");
    }
    return configuration.endian.getByteOrder();
  }


  public static final class Configuration{

    @Nonnull
    public final BytesCodec.Endian endian;

    @JsonCreator
    public Configuration(
        @JsonProperty(value = "endian", defaultValue = "little") BytesCodec.Endian endian) {
      this.endian = endian;
    }
  }
}

