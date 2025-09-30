package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.ArrayMetadata;

import java.nio.ByteOrder;
import javax.annotation.Nonnull;

public class BytesCodec extends dev.zarr.zarrjava.core.codec.core.BytesCodec implements Codec {

  public final String name = "bytes";
  @Nonnull
  public final Configuration configuration;

  @JsonCreator
  public BytesCodec(
      @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
  ) {
    this.configuration = configuration;
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
  protected ByteOrder getByteOrder() {
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

