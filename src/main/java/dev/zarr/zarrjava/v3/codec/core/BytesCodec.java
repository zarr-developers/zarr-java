package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.codec.ArrayBytesCodec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import javax.annotation.Nonnull;
import ucar.ma2.Array;

public class BytesCodec extends Codec implements ArrayBytesCodec {

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
  public Array decode(ByteBuffer chunkBytes) {
    chunkBytes.order(configuration.endian.getByteOrder());
    return Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape,
        chunkBytes);
  }

  @Override
  public ByteBuffer encode(Array chunkArray) {
    return chunkArray.getDataAsByteBuffer(configuration.endian.getByteOrder());
  }

  @Override
  public long computeEncodedSize(long inputByteLength,
      ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
    return inputByteLength;
  }

  public enum Endian {
    LITTLE("little"),
    BIG("big");
    private final String endian;

    Endian(String endian) {
      this.endian = endian;
    }

    @JsonValue
    public String getValue() {
      return endian;
    }

    public ByteOrder getByteOrder() {
      switch (this) {
        case LITTLE:
          return ByteOrder.LITTLE_ENDIAN;
        case BIG:
          return ByteOrder.BIG_ENDIAN;
        default:
          throw new RuntimeException("Unreachable");
      }
    }
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

