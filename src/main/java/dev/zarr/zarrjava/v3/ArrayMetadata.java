package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.chunkgrid.ChunkGrid;
import dev.zarr.zarrjava.v3.chunkgrid.RegularChunkGrid;
import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.codec.Codec;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public final class ArrayMetadata {

  static final String NODE_TYPE = "array";
  static final int ZARR_FORMAT = 3;

  @JsonProperty("zarr_format")
  public final int zarrFormat = ZARR_FORMAT;
  @JsonProperty("node_type")
  public final String nodeType = NODE_TYPE;


  public final long[] shape;

  @JsonProperty("data_type")
  public final DataType dataType;

  @JsonProperty("chunk_grid")
  public final ChunkGrid chunkGrid;

  @JsonProperty("chunk_key_encoding")
  public final ChunkKeyEncoding chunkKeyEncoding;

  @JsonProperty("fill_value")
  public final Object fillValue;
  @JsonIgnore
  public final Object parsedFillValue;

  @JsonProperty("codecs")
  public final Codec[] codecs;
  @Nullable
  @JsonProperty("attributes")
  public final Map<String, Object> attributes;
  @Nullable
  @JsonProperty("dimension_names")
  public String[] dimensionNames;

  @JsonIgnore
  public CoreArrayMetadata coreArrayMetadata;

  public ArrayMetadata(
      long[] shape, DataType dataType, ChunkGrid chunkGrid, ChunkKeyEncoding chunkKeyEncoding,
      Object fillValue,
      @Nonnull Codec[] codecs,
      @Nullable String[] dimensionNames,
      @Nullable Map<String, Object> attributes
  ) throws ZarrException {
    this(ZARR_FORMAT, NODE_TYPE, shape, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
        dimensionNames,
        attributes
    );
  }

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ArrayMetadata(
      @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
      @JsonProperty(value = "node_type", required = true) String nodeType,
      @JsonProperty(value = "shape", required = true) long[] shape,
      @JsonProperty(value = "data_type", required = true) DataType dataType,
      @JsonProperty(value = "chunk_grid", required = true) ChunkGrid chunkGrid,
      @JsonProperty(value = "chunk_key_encoding", required = true) ChunkKeyEncoding chunkKeyEncoding,
      @JsonProperty(value = "fill_value", required = true) Object fillValue,
      @Nonnull @JsonProperty(value = "codecs") Codec[] codecs,
      @Nullable @JsonProperty(value = "dimension_names") String[] dimensionNames,
      @Nullable @JsonProperty(value = "attributes") Map<String, Object> attributes
  ) throws ZarrException {
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
          "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }
    if (!nodeType.equals(this.nodeType)) {
      throw new ZarrException(
          "Expected node type '" + this.nodeType + "', got '" + nodeType + "'.");
    }

    this.shape = shape;
    this.dataType = dataType;
    this.chunkGrid = chunkGrid;
    this.chunkKeyEncoding = chunkKeyEncoding;
    this.fillValue = fillValue;
    this.parsedFillValue = parseFillValue(fillValue, dataType);
    this.codecs = codecs;
    this.dimensionNames = dimensionNames;
    this.attributes = attributes;
    this.coreArrayMetadata =
        new CoreArrayMetadata(shape, ((RegularChunkGrid) chunkGrid).configuration.chunkShape,
            dataType,
            parsedFillValue
        );
  }

  public static Object parseFillValue(Object fillValue, @Nonnull DataType dataType)
      throws ZarrException {
    if (fillValue instanceof Boolean) {
      Boolean fillValueBool = (Boolean) fillValue;
      if (dataType == DataType.BOOL) {
        return fillValueBool;
      }
    }
    if (fillValue instanceof Number) {
      Number fillValueNumber = (Number) fillValue;
      switch (dataType) {
        case BOOL:
          return fillValueNumber.byteValue() != 0;
        case INT8:
        case UINT8:
          return fillValueNumber.byteValue();
        case INT16:
        case UINT16:
          return fillValueNumber.shortValue();
        case INT32:
        case UINT32:
          return fillValueNumber.intValue();
        case INT64:
        case UINT64:
          return fillValueNumber.longValue();
        case FLOAT32:
          return fillValueNumber.floatValue();
        case FLOAT64:
          return fillValueNumber.doubleValue();
        default:
          // Fallback to throwing below
      }
    } else if (fillValue instanceof String) {
      String fillValueString = (String) fillValue;
      if (fillValueString.equals("NaN")) {
        switch (dataType) {
          case FLOAT32:
            return Float.NaN;
          case FLOAT64:
            return Double.NaN;
          default:
            throw new ZarrException(
                "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.equals("+Infinity")) {
        switch (dataType) {
          case FLOAT32:
            return Float.POSITIVE_INFINITY;
          case FLOAT64:
            return Double.POSITIVE_INFINITY;
          default:
            throw new ZarrException(
                "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.equals("-Infinity")) {
        switch (dataType) {
          case FLOAT32:
            return Float.NEGATIVE_INFINITY;
          case FLOAT64:
            return Double.NEGATIVE_INFINITY;
          default:
            throw new ZarrException(
                "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.startsWith("0b") || fillValueString.startsWith("0x")) {
        ByteBuffer buf = null;
        if (fillValueString.startsWith("0b")) {
          buf = Utils.makeByteBuffer(dataType.getByteCount(), b -> {
            for (int i = 0; i < dataType.getByteCount(); i++) {
              b.put((byte) Integer.parseInt(fillValueString.substring(2 + i * 8, 2 + (i + 1) * 8),
                  2));
            }
            return b;
          });
        } else if (fillValueString.startsWith("0x")) {
          buf = Utils.makeByteBuffer(dataType.getByteCount(), b -> {
            for (int i = 0; i < dataType.getByteCount(); i++) {
              b.put((byte) Integer.parseInt(fillValueString.substring(2 + i * 2, 2 + (i + 1) * 2),
                  16));
            }
            return b;
          });
        }
        if (buf != null) {
          switch (dataType) {
            case BOOL:
              return buf.get() != 0;
            case INT8:
            case UINT8:
              return buf.get();
            case INT16:
            case UINT16:
              return buf.getShort();
            case INT32:
            case UINT32:
              return buf.getInt();
            case INT64:
            case UINT64:
              return buf.getLong();
            case FLOAT32:
              return buf.getFloat();
            case FLOAT64:
              return buf.getDouble();
            default:
              // Fallback to throwing below
          }
        }
      }
    }
    throw new ZarrException("Invalid fill value '" + fillValue + "'.");
  }

  public ucar.ma2.Array allocateFillValueChunk() {
    return coreArrayMetadata.allocateFillValueChunk();
  }

  public int ndim() {
    return shape.length;
  }

  public int[] chunkShape() {
    return ((RegularChunkGrid) this.chunkGrid).configuration.chunkShape;
  }

  public int chunkSize() {
    return coreArrayMetadata.chunkSize();
  }

  public int chunkByteLength() {
    return coreArrayMetadata.chunkByteLength();
  }

  public static final class CoreArrayMetadata {

    public final long[] shape;
    public final int[] chunkShape;
    public final DataType dataType;
    public final Object parsedFillValue;

    public CoreArrayMetadata(long[] shape, int[] chunkShape, DataType dataType,
        Object parsedFillValue) {
      this.shape = shape;
      this.chunkShape = chunkShape;
      this.dataType = dataType;
      this.parsedFillValue = parsedFillValue;
    }

    public int ndim() {
      return shape.length;
    }

    public int chunkSize() {
      return Arrays.stream(chunkShape)
          .reduce(1, (acc, a) -> acc * a);
    }

    public int chunkByteLength() {
      return this.dataType.getByteCount() * chunkSize();
    }

    public ucar.ma2.Array allocateFillValueChunk() {
      ucar.ma2.Array outputArray = ucar.ma2.Array.factory(dataType.getMA2DataType(), chunkShape);
      MultiArrayUtils.fill(outputArray, parsedFillValue);
      return outputArray;
    }
  }

}
