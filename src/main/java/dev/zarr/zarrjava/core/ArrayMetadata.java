package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Arrays;

public interface ArrayMetadata {
    int ndim();

    int[] chunkShape();

    long[] shape();

    DataType dataType();

    Array allocateFillValueChunk();

    ChunkKeyEncoding chunkKeyEncoding();

    Object parsedFillValue();

    static Object parseFillValue(Object fillValue, @Nonnull DataType dataType)
      throws ZarrException {
    boolean dataTypeIsBool = dataType == dev.zarr.zarrjava.v3.DataType.BOOL || dataType == dev.zarr.zarrjava.v2.DataType.BOOL;
    boolean dataTypeIsByte = dataType == dev.zarr.zarrjava.v3.DataType.INT8 || dataType == dev.zarr.zarrjava.v2.DataType.INT8 || dataType == dev.zarr.zarrjava.v3.DataType.UINT8 || dataType == dev.zarr.zarrjava.v2.DataType.UINT8;
    boolean dataTypeIsShort = dataType == dev.zarr.zarrjava.v3.DataType.INT16 || dataType == dev.zarr.zarrjava.v2.DataType.INT16 || dataType == dev.zarr.zarrjava.v3.DataType.UINT16 || dataType == dev.zarr.zarrjava.v2.DataType.UINT16;
    boolean dataTypeIsInt = dataType == dev.zarr.zarrjava.v3.DataType.INT32 || dataType == dev.zarr.zarrjava.v2.DataType.INT32 || dataType == dev.zarr.zarrjava.v3.DataType.UINT32 || dataType == dev.zarr.zarrjava.v2.DataType.UINT32;
    boolean dataTypeIsLong = dataType == dev.zarr.zarrjava.v3.DataType.INT64 || dataType == dev.zarr.zarrjava.v2.DataType.INT64 || dataType == dev.zarr.zarrjava.v3.DataType.UINT64 || dataType == dev.zarr.zarrjava.v2.DataType.UINT64;
    boolean dataTypeIsFloat = dataType == dev.zarr.zarrjava.v3.DataType.FLOAT32 || dataType == dev.zarr.zarrjava.v2.DataType.FLOAT32;
    boolean dataTypeIsDouble = dataType == dev.zarr.zarrjava.v3.DataType.FLOAT64 || dataType == dev.zarr.zarrjava.v2.DataType.FLOAT64;

    if (fillValue instanceof Boolean) {
      Boolean fillValueBool = (Boolean) fillValue;
      if (dataTypeIsBool) {
        return fillValueBool;
      }
    }
    if (fillValue instanceof Number) {
      Number fillValueNumber = (Number) fillValue;
        if (dataTypeIsBool) {
            return fillValueNumber.byteValue() != 0;
        } else if (dataTypeIsByte) {
            return fillValueNumber.byteValue();
        } else if (dataTypeIsShort) {
            return fillValueNumber.shortValue();
        } else if (dataTypeIsInt) {
            return fillValueNumber.intValue();
        } else if (dataTypeIsLong) {
            return fillValueNumber.longValue();
        } else if (dataTypeIsFloat) {
            return fillValueNumber.floatValue();
        } else if (dataTypeIsDouble) {
            return fillValueNumber.doubleValue();
        }
        // Fallback to throwing below
    } else if (fillValue instanceof String) {
      String fillValueString = (String) fillValue;
      if (fillValueString.equals("NaN")) {
          if (dataTypeIsFloat) {
              return Float.NaN;
          } else if (dataTypeIsDouble) {
              return Double.NaN;
          }
          throw new ZarrException(
              "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
      } else if (fillValueString.equals("+Infinity")) {
          if (dataTypeIsFloat) {
              return Float.POSITIVE_INFINITY;
          } else if (dataTypeIsDouble) {
              return Double.POSITIVE_INFINITY;
          }
          throw new ZarrException(
              "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
      } else if (fillValueString.equals("-Infinity")) {
          if (dataTypeIsFloat) {
              return Float.NEGATIVE_INFINITY;
          } else if (dataTypeIsDouble) {
              return Double.NEGATIVE_INFINITY;
          }
          throw new ZarrException(
              "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
      }
      else if (fillValueString.startsWith("0b") || fillValueString.startsWith("0x")) {
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
          if (dataTypeIsBool) {
              return buf.get() != 0;
          } else if (dataTypeIsByte) {
              return buf.get();
          } else if (dataTypeIsShort) {
              return buf.getShort();
          } else if (dataTypeIsInt) {
              return buf.getInt();
          } else if (dataTypeIsLong) {
              return buf.getLong();
          } else if (dataTypeIsFloat) {
              return buf.getFloat();
          } else if (dataTypeIsDouble) {
              return buf.getDouble();
            // Fallback to throwing below
          }
        }
      }
    }
    throw new ZarrException("Invalid fill value '" + fillValue + "'.");
  }

    final class CoreArrayMetadata {

    public final long[] shape;
    public final int[] chunkShape;
    public final dev.zarr.zarrjava.v3.DataType dataType;
    public final Object parsedFillValue;

    public CoreArrayMetadata(long[] shape, int[] chunkShape, dev.zarr.zarrjava.v3.DataType dataType,
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
