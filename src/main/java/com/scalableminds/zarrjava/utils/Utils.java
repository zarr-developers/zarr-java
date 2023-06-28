package com.scalableminds.zarrjava.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

  public static ByteBuffer allocateNative(int capacity) {
    return ByteBuffer.allocate(capacity)
        .order(ByteOrder.nativeOrder());
  }

  public static ByteBuffer makeByteBuffer(int capacity, Function<ByteBuffer, ByteBuffer> func) {
    ByteBuffer buf = ByteBuffer.allocate(capacity)
        .order(ByteOrder.LITTLE_ENDIAN);
    buf = func.apply(buf);
    buf.rewind();
    return buf;
  }

  public static ByteBuffer asByteBuffer(InputStream inputStream) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    int nRead;
    byte[] data = new byte[1024];

    while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }

    buffer.flush();
    return ByteBuffer.wrap(buffer.toByteArray());
  }

  public static long[] toLongArray(int[] array) {
    return Arrays.stream(array)
        .mapToLong(i -> (long) i)
        .toArray();
  }

  public static int[] toIntArray(long[] array) {
    return Arrays.stream(array)
        .mapToInt(i -> (int) i)
        .toArray();
  }

  public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
    Iterable<T> iterable = () -> sourceIterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  public static <T> T[] concatArrays(T[] array1, T[]... arrays) {
    if (arrays.length == 0) {
      return array1;
    }
    T[] result = Arrays.copyOf(array1, array1.length + Arrays.stream(arrays)
        .mapToInt(a -> a.length)
        .sum());
    int offset = array1.length;
    for (T[] array2 : arrays) {
      System.arraycopy(array2, 0, result, offset, array2.length);
      offset += array2.length;
    }
    return result;
  }
}
