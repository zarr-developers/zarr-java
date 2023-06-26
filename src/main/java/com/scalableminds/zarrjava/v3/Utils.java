package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.scalableminds.zarrjava.v3.codec.CodecRegistry;

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
        return ByteBuffer.allocate(capacity).order(ByteOrder.nativeOrder());
    }

    public static ByteBuffer makeByteBuffer(int capacity, Function<ByteBuffer, ByteBuffer> func) {
        ByteBuffer buf = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
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
        return Arrays.stream(array).mapToLong(i -> (long) i).toArray();
    }

    public static int[] toIntArray(long[] array) {
        return Arrays.stream(array).mapToInt(i -> (int) i).toArray();
    }

    public static ObjectMapper makeObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
        return objectMapper;
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
