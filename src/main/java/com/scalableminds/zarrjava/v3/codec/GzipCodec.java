package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.store.BufferValueHandle;
import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.store.NoneHandle;
import com.scalableminds.zarrjava.store.ValueHandle;
import com.scalableminds.zarrjava.v3.ArrayMetadata;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec extends Codec {

    public final String name = "gzip";
    public Configuration configuration;

    private void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[4096];
        int len;
        while ((len = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, len);
        }
    }

    @Override
    public ValueHandle decode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(chunk.toBytes().array()))) {

            copy(inputStream, outputStream);

            return new BufferValueHandle(outputStream.toByteArray());
        } catch (IOException ex) {
            return new NoneHandle();
        }
    }

    @Override
    public ValueHandle encode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(outputStream);
             ByteArrayInputStream inputStream = new ByteArrayInputStream(chunk.toBytes().array())) {

            copy(inputStream, gzipStream);

            return new BufferValueHandle(outputStream.toByteArray());
        } catch (IOException ex) {
            return new NoneHandle();
        }
    }

    public static final class Configuration {
        public int level = 5;
    }
}


