package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.Codec.BytesBytesCodec;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec extends BytesBytesCodec {

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
    public ByteBuffer innerDecode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPInputStream inputStream = new GZIPInputStream(
                new ByteArrayInputStream(chunkBytes.array()))) {
            copy(inputStream, outputStream);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            return null;
        }
    }

    @Override
    public ByteBuffer innerEncode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPOutputStream gzipStream = new GZIPOutputStream(
                outputStream); ByteArrayInputStream inputStream = new ByteArrayInputStream(chunkBytes.array())) {
            copy(inputStream, gzipStream);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            return null;
        }
    }

    public static final class Configuration {
        public int level = 5;
    }
}


