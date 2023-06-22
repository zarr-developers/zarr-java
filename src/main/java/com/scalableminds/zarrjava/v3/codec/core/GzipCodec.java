package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.BytesBytesCodec;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec implements BytesBytesCodec {

    public final String name = "gzip";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GzipCodec(@Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    private void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[4096];
        int len;
        while ((len = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, len);
        }
    }

    @Override
    public ByteBuffer decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPInputStream inputStream = new GZIPInputStream(
                new ByteArrayInputStream(chunkBytes.array()))) {
            copy(inputStream, outputStream);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            return null;
        }
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPOutputStream gzipStream = new GZIPOutputStream(
                outputStream); ByteArrayInputStream inputStream = new ByteArrayInputStream(chunkBytes.array())) {
            copy(inputStream, gzipStream);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            return null;
        }
    }

    public static final class Configuration {
        public final int level;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@JsonProperty(value = "level", defaultValue = "5") int level) {
            this.level = level;
        }
    }
}


