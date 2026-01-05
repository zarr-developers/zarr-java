package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.Codec;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec extends BytesBytesCodec implements Codec {

    @JsonIgnore
    public final String name = "gzip";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GzipCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }


    @Override
    public ByteBuffer decode(ByteBuffer chunkBytes)
            throws ZarrException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPInputStream inputStream = new GZIPInputStream(
                new ByteArrayInputStream(Utils.toArray(chunkBytes)))) {
            Utils.copyStream(inputStream, outputStream);
            inputStream.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in decoding gzip.", ex);
        }
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes)
            throws ZarrException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPOutputStream gzipStream = new GZIPOutputStream(
                outputStream)) {
            gzipStream.write(Utils.toArray(chunkBytes));
            gzipStream.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in encoding gzip.", ex);
        }
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for Gzip codec.");
    }

    public static final class Configuration {

        public final int level;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@JsonProperty(value = "level", defaultValue = "5") int level)
                throws ZarrException {
            if (level < 0 || level > 9) {
                throw new ZarrException("'level' needs to be between 0 and 9.");
            }
            this.level = level;
        }
    }
}


