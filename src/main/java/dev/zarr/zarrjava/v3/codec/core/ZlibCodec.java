package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.BytesBytesCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.*;
import javax.annotation.Nonnull;

public class ZlibCodec extends BytesBytesCodec {

    public final String name = "zlib";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ZlibCodec(
        @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }


    @Override
    public ByteBuffer decode(ByteBuffer chunkBytes) throws ZarrException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); InflaterInputStream inputStream = new InflaterInputStream(
            new ByteArrayInputStream(Utils.toArray(chunkBytes)))) {
            copy(inputStream, outputStream);
            inputStream.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in decoding gzip.", ex);
        }
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             DeflaterOutputStream dos = new DeflaterOutputStream(outputStream, new Deflater(this.configuration.level))) {
            dos.write(Utils.toArray(chunkBytes));
            dos.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in encoding zlib.", ex);
        }
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for Zlib codec.");
    }


    public static final class Configuration {

        public final int level;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@JsonProperty(value = "level", defaultValue = "1") int level)
            throws ZarrException {
            if (level < 0 || level > 9) {
                throw new ZarrException("'level' needs to be between 0 and 9.");
            }
            this.level = level;
        }
    }
}
