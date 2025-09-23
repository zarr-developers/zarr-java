package dev.zarr.zarrjava.v2.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.*;

public class ZlibCodec extends BytesBytesCodec implements Codec {

    public final String id = "zlib";
    public final int level;


    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ZlibCodec(
        @JsonProperty(value = "level", defaultValue = "1") int level) throws ZarrException {
        if (level < 0 || level > 9) {
            throw new ZarrException("'level' needs to be between 0 and 9.");
        }
        this.level = level;
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
             DeflaterOutputStream dos = new DeflaterOutputStream(outputStream, new Deflater(this.level))) {
            dos.write(Utils.toArray(chunkBytes));
            dos.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in encoding zlib.", ex);
        }
    }
}
