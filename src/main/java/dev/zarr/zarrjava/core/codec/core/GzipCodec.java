package dev.zarr.zarrjava.core.codec.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;
import dev.zarr.zarrjava.utils.Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class GzipCodec extends BytesBytesCodec {

    @Override
    public ByteBuffer decode(ByteBuffer chunkBytes) throws ZarrException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPInputStream inputStream = new GZIPInputStream(
                new ByteArrayInputStream(Utils.toArray(chunkBytes)))) {
            Utils.copyStream(inputStream, outputStream);
            inputStream.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in decoding gzip.", ex);
        }
    }

    protected ByteBuffer encodeInternal(int level, ByteBuffer chunkBytes) throws ZarrException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPOutputStream gzipStream = new GZIPOutputStream(
                outputStream) {
            {
                this.def.setLevel(level);
            }
        }) {
            gzipStream.write(Utils.toArray(chunkBytes));
            gzipStream.close();
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (IOException ex) {
            throw new ZarrException("Error in encoding gzip.", ex);
        }
    }
}
