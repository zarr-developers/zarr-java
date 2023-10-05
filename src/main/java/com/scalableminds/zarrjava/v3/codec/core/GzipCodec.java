package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.utils.Utils;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.BytesBytesCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;

public class GzipCodec implements BytesBytesCodec {

  public final String name = "gzip";
  @Nonnull
  public final Configuration configuration;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public GzipCodec(
      @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
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
  public ByteBuffer decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata)
      throws ZarrException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); GZIPInputStream inputStream = new GZIPInputStream(
        new ByteArrayInputStream(Utils.toArray(chunkBytes)))) {
      copy(inputStream, outputStream);
      inputStream.close();
      return ByteBuffer.wrap(outputStream.toByteArray());
    } catch (IOException ex) {
      throw new ZarrException("Error in decoding gzip.", ex);
    }
  }

  @Override
  public ByteBuffer encode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata)
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


