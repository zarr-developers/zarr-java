package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.scalableminds.bloscjava.Blosc;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.BytesBytesCodec;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

public class BloscCodec implements BytesBytesCodec {

  public final String name = "blosc";
  @Nonnull
  public final Configuration configuration;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public BloscCodec(
      @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ByteBuffer decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
    return ByteBuffer.wrap(Blosc.decompress(chunkBytes.array()));
  }

  @Override
  public ByteBuffer encode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {

    return ByteBuffer.wrap(
        Blosc.compress(chunkBytes.array(), configuration.typesize, configuration.cname,
            configuration.clevel,
            configuration.shuffle, configuration.blocksize
        ));
  }

  public static final class CustomShuffleSerializer extends StdSerializer<Blosc.Shuffle> {

    public CustomShuffleSerializer() {
      super(Blosc.Shuffle.class);
    }

    public CustomShuffleSerializer(Class t) {
      super(t);
    }

    @Override
    public void serialize(Blosc.Shuffle shuffle, JsonGenerator generator,
        SerializerProvider provider)
        throws IOException {
      switch (shuffle) {
        case NO_SHUFFLE:
          generator.writeString("noshuffle");
          break;
        case BIT_SHUFFLE:
          generator.writeString("bitshuffle");
          break;
        case BYTE_SHUFFLE:
          generator.writeString("byteshuffle");
          break;
      }
    }
  }

  public static final class CustomCompressorDeserializer extends StdDeserializer<Blosc.Compressor> {

    public CustomCompressorDeserializer() {
      this(null);
    }

    public CustomCompressorDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Blosc.Compressor deserialize(JsonParser jsonParser, DeserializationContext ctxt)
        throws IOException {
      String cname = jsonParser.getCodec()
          .readValue(jsonParser, String.class);
      Blosc.Compressor compressor = Blosc.Compressor.fromString(cname);
      if (compressor == null) {
        throw new JsonParseException(
            jsonParser,
            String.format("Could not parse the Blosc.Compressor. Got '%s'", cname)
        );
      }
      return compressor;
    }
  }

  public static final class CustomCompressorSerializer extends StdSerializer<Blosc.Compressor> {

    public CustomCompressorSerializer() {
      super(Blosc.Compressor.class);
    }

    public CustomCompressorSerializer(Class t) {
      super(t);
    }

    @Override
    public void serialize(Blosc.Compressor compressor, JsonGenerator generator,
        SerializerProvider provider)
        throws IOException {
      generator.writeString(compressor.getValue());
    }
  }

  public static final class CustomShuffleDeserializer extends StdDeserializer<Blosc.Shuffle> {

    public CustomShuffleDeserializer() {
      this(null);
    }

    public CustomShuffleDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Blosc.Shuffle deserialize(JsonParser jsonParser, DeserializationContext ctxt)
        throws IOException {
      String shuffle = jsonParser.getCodec()
          .readValue(jsonParser, String.class);
      switch (shuffle) {
        case "noshuffle":
          return Blosc.Shuffle.NO_SHUFFLE;
        case "bitshuffle":
          return Blosc.Shuffle.BIT_SHUFFLE;
        case "byteshuffle":
          return Blosc.Shuffle.BYTE_SHUFFLE;
        default:
          throw new JsonParseException(
              jsonParser,
              String.format(
                  "Could not parse the value for Blosc.Shuffle." + " Got '%s'",
                  shuffle
              )
          );
      }
    }
  }

  public static final class Configuration {

    @Nonnull
    @JsonSerialize(using = BloscCodec.CustomCompressorSerializer.class)
    public final Blosc.Compressor cname;
    @Nonnull
    @JsonSerialize(using = BloscCodec.CustomShuffleSerializer.class)
    public final Blosc.Shuffle shuffle;
    public final int clevel;
    public final int typesize;
    public final int blocksize;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Configuration(
        @Nonnull @JsonProperty(value = "cname", defaultValue = "zstd")
        @JsonDeserialize(using = BloscCodec.CustomCompressorDeserializer.class)
        Blosc.Compressor cname,
        @Nonnull @JsonProperty(value = "shuffle", defaultValue = "noshuffle")
        @JsonDeserialize(using = BloscCodec.CustomShuffleDeserializer.class) Blosc.Shuffle shuffle,
        @JsonProperty(value = "clevel", defaultValue = "5") int clevel,
        @JsonProperty(value = "typesize", defaultValue = "0") int typesize,
        @JsonProperty(value = "blocksize", defaultValue = "0")
        int blocksize
    ) throws ZarrException {
      if (typesize < 1) {
        throw new ZarrException("'typesize' needs to be larger than 0.");
      }
      if (clevel < 0 || clevel > 9) {
        throw new ZarrException("'clevel' needs to be between 0 and 9.");
      }
      this.cname = cname;
      this.shuffle = shuffle;
      this.clevel = clevel;
      this.typesize = typesize;
      this.blocksize = blocksize;
    }
  }
}
