package dev.zarr.zarrjava.v2.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v2.codec.Codec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BloscCodec extends dev.zarr.zarrjava.core.codec.core.BloscCodec implements Codec {

  public final String id = "blosc";

  @Nonnull
  @JsonSerialize(using = CustomCompressorSerializer.class)
  public final Blosc.Compressor cname;
  @Nonnull
  @JsonSerialize(using = CustomShuffleSerializer.class)
  public final Blosc.Shuffle shuffle;
  public final int clevel;
  public final int typesize;
  public final int blocksize;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public BloscCodec(
      @Nonnull @JsonProperty(value = "cname", defaultValue = "zstd")
      @JsonDeserialize(using = CustomCompressorDeserializer.class)
      Blosc.Compressor cname,
      @Nonnull @JsonProperty(value = "shuffle", defaultValue = "noshuffle")
      @JsonDeserialize(using = CustomShuffleDeserializer.class) Blosc.Shuffle shuffle,
      @JsonProperty(value = "clevel", defaultValue = "5") int clevel,
      @JsonProperty(value = "typesize", defaultValue = "0") int typesize,
      @JsonProperty(value = "blocksize", defaultValue = "0")
      int blocksize
  ) throws ZarrException {
    if (typesize < 1 && shuffle != Blosc.Shuffle.NO_SHUFFLE) {
      typesize = 4; // in v2 typesize is not required. todo: deflault to correct value based on dtype
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


  @Override
  public ByteBuffer encode(ByteBuffer chunkBytes)
      throws ZarrException {
    try {
      return ByteBuffer.wrap(
          Blosc.compress(Utils.toArray(chunkBytes), this.typesize, this.cname,
              this.clevel,
              this.shuffle, this.blocksize
          ));
    } catch (Exception ex) {
      throw new ZarrException("Error in encoding blosc.", ex);
    }
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
      generator.writeNumber(shuffle.ordinal());
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
      int shuffle = jsonParser.getCodec()
          .readValue(jsonParser, int.class);
      return Blosc.Shuffle.values()[shuffle];
    }
  }
}
