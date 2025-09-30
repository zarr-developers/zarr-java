package dev.zarr.zarrjava.core.codec.core;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;
import dev.zarr.zarrjava.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class BloscCodec extends BytesBytesCodec {

  @Override
  public ByteBuffer decode(ByteBuffer chunkBytes)
      throws ZarrException {
    try {
      return ByteBuffer.wrap(Blosc.decompress(Utils.toArray(chunkBytes)));
    } catch (Exception ex) {
      throw new ZarrException("Error in decoding blosc.", ex);
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
}
