package com.scalableminds.zarrjava.v3.codec;

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
import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.Codec.BytesBytesCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BloscCodec extends BytesBytesCodec {
    public final String name = "blosc";
    public Configuration configuration;

    @Override
    public ByteBuffer innerDecode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return ByteBuffer.wrap(Blosc.decompress(chunkBytes.array()));
    }

    @Override
    public ByteBuffer innerEncode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return ByteBuffer.wrap(
                Blosc.compress(chunkBytes.array(), configuration.typesize, configuration.cname, configuration.clevel,
                        configuration.shuffle, configuration.blocksize));
    }

    public static final class CustomShuffleSerializer extends StdSerializer<Blosc.Shuffle> {

        public CustomShuffleSerializer() {
            super(Blosc.Shuffle.class);
        }

        public CustomShuffleSerializer(Class t) {
            super(t);
        }

        @Override
        public void serialize(Blosc.Shuffle shuffle, JsonGenerator generator, SerializerProvider provider) throws IOException {
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
        public Blosc.Compressor deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
            String cname = jsonParser.getCodec().readValue(jsonParser, String.class);
            Blosc.Compressor compressor = Blosc.Compressor.fromString(cname);
            if (compressor == null) {
                throw new JsonParseException(jsonParser,
                        String.format("Could not parse the Blosc.Compressor. Got '%s'", cname));
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
        public void serialize(Blosc.Compressor compressor, JsonGenerator generator, SerializerProvider provider) throws IOException {
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
        public Blosc.Shuffle deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
            String shuffle = jsonParser.getCodec().readValue(jsonParser, String.class);
            switch (shuffle) {
                case "noshuffle":
                    return Blosc.Shuffle.NO_SHUFFLE;
                case "bitshuffle":
                    return Blosc.Shuffle.BIT_SHUFFLE;
                case "byteshuffle":
                    return Blosc.Shuffle.BYTE_SHUFFLE;
                default:
                    throw new JsonParseException(jsonParser,
                            String.format("Could not parse the value for Blosc.Shuffle." + " Got '%s'", shuffle));
            }
        }
    }

    public static final class Configuration {
        @JsonDeserialize(using = BloscCodec.CustomCompressorDeserializer.class)
        @JsonSerialize(using = BloscCodec.CustomCompressorSerializer.class)
        public Blosc.Compressor cname = Blosc.Compressor.ZSTD;
        @JsonDeserialize(using = BloscCodec.CustomShuffleDeserializer.class)
        @JsonSerialize(using = BloscCodec.CustomShuffleSerializer.class)
        public Blosc.Shuffle shuffle;
        public int clevel = 5;
        public int typesize = 0;
        public int blocksize = 0;
    }
}
