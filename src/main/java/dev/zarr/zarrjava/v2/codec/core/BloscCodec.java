package dev.zarr.zarrjava.v2.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v2.codec.Codec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BloscCodec extends dev.zarr.zarrjava.core.codec.core.BloscCodec implements Codec {

    /**
     * Value of 'shuffle' that lets Blosc pick the shuffle variant at write time: bit shuffle for
     * single-byte items, byte shuffle otherwise.
     */
    private static final int AUTO_SHUFFLE = -1;

    @JsonIgnore
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

    /**
     * True if 'shuffle' was given as {@link #AUTO_SHUFFLE} and the variant still needs to be
     * derived from the item size.
     */
    @JsonIgnore
    public final boolean autoShuffle;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    BloscCodec(
            @Nonnull @JsonProperty(value = "cname", defaultValue = "zstd")
            @JsonDeserialize(using = CustomCompressorDeserializer.class)
            Blosc.Compressor cname,
            @JsonProperty(value = "shuffle", defaultValue = "0") int shuffle,
            @JsonProperty(value = "clevel", defaultValue = "5") int clevel,
            @JsonProperty(value = "typesize", defaultValue = "0") int typesize,
            @JsonProperty(value = "blocksize", defaultValue = "0") int blocksize
    ) throws ZarrException {
        this(cname, parseShuffle(shuffle, typesize), shuffle == AUTO_SHUFFLE, clevel, typesize,
                blocksize
        );
    }

    public BloscCodec(
            @Nonnull Blosc.Compressor cname, @Nonnull Blosc.Shuffle shuffle, int clevel,
            int typesize, int blocksize
    ) throws ZarrException {
        this(cname, shuffle, false, clevel, typesize, blocksize);
    }

    private BloscCodec(
            @Nonnull Blosc.Compressor cname, @Nonnull Blosc.Shuffle shuffle, boolean autoShuffle,
            int clevel, int typesize, int blocksize
    ) throws ZarrException {
        if (clevel < 0 || clevel > 9) {
            throw new ZarrException("'clevel' needs to be between 0 and 9.");
        }
        this.cname = cname;
        this.shuffle = shuffle;
        this.autoShuffle = autoShuffle;
        this.clevel = clevel;
        this.typesize = typesize;
        this.blocksize = blocksize;
    }

    private static Blosc.Shuffle parseShuffle(int shuffle, int typesize) throws ZarrException {
        if (shuffle == AUTO_SHUFFLE) {
            return autoShuffle(typesize);
        }
        Blosc.Shuffle parsedShuffle = Blosc.Shuffle.fromInt(shuffle);
        if (parsedShuffle == null) {
            throw new ZarrException(
                    String.format("Could not parse the Blosc.Shuffle. Got '%d'", shuffle));
        }
        return parsedShuffle;
    }

    /**
     * Blosc bit-shuffles single-byte items and byte-shuffles everything else. A 'typesize' of 0
     * means that the item size is not known yet, in which case the variant is derived again in
     * {@link #evolveFromCoreArrayMetadata}.
     */
    private static Blosc.Shuffle autoShuffle(int typesize) {
        return typesize == 1 ? Blosc.Shuffle.BIT_SHUFFLE : Blosc.Shuffle.BYTE_SHUFFLE;
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

    @Override
    public BloscCodec evolveFromCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        if (typesize == 0) {
            int evolvedTypesize = arrayMetadata.dataType.getByteCount();
            return new BloscCodec(
                    this.cname,
                    this.autoShuffle ? autoShuffle(evolvedTypesize) : this.shuffle,
                    this.autoShuffle,
                    this.clevel,
                    evolvedTypesize,
                    this.blocksize
            );
        }
        return this;
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
}
