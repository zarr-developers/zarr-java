package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.store.StoreHandle;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.core.EndianCodec;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class CodecPipeline {
    @Nonnull
    final Codec[] codecs;

    public CodecPipeline(@Nullable Codec[] codecs) throws ZarrException {
        if (codecs == null) {
            this.codecs = new Codec[0];
        } else {
            Codec prevCodec = null;
            for (Codec codec : codecs) {
                if (prevCodec != null) {
                    if (codec instanceof ArrayBytesCodec && prevCodec instanceof ArrayBytesCodec) {
                        throw new ZarrException(
                                "ArrayBytesCodec '" + codec.getClass() + "' cannot follow after ArrayBytesCodec '" +
                                        prevCodec.getClass() + "' because only 1 ArrayBytesCodec is allowed.");
                    }
                    if (codec instanceof ArrayBytesCodec && prevCodec instanceof BytesBytesCodec) {
                        throw new ZarrException(
                                "ArrayBytesCodec '" + codec.getClass() + "' cannot follow after BytesBytesCodec '" +
                                        prevCodec.getClass() + "'.");
                    }
                    if (codec instanceof ArrayArrayCodec && prevCodec instanceof ArrayBytesCodec) {
                        throw new ZarrException(
                                "ArrayArrayCodec '" + codec.getClass() + "' cannot follow after ArrayBytesCodec '" +
                                        prevCodec.getClass() + "'.");
                    }
                    if (codec instanceof ArrayArrayCodec && prevCodec instanceof BytesBytesCodec) {
                        throw new ZarrException(
                                "ArrayArrayCodec '" + codec.getClass() + "' cannot follow after BytesBytesCodec '" +
                                        prevCodec.getClass() + "'.");
                    }
                }
                prevCodec = codec;
            }

            this.codecs = codecs;
        }
    }

    ArrayArrayCodec[] getArrayArrayCodecs() {
        return Arrays.stream(codecs).filter(c -> c instanceof ArrayArrayCodec).toArray(ArrayArrayCodec[]::new);
    }

    ArrayBytesCodec getArrayBytesCodec() {
        for (Codec codec : codecs) if (codec instanceof ArrayBytesCodec) return (ArrayBytesCodec) codec;
        return new EndianCodec(new EndianCodec.Configuration(EndianCodec.Endian.LITTLE));
    }

    BytesBytesCodec[] getBytesBytesCodecs() {
        return Arrays.stream(codecs).filter(c -> c instanceof BytesBytesCodec).toArray(BytesBytesCodec[]::new);
    }

    @Nonnull
    public Array decode(
            @Nonnull ByteBuffer chunkBytes,
            @Nonnull ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        for (BytesBytesCodec codec : getBytesBytesCodecs()) {
            chunkBytes = codec.decode(chunkBytes, arrayMetadata);
        }

        Array chunkArray = getArrayBytesCodec().decode(chunkBytes, arrayMetadata);

        for (ArrayArrayCodec codec : getArrayArrayCodecs()) {
            chunkArray = codec.decode(chunkArray, arrayMetadata);
        }
        return chunkArray;
    }

    @Nonnull
    public ByteBuffer encode(
            @Nonnull Array chunkArray, @Nonnull ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        for (ArrayArrayCodec codec : getArrayArrayCodecs()) {
            chunkArray = codec.encode(chunkArray, arrayMetadata);
        }

        ByteBuffer chunkBytes = getArrayBytesCodec().encode(chunkArray, arrayMetadata);

        for (BytesBytesCodec codec : getBytesBytesCodecs()) {
            chunkBytes = codec.encode(chunkBytes, arrayMetadata);
        }
        return chunkBytes;
    }

    public Array partialDecode(StoreHandle valueHandle, long[] offset, int[] shape,
                               ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return null; // TODO
    }

    public ByteBuffer partialEncode(StoreHandle oldValueHandle, Array array, long[] offset, int[] shape,
                                    ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return null; // TODO
    }
}
