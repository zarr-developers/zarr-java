package com.scalableminds.zarrjava.v3.codec;

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

    public CodecPipeline(@Nullable Codec[] codecs) {
        if (codecs == null) {
            this.codecs = new Codec[0];
        } else {
            Codec prevCodec = null;
            for (Codec codec : codecs) {
                assert prevCodec == null || codec instanceof BytesBytesCodec ||
                        ((codec instanceof ArrayBytesCodec || codec instanceof ArrayArrayCodec) &&
                                prevCodec instanceof ArrayArrayCodec);
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
    public Array decode(@Nonnull ByteBuffer chunkBytes, @Nonnull ArrayMetadata.CoreArrayMetadata arrayMetadata) {
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
    public ByteBuffer encode(@Nonnull Array chunkArray, @Nonnull ArrayMetadata.CoreArrayMetadata arrayMetadata) {
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
