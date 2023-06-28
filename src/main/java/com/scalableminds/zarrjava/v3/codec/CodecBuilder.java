package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.bloscjava.Blosc;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.DataType;
import com.scalableminds.zarrjava.v3.codec.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CodecBuilder {
    List<Codec> codecs;

    public CodecBuilder() {
        codecs = new ArrayList<>();
    }

    public CodecBuilder withBlosc(Blosc.Compressor cname, Blosc.Shuffle shuffle, int clevel, int typeSize, int blockSize) {
        try {
            codecs.add(new BloscCodec(new BloscCodec.Configuration(cname, shuffle, clevel, typeSize, blockSize)));
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CodecBuilder withBlosc(String cname, String shuffle, int clevel, DataType dataType, int blockSize) {
        return withBlosc(Blosc.Compressor.fromString(cname), Blosc.Shuffle.fromString(shuffle), clevel,
                dataType.getByteCount(), blockSize);
    }

    public CodecBuilder withBlosc(String cname, String shuffle, int clevel, DataType dataType) {
        return withBlosc(cname, shuffle, clevel, dataType, 0);
    }

    public CodecBuilder withBlosc(String cname, int clevel, DataType dataType) {
        return withBlosc(cname, "noshuffle", clevel, dataType);
    }

    public CodecBuilder withBlosc(String cname, DataType dataType) {
        return withBlosc(cname, 5, dataType);
    }

    public CodecBuilder withBlosc(DataType dataType) {
        return withBlosc("zstd", dataType);
    }

    public CodecBuilder withTranspose(String order) {
        try {
            codecs.add(new TransposeCodec(new TransposeCodec.Configuration(order)));
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CodecBuilder withEndian(EndianCodec.Endian endian) {
        codecs.add(new EndianCodec(new EndianCodec.Configuration(endian)));
        return this;
    }

    public CodecBuilder withEndian(String endian) {
        return withEndian(EndianCodec.Endian.valueOf(endian));
    }

    public CodecBuilder withGzip(int clevel) {
        try {
            codecs.add(new GzipCodec(new GzipCodec.Configuration(clevel)));
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CodecBuilder withGzip() {
        return withGzip(5);
    }

    public CodecBuilder withSharding(int[] chunkShape) {
        try {
            codecs.add(new ShardingIndexedCodec(new ShardingIndexedCodec.Configuration(chunkShape, null)));
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CodecBuilder withSharding(int[] chunkShape, Function<CodecBuilder, CodecBuilder> codecBuilder) {
        CodecBuilder nestedBuilder = new CodecBuilder();
        try {
            codecs.add(new ShardingIndexedCodec(
                    new ShardingIndexedCodec.Configuration(chunkShape, codecBuilder.apply(nestedBuilder).build())));
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public Codec[] build() {
        return codecs.toArray(new Codec[0]);
    }
}
