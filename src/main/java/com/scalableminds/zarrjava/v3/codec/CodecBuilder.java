package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.bloscjava.Blosc;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.DataType;
import com.scalableminds.zarrjava.v3.codec.core.BloscCodec;
import com.scalableminds.zarrjava.v3.codec.core.EndianCodec;
import com.scalableminds.zarrjava.v3.codec.core.GzipCodec;
import com.scalableminds.zarrjava.v3.codec.core.ShardingIndexedCodec;
import com.scalableminds.zarrjava.v3.codec.core.TransposeCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CodecBuilder {

  private DataType dataType;
  private List<Codec> codecs;

  public CodecBuilder(DataType dataType) {
    this.dataType = dataType;
    this.codecs = new ArrayList<>();
  }

  public CodecBuilder withBlosc(
      Blosc.Compressor cname, Blosc.Shuffle shuffle, int clevel, int typeSize,
      int blockSize
  ) {
    try {
      codecs.add(new BloscCodec(
          new BloscCodec.Configuration(cname, shuffle, clevel, typeSize, blockSize)));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public CodecBuilder withBlosc(String cname, String shuffle, int clevel, int blockSize) {
    return withBlosc(Blosc.Compressor.fromString(cname), Blosc.Shuffle.fromString(shuffle), clevel,
        dataType.getByteCount(), blockSize
    );
  }

  public CodecBuilder withBlosc(String cname, String shuffle, int clevel) {
    return withBlosc(cname, shuffle, clevel, 0);
  }

  public CodecBuilder withBlosc(String cname, int clevel) {
    return withBlosc(cname, "noshuffle", clevel);
  }

  public CodecBuilder withBlosc(String cname) {
    return withBlosc(cname, 5);
  }

  public CodecBuilder withBlosc() {
    return withBlosc("zstd");
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
      codecs.add(
          new ShardingIndexedCodec(new ShardingIndexedCodec.Configuration(chunkShape, null)));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public CodecBuilder withSharding(int[] chunkShape,
      Function<CodecBuilder, CodecBuilder> codecBuilder) {
    CodecBuilder nestedBuilder = new CodecBuilder(dataType);
    try {
      codecs.add(new ShardingIndexedCodec(
          new ShardingIndexedCodec.Configuration(chunkShape, codecBuilder.apply(nestedBuilder)
              .build())));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public Codec[] build() {
    return codecs.toArray(new Codec[0]);
  }
}
