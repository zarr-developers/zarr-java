package dev.zarr.zarrjava.v3.codec;

import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.BloscCodec;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec.Configuration;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec.Endian;
import dev.zarr.zarrjava.v3.codec.core.Crc32cCodec;
import dev.zarr.zarrjava.v3.codec.core.GzipCodec;
import dev.zarr.zarrjava.v3.codec.core.ShardingIndexedCodec;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import dev.zarr.zarrjava.v3.codec.core.ZstdCodec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class CodecBuilder {

  final private DataType dataType;
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
    if (shuffle.equals("shuffle")){
      shuffle = "byteshuffle";
    }
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

  public CodecBuilder withTranspose(int[] order) {
      codecs.add(new TransposeCodec(new TransposeCodec.Configuration(order)));
      return this;
  }

  public CodecBuilder withBytes(Endian endian) {
    codecs.add(new BytesCodec(new BytesCodec.Configuration(endian)));
    return this;
  }

  public CodecBuilder withBytes(String endian) {
    return withBytes(BytesCodec.Endian.valueOf(endian));
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

  public CodecBuilder withZstd(int clevel, boolean checksum) {
    try {
      codecs.add(new ZstdCodec(new ZstdCodec.Configuration(clevel, checksum)));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public CodecBuilder withZstd() {
    return withZstd(5, true);
  }

  public CodecBuilder withZstd(int clevel) {
    return withZstd(clevel, true);
  }

  public CodecBuilder withSharding(int[] chunkShape) {
    try {
      codecs.add(
              new ShardingIndexedCodec(new ShardingIndexedCodec.Configuration(chunkShape,
                      new Codec[]{new BytesCodec(new Configuration(Endian.LITTLE))},
                      new Codec[]{new BytesCodec(new Configuration(Endian.LITTLE)), new Crc32cCodec()},
                      "end")));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public CodecBuilder withSharding(int[] chunkShape,
                                   Function<CodecBuilder, CodecBuilder> codecBuilder) {
    return withSharding(chunkShape, codecBuilder, "end");
  }

  public CodecBuilder withSharding(int[] chunkShape,
                                   Function<CodecBuilder, CodecBuilder> codecBuilder, String indexLocation) {
    CodecBuilder nestedBuilder = new CodecBuilder(dataType);
    try {
      codecs.add(new ShardingIndexedCodec(
              new ShardingIndexedCodec.Configuration(chunkShape,
                      codecBuilder.apply(nestedBuilder).build(),
                      new Codec[]{new BytesCodec(Endian.LITTLE), new Crc32cCodec()},
                      indexLocation)));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public CodecBuilder withCrc32c() {
      codecs.add(new Crc32cCodec());
      return this;
  }
  private void autoInsertBytesCodec() {
    if (codecs.stream().noneMatch(c -> c instanceof ArrayBytesCodec)) {
      Codec[] arrayArrayCodecs = codecs.stream().filter(c -> c instanceof ArrayArrayCodec)
          .toArray(Codec[]::new);
      Codec[] bytesBytesCodecs = codecs.stream().filter(c -> c instanceof BytesBytesCodec)
          .toArray(Codec[]::new);
      this.codecs = new ArrayList<>();
      Collections.addAll(this.codecs, arrayArrayCodecs);
      this.codecs.add(new BytesCodec(new BytesCodec.Configuration(Endian.LITTLE)));
      Collections.addAll(this.codecs, bytesBytesCodecs);
    }
  }

  public Codec[] build() {
    return build(true);
  }

  public Codec[] build(boolean includeBytesCodec) {
    if (includeBytesCodec) {
      autoInsertBytesCodec();
    }
    return codecs.toArray(new Codec[0]);
  }
}
