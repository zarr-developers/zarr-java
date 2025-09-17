package dev.zarr.zarrjava.v2.codec;

import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v2.codec.core.ZlibCodec;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v2.codec.core.*;

import java.util.ArrayList;
import java.util.List;

public class CodecBuilder extends dev.zarr.zarrjava.core.codec.CodecBuilder {

  protected List<Codec> codecs;

  public CodecBuilder(DataType dataType) {
    super(dataType);
    this.codecs = new ArrayList<>();
  }

  public CodecBuilder withBlosc(
      Blosc.Compressor cname, Blosc.Shuffle shuffle, int clevel, int typeSize,
      int blockSize
  ) {
    try {
      codecs.add(new BloscCodec(cname, shuffle, clevel, typeSize, blockSize));
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

  public CodecBuilder withZlib(int level) {
    try {
      codecs.add(new ZlibCodec(level));
    } catch (ZarrException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public CodecBuilder withZlib() {
      return withZlib(5);
  }

  public Codec[] build() {
    return codecs.toArray(new Codec[0]);
  }

}
