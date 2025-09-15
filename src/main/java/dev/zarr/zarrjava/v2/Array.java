package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.IndexingUtils;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.codec.CodecPipeline;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import ucar.ma2.InvalidRangeException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;

public class Array implements dev.zarr.zarrjava.interfaces.Array {

  static final String ZARRAY = ".zarray";
  public ArrayMetadata metadata;
  public StoreHandle storeHandle;
  CodecPipeline codecPipeline;

  protected Array(StoreHandle storeHandle, ArrayMetadata arrayMetadata) throws IOException, ZarrException {
    this.storeHandle = storeHandle;
    this.metadata = arrayMetadata;
    this.codecPipeline = new CodecPipeline(Utils.concatArrays(
        metadata.filters,
        new Codec[]{new BytesCodec(arrayMetadata.endianness.toEndian())},
        metadata.compressor == null ? new Codec[]{} : new Codec[]{metadata.compressor}
    ), metadata.coreArrayMetadata);
  }

  public static Array open(StoreHandle storeHandle) throws IOException, ZarrException {
    return new Array(
        storeHandle,
        makeObjectMapper()
            .readValue(
                Utils.toArray(storeHandle.resolve(ZARRAY).readNonNull()),
                ArrayMetadata.class
            )
    );
  }

  public static Array create(StoreHandle storeHandle, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
    return Array.create(storeHandle, arrayMetadata, false);
  }

  public static Array create(StoreHandle storeHandle, ArrayMetadata arrayMetadata, boolean existsOk)
      throws IOException, ZarrException {
    StoreHandle metadataHandle = storeHandle.resolve(ZARRAY);
    if (!existsOk && metadataHandle.exists()) {
      throw new RuntimeException(
          "Trying to create a new array in " + storeHandle + ". But " + metadataHandle
              + " already exists.");
    }
    ObjectMapper objectMapper = makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(arrayMetadata));
    metadataHandle.set(metadataBytes);
    return new Array(storeHandle, arrayMetadata);
  }

  public static Array create(StoreHandle storeHandle,
                             Function<ArrayMetadataBuilder, ArrayMetadataBuilder> arrayMetadataBuilderMapper,
                             boolean existsOk) throws IOException, ZarrException {
    return create(storeHandle,
        arrayMetadataBuilderMapper.apply(new ArrayMetadataBuilder()).build(), existsOk);
  }

  @Nonnull
  public static ArrayMetadataBuilder metadataBuilder() {
    return new ArrayMetadataBuilder();
  }

  @Nonnull
  public static ArrayMetadataBuilder metadataBuilder(ArrayMetadata existingMetadata) {
    return ArrayMetadataBuilder.fromArrayMetadata(existingMetadata);
  }



  @Override
  public String toString() {
    return String.format("<v2.Array {%s} (%s) %s>", storeHandle,
        Arrays.stream(metadata.shape)
            .mapToObj(Long::toString)
            .collect(Collectors.joining(", ")),
        metadata.dataType
    );
  }

  @Override
  public ArrayMetadata metadata() {
    return metadata;
  }

  @Override
  public StoreHandle storeHandle() {
    return storeHandle;
  }

  @Override
  public CodecPipeline codecPipeline() {
    return codecPipeline;
  }

}
