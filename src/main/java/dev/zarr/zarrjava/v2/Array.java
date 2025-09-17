package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.core.codec.CodecPipeline;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.v2.codec.CodecRegistry;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Array implements dev.zarr.zarrjava.core.Array {

  static final String ZARRAY = ".zarray";
  public ArrayMetadata metadata;
  public StoreHandle storeHandle;
  CodecPipeline codecPipeline;

  protected Array(StoreHandle storeHandle, ArrayMetadata arrayMetadata) throws IOException, ZarrException {
    this.storeHandle = storeHandle;
    this.metadata = arrayMetadata;
    this.codecPipeline = new CodecPipeline(Utils.concatArrays(
        new dev.zarr.zarrjava.core.codec.Codec[]{},
        metadata.filters == null ? new Codec[]{} : metadata.filters,
        new dev.zarr.zarrjava.core.codec.Codec[]{new BytesCodec(arrayMetadata.endianness.toEndian())},
        metadata.compressor == null ? new Codec[]{} : new Codec[]{metadata.compressor}
    ), metadata.coreArrayMetadata);
  }

  /**
   * Opens an existing Zarr array at a specified storage location.
   *
   * @param storeHandle the storage location of the Zarr array
   * @throws IOException throws IOException if the metadata cannot be read
   * @throws ZarrException throws ZarrException if the Zarr array cannot be opened
   */
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

  public static ObjectMapper makeObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
    return objectMapper;
  }


  /**
   * Creates a new Zarr array with the provided metadata at a specified storage location. This
   * method will raise an exception if a Zarr array already exists at the specified storage
   * location.
   *
   * @param storeHandle the storage location of the Zarr array
   * @param arrayMetadata the metadata of the Zarr array
   * @throws IOException if the metadata cannot be serialized
   * @throws ZarrException if the Zarr array cannot be created
   */
  public static Array create(StoreHandle storeHandle, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
    return Array.create(storeHandle, arrayMetadata, false);
  }

  /**
   * Creates a new Zarr array with the provided metadata at a specified storage location. If
   * `existsOk` is false, this method will raise an exception if a Zarr array already exists at the
   * specified storage location.
   *
   * @param storeHandle the storage location of the Zarr array
   * @param arrayMetadata the metadata of the Zarr array
   * @param existsOk if true, no exception is raised if the Zarr array already exists
   * @throws IOException throws IOException if the metadata cannot be serialized
   * @throws ZarrException throws ZarrException if the Zarr array cannot be created
   */
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
