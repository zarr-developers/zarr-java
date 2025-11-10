package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectWriter;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.core.codec.CodecPipeline;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.v2.codec.core.BytesCodec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import static dev.zarr.zarrjava.v2.Node.makeObjectMapper;
import static dev.zarr.zarrjava.v2.Node.makeObjectWriter;

public class Array extends dev.zarr.zarrjava.core.Array implements Node {

  private final ArrayMetadata metadata;

  public ArrayMetadata metadata() {
      return metadata;
  }

  protected Array(StoreHandle storeHandle, ArrayMetadata arrayMetadata) throws IOException, ZarrException {
    super(storeHandle);
    this.metadata = arrayMetadata;
    this.codecPipeline = new CodecPipeline(Utils.concatArrays(
        new Codec[]{},
        metadata.filters == null ? new Codec[]{} : metadata.filters,
        new Codec[]{new BytesCodec(arrayMetadata.endianness.toEndian())},
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

  /**
   * Opens an existing Zarr array at a specified storage location.
   *
   * @param path the storage location of the Zarr array
   * @throws IOException throws IOException if the metadata cannot be read
   * @throws ZarrException throws ZarrException if the Zarr array cannot be opened
   */
  public static Array open(Path path) throws IOException, ZarrException {
    return open(new StoreHandle(new FilesystemStore(path)));
  }

    /**
     * Opens an existing Zarr array at a specified storage location.
     *
     * @param path the storage location of the Zarr array
     * @throws IOException throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr array cannot be opened
     */
    public static Array open(String path) throws IOException, ZarrException {
      return open(Paths.get(path));
    }

  /**
   * Creates a new Zarr array with the provided metadata at a specified storage location. This
   * method will raise an exception if a Zarr array already exists at the specified storage
   * location.
   *
   * @param path the storage location of the Zarr array
   * @param arrayMetadata the metadata of the Zarr array
   * @throws IOException if the metadata cannot be serialized
   * @throws ZarrException if the Zarr array cannot be created
   */
  public static Array create(Path path, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
      return create(new StoreHandle(new FilesystemStore(path)), arrayMetadata);
  }

  /**
   * Creates a new Zarr array with the provided metadata at a specified storage location. This
   * method will raise an exception if a Zarr array already exists at the specified storage
   * location.
   *
   * @param path the storage location of the Zarr array
   * @param arrayMetadata the metadata of the Zarr array
   * @throws IOException if the metadata cannot be serialized
   * @throws ZarrException if the Zarr array cannot be created
   */
  public static Array create(String path, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
      return create(Paths.get(path), arrayMetadata);
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
    ObjectWriter objectWriter = makeObjectWriter();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectWriter.writeValueAsBytes(arrayMetadata));
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
}
