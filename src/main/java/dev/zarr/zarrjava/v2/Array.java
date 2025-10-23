package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
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
    ObjectMapper mapper = makeObjectMapper();
    ArrayMetadata metadata = mapper.readValue(
        Utils.toArray(storeHandle.resolve(ZARRAY).readNonNull()),
        ArrayMetadata.class
    );
    if (storeHandle.resolve(ZATTRS).exists())
      metadata.attributes = mapper.readValue(
          Utils.toArray(storeHandle.resolve(ZATTRS).readNonNull()),
          Attributes.class
      );
    return new Array(
        storeHandle,
        metadata
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
    ObjectMapper objectMapper = makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(arrayMetadata));
    if (arrayMetadata.attributes != null) {
      StoreHandle attrsHandle = storeHandle.resolve(ZATTRS);
      ByteBuffer attrsBytes = ByteBuffer.wrap(
          objectMapper.writeValueAsBytes(arrayMetadata.attributes));
      attrsHandle.set(attrsBytes);
    }
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

  private Array writeMetadata(ArrayMetadata newArrayMetadata) throws ZarrException, IOException {
    return Array.create(storeHandle, newArrayMetadata, true);
  }

  /**
   * Sets a new shape for the Zarr array. It only changes the metadata, no array data is modified or
   * deleted. This method returns a new instance of the Zarr array class and the old instance
   * becomes invalid.
   *
   * @param newShape the new shape of the Zarr array
   * @throws ZarrException if the new metadata is invalid
   * @throws IOException throws IOException if the new metadata cannot be serialized
   */
  public Array resize(long[] newShape) throws ZarrException, IOException {
    //TODO: test
    if (newShape.length != metadata.ndim()) {
      throw new IllegalArgumentException(
          "'newShape' needs to have rank '" + metadata.ndim() + "'.");
    }

    ArrayMetadata newArrayMetadata = ArrayMetadataBuilder.fromArrayMetadata(metadata)
        .withShape(newShape)
        .build();
    return writeMetadata(newArrayMetadata);
  }

  /**
   * Sets the attributes of the Zarr array. It overwrites and removes any existing attributes. This
   * method returns a new instance of the Zarr array class and the old instance becomes invalid.
   *
   * @param newAttributes the new attributes of the Zarr array
   * @throws ZarrException throws ZarrException if the new metadata is invalid
   * @throws IOException throws IOException if the new metadata cannot be serialized
   */
  public Array setAttributes(Attributes newAttributes) throws ZarrException, IOException {
    ArrayMetadata newArrayMetadata =
        ArrayMetadataBuilder.fromArrayMetadata(metadata)
            .withAttributes(newAttributes)
            .build();
    return writeMetadata(newArrayMetadata);
  }

  /**
   * Updates the attributes of the Zarr array. It provides a callback that gets the current
   * attributes as input and needs to return the new set of attributes. The attributes in the
   * callback may be mutated. This method overwrites and removes any existing attributes. This
   * method returns a new instance of the Zarr array class and the old instance becomes invalid.
   *
   * @param attributeMapper the callback that is used to construct the new attributes
   * @throws ZarrException throws ZarrException if the new metadata is invalid
   * @throws IOException   throws IOException if the new metadata cannot be serialized
   */
  public Array updateAttributes(Function<Attributes, Attributes> attributeMapper) throws ZarrException, IOException {
    return setAttributes(attributeMapper.apply(metadata.attributes));
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
