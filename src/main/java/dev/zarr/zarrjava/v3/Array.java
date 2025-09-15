package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.codec.CodecPipeline;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class Array extends Node implements dev.zarr.zarrjava.interfaces.Array {

  public ArrayMetadata metadata;
  CodecPipeline codecPipeline;

  protected Array(StoreHandle storeHandle, ArrayMetadata arrayMetadata)
      throws ZarrException {
    super(storeHandle);
    this.metadata = arrayMetadata;
    this.codecPipeline = new CodecPipeline(arrayMetadata.codecs, arrayMetadata.coreArrayMetadata);
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
                Utils.toArray(storeHandle.resolve(ZARR_JSON).readNonNull()),
                ArrayMetadata.class
            )
    );
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
    StoreHandle metadataHandle = storeHandle.resolve(ZARR_JSON);
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

  /**
   * Creates a new Zarr array at a specified storage location. This method provides a callback that
   * gets an ArrayMetadataBuilder and needs to return such an ArrayMetadataBuilder. The callback can
   * be used to construct the metadata of the Zarr array. If `existsOk` is false, this method will
   * raise an exception if a Zarr array already exists at the specified storage location.
   *
   * @param storeHandle the storage location of the Zarr array
   * @param arrayMetadataBuilderMapper a callback of ArrayMetadataBuilder -> ArrayMetadataBuilder that is
   *                                   used to construct the metadata of the Zarr array
   * @param existsOk if true, no exception is raised if the Zarr array already exists
   * @throws IOException if the metadata cannot be serialized
   * @throws ZarrException if the Zarr array cannot be created
   */
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
  public CodecPipeline codecPipeline() {
    return codecPipeline;
  }




  @Override
  public ArrayMetadata metadata() {
    return metadata;
  }



  private Array writeMetadata(ArrayMetadata newArrayMetadata) throws ZarrException, IOException {
    ObjectMapper objectMapper = makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(newArrayMetadata));
    storeHandle.resolve(ZARR_JSON)
        .set(metadataBytes);
    return new Array(storeHandle, newArrayMetadata);
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
  public Array setAttributes(Map<String, Object> newAttributes) throws ZarrException, IOException {
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
   * @param attributeMapper a callback of Map<String, Object> -> Map<String, Object> that is used to construct the new
   *                        attributes
   * @throws ZarrException throws ZarrException if the new metadata is invalid
   * @throws IOException throws IOException if the new metadata cannot be serialized
   */
  public Array updateAttributes(Function<Map<String, Object>, Map<String, Object>> attributeMapper)
      throws ZarrException, IOException {
    return setAttributes(attributeMapper.apply(new HashMap<String, Object>(metadata.attributes) {
    }));
  }

  @Override
  public String toString() {
    return String.format("<v3.Array {%s} (%s) %s>", storeHandle,
        Arrays.stream(metadata.shape)
            .mapToObj(Long::toString)
            .collect(Collectors.joining(", ")),
        metadata.dataType
    );
  }


}
