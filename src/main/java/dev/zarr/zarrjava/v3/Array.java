package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.IndexingUtils;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
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
import javax.annotation.Nullable;
import ucar.ma2.InvalidRangeException;

public class Array extends Node {

  public ArrayMetadata metadata;
  CodecPipeline codecPipeline;

  protected Array(StoreHandle storeHandle, ArrayMetadata arrayMetadata)
      throws IOException, ZarrException {
    super(storeHandle);
    this.metadata = arrayMetadata;
    this.codecPipeline = new CodecPipeline(arrayMetadata.codecs);
  }

  /**
   * Opens an existing Zarr array at a specified storage location.
   *
   * @param storeHandle
   * @throws IOException
   * @throws ZarrException
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
   * @param storeHandle
   * @param arrayMetadata
   * @throws IOException
   * @throws ZarrException
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
   * @param storeHandle
   * @param arrayMetadata
   * @param existsOk
   * @throws IOException
   * @throws ZarrException
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
   * @param storeHandle
   * @param arrayMetadataBuilderMapper
   * @param existsOk
   * @throws IOException
   * @throws ZarrException
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

  /**
   * Reads the entire Zarr array into an ucar.ma2.Array.
   *
   * @throws ZarrException
   */
  @Nonnull
  public ucar.ma2.Array read() throws ZarrException {
    return read(new long[metadata.ndim()], Utils.toIntArray(metadata.shape));
  }

  /**
   * Reads a part of the Zarr array based on a requested offset and shape into an ucar.ma2.Array.
   *
   * @param offset
   * @param shape
   * @throws ZarrException
   */
  @Nonnull
  public ucar.ma2.Array read(final long[] offset, final int[] shape) throws ZarrException {
    if (offset.length != metadata.ndim()) {
      throw new IllegalArgumentException("'offset' needs to have rank '" + metadata.ndim() + "'.");
    }
    if (shape.length != metadata.ndim()) {
      throw new IllegalArgumentException("'shape' needs to have rank '" + metadata.ndim() + "'.");
    }

    final int[] chunkShape = metadata.chunkShape();
    if (IndexingUtils.isSingleFullChunk(offset, shape, chunkShape)) {
      return readChunk(IndexingUtils.computeSingleChunkCoords(offset, chunkShape));
    }

    final ucar.ma2.Array outputArray = ucar.ma2.Array.factory(metadata.dataType.getMA2DataType(),
        shape);
    Arrays.stream(IndexingUtils.computeChunkCoords(metadata.shape, chunkShape, offset, shape))
        .forEach(
            chunkCoords -> {
              try {
                final IndexingUtils.ChunkProjection chunkProjection =
                    IndexingUtils.computeProjection(chunkCoords, metadata.shape, chunkShape, offset,
                        shape
                    );

                if (chunkIsInArray(chunkCoords)) {
                  MultiArrayUtils.copyRegion(metadata.allocateFillValueChunk(),
                      chunkProjection.chunkOffset, outputArray, chunkProjection.outOffset,
                      chunkProjection.shape
                  );
                }

                final String[] chunkKeys = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
                final StoreHandle chunkHandle = storeHandle.resolve(chunkKeys);

                if (codecPipeline.supportsPartialDecode()) {
                  final ucar.ma2.Array chunkArray = codecPipeline.decodePartial(chunkHandle,
                      Utils.toLongArray(chunkProjection.chunkOffset), chunkProjection.shape,
                      metadata.coreArrayMetadata);
                  MultiArrayUtils.copyRegion(chunkArray, new int[metadata.ndim()], outputArray,
                      chunkProjection.outOffset, chunkProjection.shape
                  );
                } else {
                  MultiArrayUtils.copyRegion(readChunk(chunkCoords), chunkProjection.chunkOffset,
                      outputArray, chunkProjection.outOffset, chunkProjection.shape
                  );
                }

              } catch (ZarrException e) {
                throw new RuntimeException(e);
              }
            });
    return outputArray;
  }

  boolean chunkIsInArray(long[] chunkCoords) {
    final int[] chunkShape = metadata.chunkShape();
    for (int dimIdx = 0; dimIdx < metadata.ndim(); dimIdx++) {
      if (chunkCoords[dimIdx] < 0
          || chunkCoords[dimIdx] * chunkShape[dimIdx] >= metadata.shape[dimIdx]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Reads one chunk of the Zarr array as specified by the chunk coordinates into an
   * ucar.ma2.Array.
   *
   * @param chunkCoords The coordinates of the chunk as computed by the offset of the chunk divided
   *                    by the chunk shape.
   * @throws ZarrException
   */
  @Nonnull
  public ucar.ma2.Array readChunk(long[] chunkCoords)
      throws ZarrException {
    if (chunkIsInArray(chunkCoords)) {
      return metadata.allocateFillValueChunk();
    }

    final String[] chunkKeys = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
    final StoreHandle chunkHandle = storeHandle.resolve(chunkKeys);

    ByteBuffer chunkBytes = chunkHandle.read();
    if (chunkBytes == null) {
      return metadata.allocateFillValueChunk();
    }

    return codecPipeline.decode(chunkBytes, metadata.coreArrayMetadata);
  }

  /**
   * Writes a ucar.ma2.Array into the Zarr array at the beginning of the Zarr array. The shape of
   * the Zarr array needs be large enough for the write.
   *
   * @param array
   */
  public void write(ucar.ma2.Array array) {
    write(new long[metadata.ndim()], array);
  }

  /**
   * Writes a ucar.ma2.Array into the Zarr array at a specified offset. The shape of the Zarr array
   * needs be large enough for the write.
   *
   * @param offset
   * @param array
   */
  public void write(long[] offset, ucar.ma2.Array array) {
    if (offset.length != metadata.ndim()) {
      throw new IllegalArgumentException("'offset' needs to have rank '" + metadata.ndim() + "'.");
    }
    if (array.getRank() != metadata.ndim()) {
      throw new IllegalArgumentException("'array' needs to have rank '" + metadata.ndim() + "'.");
    }

    int[] shape = array.getShape();

    final int[] chunkShape = metadata.chunkShape();
    Arrays.stream(IndexingUtils.computeChunkCoords(metadata.shape, chunkShape, offset, shape))
        .forEach(
            chunkCoords -> {
              try {
                final IndexingUtils.ChunkProjection chunkProjection =
                    IndexingUtils.computeProjection(chunkCoords, metadata.shape, chunkShape, offset,
                        shape
                    );

                ucar.ma2.Array chunkArray;
                if (IndexingUtils.isFullChunk(chunkProjection.chunkOffset, chunkProjection.shape,
                    chunkShape
                )) {
                  chunkArray = array.sectionNoReduce(chunkProjection.outOffset,
                      chunkProjection.shape,
                      null
                  );
                } else {
                  chunkArray = readChunk(chunkCoords);
                  MultiArrayUtils.copyRegion(array, chunkProjection.outOffset, chunkArray,
                      chunkProjection.chunkOffset, chunkProjection.shape
                  );
                }
                writeChunk(chunkCoords, chunkArray);
              } catch (ZarrException | InvalidRangeException e) {
                throw new RuntimeException(e);
              }
            });
  }

  /**
   * Writes one chunk into the Zarr array as specified by the chunk coordinates. The shape of the
   * Zarr array needs be large enough for the write.
   *
   * @param chunkCoords
   * @param chunkArray
   * @throws ZarrException
   */
  public void writeChunk(long[] chunkCoords, ucar.ma2.Array chunkArray) throws ZarrException {
    String[] chunkKeys = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
    StoreHandle chunkHandle = storeHandle.resolve(chunkKeys);

    if (MultiArrayUtils.allValuesEqual(chunkArray, metadata.parsedFillValue)) {
      chunkHandle.delete();
    } else {
      ByteBuffer chunkBytes = codecPipeline.encode(chunkArray, metadata.coreArrayMetadata);
      chunkHandle.set(chunkBytes);
    }
  }

  public ArrayAccessor access() {
    return new ArrayAccessor(this);
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
   * @param newShape
   * @throws ZarrException
   * @throws IOException
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
   * @param newAttributes
   * @throws ZarrException
   * @throws IOException
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
   * @param attributeMapper
   * @throws ZarrException
   * @throws IOException
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

  public static final class ArrayAccessor {

    @Nullable
    long[] offset;
    @Nullable
    int[] shape;
    @Nonnull
    Array array;

    private ArrayAccessor(@Nonnull Array array) {
      this.array = array;
    }

    @Nonnull
    public ArrayAccessor withOffset(@Nonnull long... offset) {
      this.offset = offset;
      return this;
    }


    @Nonnull
    public ArrayAccessor withShape(@Nonnull int... shape) {
      this.shape = shape;
      return this;
    }

    @Nonnull
    public ArrayAccessor withShape(@Nonnull long... shape) {
      this.shape = Utils.toIntArray(shape);
      return this;
    }

    @Nonnull
    public ucar.ma2.Array read() throws ZarrException {
      if (offset == null) {
        throw new ZarrException("`offset` needs to be set.");
      }
      if (shape == null) {
        throw new ZarrException("`shape` needs to be set.");
      }
      return array.read(offset, shape);
    }

    public void write(@Nonnull ucar.ma2.Array content) throws ZarrException {
      if (offset == null) {
        throw new ZarrException("`offset` needs to be set.");
      }
      array.write(offset, content);
    }

  }
}
