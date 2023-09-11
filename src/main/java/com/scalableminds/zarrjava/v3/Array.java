package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.store.StoreHandle;
import com.scalableminds.zarrjava.utils.IndexingUtils;
import com.scalableminds.zarrjava.utils.MultiArrayUtils;
import com.scalableminds.zarrjava.utils.Utils;
import com.scalableminds.zarrjava.v3.codec.CodecPipeline;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
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

  public static Array open(StoreHandle storeHandle) throws IOException, ZarrException {
    return new Array(
        storeHandle,
        Node.makeObjectMapper()
            .readValue(
                storeHandle.resolve(ZARR_JSON)
                    .readNonNull()
                    .array(),
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
    StoreHandle metadataHandle = storeHandle.resolve(ZARR_JSON);
    if (!existsOk && metadataHandle.exists()) {
      throw new RuntimeException(
          "Trying to create a new array in " + storeHandle + ". But " + metadataHandle
              + " already exists.");
    }
    ObjectMapper objectMapper = Node.makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(arrayMetadata));
    metadataHandle.set(metadataBytes);
    return new Array(storeHandle, arrayMetadata);
  }

  public static ArrayMetadataBuilder metadataBuilder() {
    return new ArrayMetadataBuilder();
  }

  public static ArrayMetadataBuilder metadataBuilder(ArrayMetadata existingMetadata) {
    return ArrayMetadataBuilder.fromArrayMetadata(existingMetadata);
  }

  public ucar.ma2.Array read() throws ZarrException {
    return read(new long[metadata.ndim()], Utils.toIntArray(metadata.shape));
  }

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

                final ucar.ma2.Array chunkArray = readChunk(chunkCoords);
                MultiArrayUtils.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray,
                    chunkProjection.outOffset, chunkProjection.shape
                );
              } catch (ZarrException e) {
                throw new RuntimeException(e);
              }
            });
    return outputArray;
  }

  @Nonnull
  public ucar.ma2.Array readChunk(long[] chunkCoords) throws ZarrException {
    final int[] chunkShape = metadata.chunkShape();

    for (int dimIdx = 0; dimIdx < metadata.ndim(); dimIdx++) {
      if (chunkCoords[dimIdx] < 0
          || chunkCoords[dimIdx] * chunkShape[dimIdx] >= metadata.shape[dimIdx]) {
        return metadata.allocateFillValueChunk();
      }
    }

    final String[] chunkKeys = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
    final StoreHandle chunkHandle = storeHandle.resolve(chunkKeys);

    ByteBuffer chunkBytes = chunkHandle.read();
    if (chunkBytes == null) {
      return metadata.allocateFillValueChunk();
    }

    ucar.ma2.Array chunkArray = codecPipeline.decode(chunkBytes, metadata.coreArrayMetadata);
    return chunkArray;
  }

  public void write(ucar.ma2.Array array) {
    write(new long[metadata.ndim()], array);
  }

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

  private Array writeMetadata(ArrayMetadata newArrayMetadata) throws ZarrException, IOException {
    ObjectMapper objectMapper = Node.makeObjectMapper();
    ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(newArrayMetadata));
    storeHandle.resolve(ZARR_JSON)
        .set(metadataBytes);
    return new Array(storeHandle, newArrayMetadata);
  }

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

  public Array setAttributes(Map<String, Object> newAttributes) throws ZarrException, IOException {
    ArrayMetadata newArrayMetadata =
        ArrayMetadataBuilder.fromArrayMetadata(metadata)
            .withAttributes(newAttributes)
            .build();
    return writeMetadata(newArrayMetadata);
  }

  public Array updateAttributes(Function<Map<String, Object>, Map<String, Object>> attributeMapper)
      throws ZarrException, IOException {
    return setAttributes(attributeMapper.apply(metadata.attributes));
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
