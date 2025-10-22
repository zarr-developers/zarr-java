package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.v3.chunkgrid.ChunkGrid;
import dev.zarr.zarrjava.v3.chunkgrid.RegularChunkGrid;
import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.core.ShardingIndexedCodec;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public final class ArrayMetadata extends dev.zarr.zarrjava.core.ArrayMetadata {
  public static final String NODE_TYPE = "array";
  static final int ZARR_FORMAT = 3;

  @JsonProperty("zarr_format")
  public final int zarrFormat = ZARR_FORMAT;
  @JsonProperty("node_type")
  public final String nodeType = NODE_TYPE;

  @JsonProperty("data_type")
  public final DataType dataType;

  @JsonProperty("chunk_grid")
  public final ChunkGrid chunkGrid;

  @JsonProperty("chunk_key_encoding")
  public final ChunkKeyEncoding chunkKeyEncoding;

  @JsonProperty("codecs")
  public final Codec[] codecs;
  @Nullable
  @JsonProperty("attributes")
  public final Attributes attributes;
  @Nullable
  @JsonProperty("dimension_names")
  public final String[] dimensionNames;
  @Nullable
  @JsonProperty("storage_transformers")
  public final Map<String, Object>[] storageTransformers;

  @JsonIgnore
  public CoreArrayMetadata coreArrayMetadata;

  public ArrayMetadata(
      long[] shape, DataType dataType, ChunkGrid chunkGrid, ChunkKeyEncoding chunkKeyEncoding,
      Object fillValue,
      @Nonnull Codec[] codecs,
      @Nullable String[] dimensionNames,
      @Nullable Attributes attributes,
      @Nullable Map<String, Object>[] storageTransformers
  ) throws ZarrException {
    this(ZARR_FORMAT, NODE_TYPE, shape, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
        dimensionNames,
        attributes, storageTransformers
    );
  }

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ArrayMetadata(
      @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
      @JsonProperty(value = "node_type", required = true) String nodeType,
      @JsonProperty(value = "shape", required = true) long[] shape,
      @JsonProperty(value = "data_type", required = true) DataType dataType,
      @JsonProperty(value = "chunk_grid", required = true) ChunkGrid chunkGrid,
      @JsonProperty(value = "chunk_key_encoding", required = true) ChunkKeyEncoding chunkKeyEncoding,
      @JsonProperty(value = "fill_value", required = true) Object fillValue,
      @Nonnull @JsonProperty(value = "codecs") Codec[] codecs,
      @Nullable @JsonProperty(value = "dimension_names") String[] dimensionNames,
      @Nullable @JsonProperty(value = "attributes") Attributes attributes,
      @Nullable @JsonProperty(value = "storage_transformers") Map<String, Object>[] storageTransformers
  ) throws ZarrException {
    super(shape, fillValue, dataType);
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
          "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }
    if (!nodeType.equals(this.nodeType)) {
      throw new ZarrException(
          "Expected node type '" + this.nodeType + "', got '" + nodeType + "'.");
    }
    if (storageTransformers != null && storageTransformers.length > 0) {
      throw new ZarrException(
          "Storage transformers are not supported in this version of Zarr Java.");
    }
    if (chunkGrid instanceof RegularChunkGrid) {
      int[] chunkShape = ((RegularChunkGrid) chunkGrid).configuration.chunkShape;
      if (shape.length != chunkShape.length) {
        throw new ZarrException("Shape (ndim=" + shape.length + ") and chunk shape (ndim=" +
            chunkShape.length + ") need to have the same number of dimensions.");
      }

      Optional<Codec> shardingCodec = getShardingIndexedCodec(codecs);
      int[] outerChunkShape = chunkShape;
      while (shardingCodec.isPresent()) {
        ShardingIndexedCodec.Configuration shardingConfig = ((ShardingIndexedCodec) shardingCodec.get()).configuration;
        int[] innerChunkShape = shardingConfig.chunkShape;
        if (outerChunkShape.length != innerChunkShape.length)
          throw new ZarrException("Sharding dimensions mismatch of outer chunk shape " + Arrays.toString(outerChunkShape) + " and inner chunk shape" + Arrays.toString(innerChunkShape));
        for (int i = 0; i < outerChunkShape.length; i++) {
          if (outerChunkShape[i] % innerChunkShape[i] != 0)
            throw new ZarrException("Sharding inner chunk shape " + Arrays.toString(innerChunkShape) + " does not evenly divide the outer chunk size " + Arrays.toString(outerChunkShape));
        }
        outerChunkShape = innerChunkShape;
        shardingCodec = getShardingIndexedCodec(shardingConfig.codecs);
      }
    }
    this.chunkGrid = chunkGrid;
    this.dataType = dataType;
    this.coreArrayMetadata =
        new CoreArrayMetadata(this.shape, ((RegularChunkGrid) chunkGrid).configuration.chunkShape,
            this.dataType,
            this.parsedFillValue
        );

    this.chunkKeyEncoding = chunkKeyEncoding;
    this.codecs = codecs;
    this.dimensionNames = dimensionNames;
    this.attributes = attributes;
    this.storageTransformers = storageTransformers;
  }


  public ucar.ma2.Array allocateFillValueChunk() {
    return coreArrayMetadata.allocateFillValueChunk();
  }

  @Override
  public ChunkKeyEncoding chunkKeyEncoding() {
    return chunkKeyEncoding;
  }

  @Override
  public Object parsedFillValue() {
    return parsedFillValue;
  }

  @Nonnull
  @Override
  public Attributes attributes() throws ZarrException {
    if (attributes == null) {
      throw new ZarrException("Array attributes have not been set.");
    }
    return new Attributes(attributes); //todo change attributes to Attributes type
  }

  public static Optional<Codec> getShardingIndexedCodec(Codec[] codecs) {
    return Arrays.stream(codecs).filter(codec -> codec instanceof ShardingIndexedCodec).findFirst();
  }

  public int[] chunkShape() {
    return ((RegularChunkGrid) this.chunkGrid).configuration.chunkShape;
  }

  @Override
  public DataType dataType() {
    return dataType;
  }

  public int chunkSize() {
    return coreArrayMetadata.chunkSize();
  }

  public int chunkByteLength() {
    return coreArrayMetadata.chunkByteLength();
  }


}
