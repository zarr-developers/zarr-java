package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;


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

    /**
     * Members of the metadata document that zarr-java does not know about. The Zarr v3 specification
     * requires that these are ignored when they declare {@code "must_understand": false}, and that
     * they are rejected otherwise. They are kept here so that rewriting the metadata does not drop
     * extensions written by another implementation.
     */
    private final Map<String, Object> extraFields;

    @JsonIgnore
    public CoreArrayMetadata coreArrayMetadata;

    @JsonIgnore
    private final long[] lowerBound;

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

    public ArrayMetadata(
            long[] shape, DataType dataType, ChunkGrid chunkGrid, ChunkKeyEncoding chunkKeyEncoding,
            Object fillValue,
            @Nonnull Codec[] codecs,
            @Nullable String[] dimensionNames,
            @Nullable Attributes attributes,
            @Nullable Map<String, Object>[] storageTransformers,
            @Nullable Map<String, Object> extraFields
    ) throws ZarrException {
        this(ZARR_FORMAT, NODE_TYPE, shape, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
                dimensionNames,
                attributes, storageTransformers, extraFields
        );
    }

    public ArrayMetadata(
            int zarrFormat,
            String nodeType,
            long[] shape,
            DataType dataType,
            ChunkGrid chunkGrid,
            ChunkKeyEncoding chunkKeyEncoding,
            Object fillValue,
            @Nonnull Codec[] codecs,
            @Nullable String[] dimensionNames,
            @Nullable Attributes attributes,
            @Nullable Map<String, Object>[] storageTransformers
    ) throws ZarrException {
        this(zarrFormat, nodeType, shape, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
                dimensionNames, attributes, storageTransformers, null
        );
    }

    public ArrayMetadata(
            int zarrFormat,
            String nodeType,
            long[] shape,
            DataType dataType,
            ChunkGrid chunkGrid,
            ChunkKeyEncoding chunkKeyEncoding,
            Object fillValue,
            @Nonnull Codec[] codecs,
            @Nullable String[] dimensionNames,
            @Nullable Attributes attributes,
            @Nullable Map<String, Object>[] storageTransformers,
            @Nullable Map<String, Object> extraFields
    ) throws ZarrException {
        this(zarrFormat, nodeType, shape, null, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
                dimensionNames, attributes, storageTransformers, extraFields
        );
    }

    /**
     * Creates array metadata from either a {@code shape} or explicit {@code bounds}; exactly one of
     * them must be given.
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ArrayMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "node_type", required = true) String nodeType,
            @Nullable @JsonProperty(value = "shape") long[] shape,
            @Nullable @JsonProperty(value = "bounds") Bounds bounds,
            @JsonProperty(value = "data_type", required = true) DataType dataType,
            @JsonProperty(value = "chunk_grid", required = true) ChunkGrid chunkGrid,
            @JsonProperty(value = "chunk_key_encoding", required = true) ChunkKeyEncoding chunkKeyEncoding,
            @JsonProperty(value = "fill_value", required = true) Object fillValue,
            @Nonnull @JsonProperty(value = "codecs") Codec[] codecs,
            @Nullable @JsonProperty(value = "dimension_names") String[] dimensionNames,
            @Nullable @JsonProperty(value = "attributes") Attributes attributes,
            @Nullable @JsonProperty(value = "storage_transformers") Map<String, Object>[] storageTransformers,
            @Nullable @JsonAnySetter Map<String, Object> extraFields
    ) throws ZarrException {
        super(derivedShape(shape, bounds), fillValue, dataType);
        this.lowerBound = bounds != null ? bounds.inclusiveMin.clone() : new long[this.shape.length];
        this.extraFields = ExtraFields.validatedArrayFields(extraFields);
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
            if (this.shape.length != chunkShape.length) {
                throw new ZarrException("Shape (ndim=" + this.shape.length + ") and chunk shape (ndim=" +
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

    /**
     * The members of the metadata document that zarr-java does not know about, but that declared
     * {@code "must_understand": false} and could therefore be ignored. They are written back out
     * unchanged, so that extensions written by another implementation survive a metadata rewrite.
     *
     * @return the extra fields, never {@code null}
     */
    @JsonAnyGetter
    public Map<String, Object> extraFields() {
        return extraFields;
    }

    private static long[] derivedShape(@Nullable long[] shape, @Nullable Bounds bounds)
            throws ZarrException {
        if ((shape == null) == (bounds == null)) {
            throw new ZarrException("Array metadata needs to contain exactly one of 'shape' and 'bounds'.");
        }
        if (shape != null) {
            return shape;
        }
        if (bounds.inclusiveMin == null || bounds.exclusiveMax == null) {
            throw new ZarrException("'bounds' needs to contain 'inclusive_min' and 'exclusive_max'.");
        }
        if (bounds.inclusiveMin.length != bounds.exclusiveMax.length) {
            throw new ZarrException("'inclusive_min' and 'exclusive_max' need to have the same length.");
        }
        long[] derived = new long[bounds.inclusiveMin.length];
        for (int i = 0; i < derived.length; i++) {
            if (bounds.inclusiveMin[i] > bounds.exclusiveMax[i]) {
                throw new ZarrException("'inclusive_min' needs to be less than or equal to 'exclusive_max'.");
            }
            try {
                derived[i] = Math.subtractExact(bounds.exclusiveMax[i], bounds.inclusiveMin[i]);
            } catch (ArithmeticException e) {
                throw new ZarrException("The length of dimension " + i + " exceeds the int64 range.");
            }
        }
        return derived;
    }

    @Override
    public long[] lowerBound() {
        return lowerBound.clone();
    }

    /**
     * The coordinate range of the array: the first and one past the last coordinate along each
     * dimension.
     */
    @JsonIgnore
    public Bounds bounds() {
        long[] exclusiveMax = new long[ndim()];
        for (int i = 0; i < exclusiveMax.length; i++) {
            exclusiveMax[i] = lowerBound[i] + shape[i];
        }
        return new Bounds(lowerBound.clone(), exclusiveMax);
    }

    private boolean hasZeroLowerBound() {
        return Arrays.stream(lowerBound).allMatch(b -> b == 0);
    }

    @Nullable
    @JsonProperty("shape")
    private long[] jsonShape() {
        return hasZeroLowerBound() ? shape : null;
    }

    @Nullable
    @JsonProperty("bounds")
    private Bounds jsonBounds() {
        return hasZeroLowerBound() ? null : bounds();
    }

    public static Optional<Codec> getShardingIndexedCodec(Codec[] codecs) {
        return Arrays.stream(codecs).filter(codec -> codec instanceof ShardingIndexedCodec).findFirst();
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
        return attributes;
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

    /**
     * A half-open coordinate range per dimension.
     */
    public static final class Bounds {
        @JsonProperty("inclusive_min")
        public final long[] inclusiveMin;
        @JsonProperty("exclusive_max")
        public final long[] exclusiveMax;

        @JsonCreator
        public Bounds(
                @JsonProperty(value = "inclusive_min", required = true) long[] inclusiveMin,
                @JsonProperty(value = "exclusive_max", required = true) long[] exclusiveMax
        ) {
            this.inclusiveMin = inclusiveMin;
            this.exclusiveMax = exclusiveMax;
        }
    }


}
